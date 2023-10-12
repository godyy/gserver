package session

import (
	"encoding/binary"
	"net"
	"sync"
	"time"

	cmsg "github.com/godyy/gserver/cluster/msg"

	"github.com/godyy/gnet"
	"github.com/godyy/gutils/log"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

var ErrServiceStarted = errors.New("service started")
var ErrServiceClose = errors.New("service close")
var ErrConnectSelf = errors.New("connect self")

const (
	serviceStarted = 1
	serviceClosed  = 2
)

type Handler interface {
	OnSessionMsg(*Session, cmsg.Msg) error
	OnSessionClosed(*Session)
}

type ServiceInfo struct {
	// 本地结点ID
	NodeId string `yaml:"NodeId"`

	// 服务地址
	Addr string `yaml:"Addr"`
}

type ServiceConfig struct {
	// 口令，用于握手
	Token string `yaml:"Token"`

	// 监听重试延迟 ms
	RetryDelayOfListening int32 `yaml:"RetryDelayOfListening"`

	// 握手超时 ms
	HandshakeTimeout int32 `yaml:"HandshakeTimeout"`

	// 会话相关配置
	Session Config `yaml:"Session"`
}

func (c *ServiceConfig) GetRetryDelayOfListening() time.Duration {
	return time.Duration(c.RetryDelayOfListening) * time.Millisecond
}

func (c *ServiceConfig) GetHandshakeTimeout() time.Duration {
	return time.Duration(c.HandshakeTimeout) * time.Millisecond
}

// Service 服务
type Service struct {
	mtx                sync.Mutex
	ServiceInfo                               // 服务信息
	config             *ServiceConfig         // 配置
	msgCodec           cmsg.Codec             // 消息编解器
	logger             log.Logger             // 日志
	sessionOption      *gnet.TCPSessionOption // 网络会话选项
	state              int32                  // 状态
	sessionHandler     Handler                // 会话处理器
	sessionListener    *gnet.TCPListener      // 网络监听器
	sessionEstablishes map[string]*sessionEs  // 建立中的会话
	sessions           map[string]*Session    // 已联通的会话
}

func NewService(
	si ServiceInfo,
	config *ServiceConfig,
	h Handler,
	msgCodec cmsg.Codec,
	logger log.Logger,
) *Service {
	s := &Service{
		ServiceInfo:        si,
		state:              0,
		config:             config,
		msgCodec:           msgCodec,
		logger:             logger,
		sessionOption:      config.Session.CreateOption(),
		sessionHandler:     h,
		sessionEstablishes: map[string]*sessionEs{},
		sessions:           map[string]*Session{},
	}
	return s
}

func (s *Service) Start() error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.state >= serviceStarted {
		return ErrServiceStarted
	}

	s.state = serviceStarted
	s.logger.Info("service started")
	go s.listen()
	return nil
}

func (s *Service) isClosed() bool {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.state == serviceClosed
}

// Close 关闭服务
func (s *Service) Close() {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.state == serviceClosed {
		return
	}

	if s.sessionListener != nil {
		s.sessionListener.Close()
	}

	for _, establish := range s.sessionEstablishes {
		establish.stop(ErrServiceClose)
	}
	for _, session := range s.sessions {
		session.close(ErrServiceClose)
	}

	s.state = serviceClosed
	s.logger.Info("service closed")
}

func (s *Service) getOrCreateEstablish(nodeId string) *sessionEs {
	se := s.sessionEstablishes[nodeId]
	if se == nil {
		se = newSessionEs(s, nodeId)
		s.sessionEstablishes[nodeId] = se
	}
	return se
}

// onSessionEsEnd 接收会话建立结束事件
func (s *Service) onSessionEsEnd(se *sessionEs) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.state == serviceClosed {
		return
	}

	delete(s.sessionEstablishes, se.remoteNodeId)
	if se.isSuccess() {
		s.sessions[se.remoteNodeId] = se.session
	}
}

func (s *Service) delSession(session *Session) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if ss := s.sessions[session.remoteNodeId]; session == ss {
		delete(s.sessions, session.remoteNodeId)
	}
}

// GetSession 获取联通指定结点的会话
func (s *Service) GetSession(remoteNodeId string) *Session {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.sessions[remoteNodeId]
}

// Connect 获取或连接到指定结点
func (s *Service) Connect(nodeId string, addr string) (*Session, error) {
	if nodeId == s.NodeId {
		return nil, ErrConnectSelf
	}

	s.mtx.Lock()
	if session := s.sessions[nodeId]; session != nil {
		s.mtx.Unlock()
		return session, nil
	}
	se := s.getOrCreateEstablish(nodeId)
	s.mtx.Unlock()

	if err := se.startActive(addr); err == nil {
		s.logger.InfoFields(
			"session establish start success",
			zap.Dict(
				"establish",
				zap.String("Flag", getEsFlagString(esFlagActiveStart)),
				zap.Int32("Id", se.id),
				zap.String("NodeId", se.remoteNodeId),
				zap.Any("Remote", addr),
			),
		)
	}
	return se.waitEnd()
}

func (s *Service) createListener() {
	retryDelay := s.config.GetRetryDelayOfListening()

	for {
		sessionListener, err := gnet.ListenTCP("tcp", s.Addr)
		if err == nil {
			s.logger.Info("service listening...")
			s.sessionListener = sessionListener
			break
		}

		if s.isClosed() {
			break
		}

		// 监听失败, 稍后重试
		s.logger.Errorf("service listening failed -> %v, retry %.2f secs later", err, retryDelay.Seconds())
		time.Sleep(retryDelay)
	}
}

// 监听过程
func (s *Service) listen() {
	var err error

	for !s.isClosed() {
		s.createListener()

		err = s.sessionListener.Start(func(conn net.Conn) {
			go func(conn net.Conn) {
				if msg, err := s.readMessage(conn, s.config.GetHandshakeTimeout()); err != nil {
					// 读取握手消息失败
					s.logger.ErrorFields("read handshake", zap.Any("Remote", conn.RemoteAddr()), zap.Error(err))
					conn.Close()
					return
				} else {
					defer msg.recycle()

					handshake, ok := msg.(*msgHandshake)
					if !ok {
						s.logger.ErrorFields("read handshake, invalid msg type", zap.Any("Remote", conn.RemoteAddr()), zap.Int8("MsgType", msg.msgType()))
						conn.Close()
						return
					}

					s.mtx.Lock()
					if s.sessions[handshake.NodeId] != nil {
						// 会话重复
						s.mtx.Unlock()
						s.writeMessage(conn, &msgHandshakeReject{Reason: "session already established"}, s.config.GetHandshakeTimeout())
						conn.Close()
						return
					}
					se := s.getOrCreateEstablish(handshake.NodeId)
					s.mtx.Unlock()

					if err := se.startPassive(conn, handshake); err == nil {
						s.logger.InfoFields(
							"session establish start success",
							zap.Dict(
								"establish",
								zap.String("Flag", getEsFlagString(esFlagPassiveStart)),
								zap.Int32("Id", se.id),
								zap.String("NodeId", se.remoteNodeId),
								zap.Any("Remote", conn.RemoteAddr()),
							))
					} else {
						s.writeMessage(conn, &msgHandshakeReject{Reason: err.Error()}, s.config.GetHandshakeTimeout())
						conn.Close()
					}
				}
			}(conn)
		})
		if err != nil {
			s.logger.ErrorFields("service listening stop", zap.Error(err))
		}
	}
}

// onSessionMsg 接收数据包事件
func (s *Service) onSessionMsg(session *Session, msg cmsg.Msg) error {
	return s.sessionHandler.OnSessionMsg(session, msg)
}

// onSessionClosed 接收会话关闭事件
func (s *Service) onSessionClosed(session *Session) {
	s.delSession(session)
	s.sessionHandler.OnSessionClosed(session)
}

// getPacket 创建packet
func (s *Service) getPacket(cap ...int) *gnet.Packet {
	return gnet.GetPacket(cap...)
}

// putPacket 回收packet
func (s *Service) putPacket(p *gnet.Packet) {
	gnet.PutPacket(p)
}

var errMessageLenOverLimited = errors.New("message length over limited")

// readMessage 读取消息
func (s *Service) readMessage(conn net.Conn, timeout ...time.Duration) (msg, error) {
	if len(timeout) > 0 {
		conn.SetReadDeadline(time.Now().Add(timeout[0]))
	} else {
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	}

	var buf [4]byte
	if _, err := conn.Read(buf[:]); err != nil {
		return nil, errors.WithMessage(err, "read length")
	}

	n := int(binary.BigEndian.Uint32(buf[:]))
	if n <= 0 {
		return nil, errMessageLenOverLimited
	}

	p := s.getPacket(n)
	defer s.putPacket(p)

	if _, err := p.ReadFromN(conn, n); err != nil {
		return nil, errors.WithMessage(err, "read message")
	}

	m, err := s.decodeMsg(p)
	if err != nil {
		return nil, errors.WithMessage(err, "decode message")
	}

	return m, nil
}

// writeMessage 发送消息
func (s *Service) writeMessage(conn net.Conn, msg msg, timeout ...time.Duration) error {
	defer msg.recycle()

	if len(timeout) > 0 {
		conn.SetWriteDeadline(time.Now().Add(timeout[0]))
	} else {
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	}

	var buf [4]byte
	n := msg.size()
	binary.BigEndian.PutUint32(buf[:], uint32(n))
	if _, err := conn.Write(buf[:]); err != nil {
		return errors.WithMessage(err, "write length")
	}

	p, err := s.encodeMsg(msg)
	if err != nil {
		return errors.WithMessage(err, "encode message")
	}

	defer s.putPacket(p)

	if _, err := p.WriteTo(conn); err != nil {
		return errors.WithMessage(err, "write message")
	}

	return nil
}
