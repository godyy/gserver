package session

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	cmsg "github.com/godyy/gserver/cluster/msg"

	"github.com/godyy/gnet"
	"github.com/godyy/gutils/log"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	sessionStarted = 1
	sessionClosed  = 2
)

var errSessionStarted = errors.New("session started")
var errSessionNotStarted = errors.New("session not started")
var errSessionClosed = errors.New("session closed")
var errSessionInactive = errors.New("session inactive")
var errLowerLevelSessionNotMatch = errors.New("lower level session not match")
var errUpperLevelSessionClosed = errors.New("upper level session closed")

var msgPing = &msgHeartbeat{Ping: true}
var msgPong = &msgHeartbeat{Ping: false}

type Config struct {
	// 心跳超时 ms
	HeartbeatTimeout int `yaml:"HeartbeatTimeout"`

	// 会话失效超时 ms
	InactiveTimeout int `yaml:"InactiveTimeout"`

	// 会话读取超时 ms
	ReadTimeout int `yaml:"ReadTimeout"`

	// 会话写入超时 ms
	WriteTimeout int `yaml:"WriteTimeout"`

	// 会话读取缓冲区大小
	ReadBufferSize int `yaml:"ReadBufferSize"`

	// 会话写入缓冲区大小
	WriteBufferSize int `yaml:"WriteBufferSize"`

	// 会话发送队列大小
	SendQueueSize int `yaml:"SendQueueSize"`

	// 会话支持的最大包体大小
	MaxPacketSize int `yaml:"MaxPacketSize"`
}

func (c *Config) GetHeartbeatTimeout() time.Duration {
	return time.Duration(c.HeartbeatTimeout) * time.Millisecond
}

func (c *Config) GetInactiveTimeout() time.Duration {
	return time.Duration(c.InactiveTimeout) * time.Millisecond
}

func (c *Config) CreateOption() *gnet.TCPSessionOption {
	return gnet.NewTCPSessionOption().
		SetReceiveTimeout(time.Duration(c.ReadTimeout) * time.Millisecond).
		SetSendTimeout(time.Duration(c.WriteTimeout) * time.Millisecond).
		SetReceiveBufferSize(c.ReadBufferSize).
		SetSendBufferSize(c.WriteBufferSize).
		SetSendQueueSize(c.SendQueueSize).
		SetMaxPacketSize(c.MaxPacketSize)
}

// Session 网络会话
type Session struct {
	mtx            sync.Mutex
	id             int32            // ID
	state          int32            // 状态
	service        *Service         // 所属service
	client         bool             // 是否主动发起的会话（需要发送心跳请求）
	gSession       *gnet.TCPSession // gnet网络会话
	remoteNodeId   string           // 远端节点ID
	activeAt       atomic.Int64     // 最近一次激活的时间（发送/接收消息）
	inactiveTimer  *time.Timer      // 失效定时器
	heartbeatTimer *time.Timer      // 心跳定时器
	logger         log.Logger       // 日志
}

func newSession(id int32, service *Service, client bool, remoteNodeId string, session *gnet.TCPSession, logger log.Logger) *Session {
	s := &Session{
		id:           id,
		service:      service,
		client:       client,
		gSession:     session,
		remoteNodeId: remoteNodeId,
		logger:       logger.WithFields(zap.Dict("session", zap.Int32("Id", id), zap.String("RemoteNodeId", remoteNodeId), zap.Bool("Client", client))),
	}
	return s
}

// start 启动会话
func (s *Session) start(option *gnet.TCPSessionOption) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.state != 0 {
		return errSessionStarted
	}

	if err := s.gSession.Start(option, s); err != nil {
		return err
	}

	s.active()
	s.state = sessionStarted
	s.logger.Info("session started")
	return nil
}

// close 关闭会话
func (s *Session) close(reason error) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.state == sessionClosed {
		return errSessionClosed
	}

	s.doClose(reason)
	s.logger.InfoFields("session closed", zap.Error(reason))
	return nil
}

func (s *Session) doClose(reason error) {
	s.stopHeartbeat()
	s.stopInactiveTimer()
	s.gSession.Close(reason)
	s.state = sessionClosed
}

func (s *Session) RemoteNodeId() string {
	return s.remoteNodeId
}

func (s *Session) LocalAddr() net.Addr {
	return s.gSession.LocalAddr()
}

func (s *Session) RemoteAddr() net.Addr {
	return s.gSession.RemoteAddr()
}

// active 会话激活
func (s *Session) active() {
	s.activeAt.Store(time.Now().UnixNano())
	s.startHeartbeat()
	s.startInactiveTimer()
}

func (s *Session) startHeartbeat() {
	if !s.client {
		return
	}

	heartbeatTimeout := s.service.config.Session.GetHeartbeatTimeout()

	if s.heartbeatTimer == nil {
		s.heartbeatTimer = time.AfterFunc(heartbeatTimeout, s.onHeartbeatTimer)
	} else {
		s.heartbeatTimer.Stop()
		s.heartbeatTimer.Reset(heartbeatTimeout)
	}
}

func (s *Session) stopHeartbeat() {
	if !s.client {
		return
	}

	if s.heartbeatTimer != nil {
		s.heartbeatTimer.Stop()
		s.heartbeatTimer = nil
	}
}

func (s *Session) onHeartbeatTimer() {
	s.mtx.Lock()
	if s.state == sessionClosed {
		s.mtx.Unlock()
		return
	}

	s.mtx.Unlock()

	if time.Now().UnixNano()-s.activeAt.Load() >= int64(s.service.config.Session.GetHeartbeatTimeout()) {
		if err := s.sendMessage(msgPing); err != nil {
			// 发送心跳消息失败
			s.logger.ErrorFields("session send heartbeat", zap.Error(err))
		}
	}

	s.mtx.Lock()
	s.startHeartbeat()
	s.mtx.Unlock()
}

func (s *Session) startInactiveTimer() {
	inactiveTimeout := s.service.config.Session.GetInactiveTimeout()

	if s.inactiveTimer == nil {
		s.inactiveTimer = time.AfterFunc(inactiveTimeout, s.onInactiveTimer)
	} else {
		s.inactiveTimer.Stop()
		s.inactiveTimer.Reset(inactiveTimeout)
	}
}

func (s *Session) stopInactiveTimer() {
	if s.inactiveTimer != nil {
		s.inactiveTimer.Stop()
		s.inactiveTimer = nil
	}
}

func (s *Session) onInactiveTimer() {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.state == sessionClosed {
		return
	}

	inactiveTimeout := s.service.config.Session.GetInactiveTimeout()
	if time.Now().UnixNano()-s.activeAt.Load() < int64(inactiveTimeout) {
		s.startInactiveTimer()
		return
	}
	s.doClose(errSessionInactive)
}

// SendMsg 发送数据包
func (s *Session) SendMsg(msg cmsg.Msg, timeout ...time.Duration) error {
	if msg == nil {
		return nil
	}

	s.mtx.Lock()
	if s.state != sessionStarted {
		s.mtx.Unlock()
		return errSessionNotStarted
	}
	s.active()
	s.mtx.Unlock()

	appMsg := &msgPayload{Payload: msg}
	return s.sendMessage(appMsg, timeout...)
}

func (s *Session) sendMessage(msg msg, timeout ...time.Duration) error {
	if msg == nil {
		return nil
	}

	defer msg.Recycle()

	p, err := s.service.encodeMsg(msg)
	if err != nil {
		return err
	}

	return s.gSession.SendPacket(p, timeout...)
}

// OnSessionPacket 接收底层数据包事件
func (s *Session) OnSessionPacket(gSession gnet.Session, p *gnet.Packet) error {
	if s.gSession != gSession {
		return errLowerLevelSessionNotMatch
	}

	defer s.service.putPacket(p)

	msg, err := s.service.decodeMsg(p)
	if err != nil {
		return errors.WithMessage(err, "decode msg")
	}

	defer msg.Recycle()

	s.mtx.Lock()
	if s.state == sessionClosed {
		s.mtx.Unlock()
		return errUpperLevelSessionClosed
	}

	switch msg.msgType() {
	case mtHeartbeat:
		s.mtx.Unlock()
		msgHeartbeat := msg.(*msgHeartbeat)
		s.logger.Info("session receive heartbeat")
		if msgHeartbeat.Ping {
			if err := s.sendMessage(msgPong); err != nil {
				return errors.WithMessage(err, "reply heartbeat")
			}
		}
	case mtPayload:
		s.active()
		s.mtx.Unlock()
		msgApplication := msg.(*msgPayload)
		return s.service.onSessionMsg(s, msgApplication.Payload)
	default:
		s.mtx.Unlock()
		return fmt.Errorf("receive invalid messge type %d", msg.msgType())
	}

	return nil
}

// OnSessionClosed 接收底层关闭事件
func (s *Session) OnSessionClosed(gSession gnet.Session, err error) {
	if s.gSession != gSession {
		s.logger.ErrorFields("on lower-level session closed", zap.Error(errLowerLevelSessionNotMatch))
		return
	}

	if err := s.close(err); err != nil {
		s.service.onSessionClosed(s)
	}
}
