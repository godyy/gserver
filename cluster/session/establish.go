package session

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"

	"github.com/godyy/gnet"
	"github.com/pkg/errors"
)

var errEsEnd = errors.New("establish end")
var errActiveEsStarted = errors.New("active establish started")
var errPassiveEsStarted = errors.New("passive establish started")
var errEsAckNodeIdWrong = errors.New("establish ack nodeId wrong")
var errEsAckTokenWrong = errors.New("establish ack token wrong")
var errEsTokenWrong = errors.New("establish token wrong")

const (
	esStateDoing   = 0 // 建立中
	esStateSuccess = 1 // 建立成功
	esStateFailed  = 2 // 建立失败

	esFlagActiveStart  = 0 // 主动建立开始
	esFlagActiveEnd    = 1 // 主动建立结束
	esFlagPassiveStart = 2 // 被动建立开始
	esFlagPassiveEnd   = 3 // 被动建立结束

	esStateMask = 0xf // 状态掩码
)

func getEsFlagString(flag uint8) string {
	switch flag {
	case esFlagActiveStart:
		return "active"
	case esFlagActiveEnd:
		return "active"
	case esFlagPassiveStart:
		return "passive"
	case esFlagPassiveEnd:
		return "passive"
	default:
		return ""
	}
}

var establishId atomic.Int32

// sessionEs 会话建立工具
type sessionEs struct {
	mtx          sync.Mutex
	id           int32         // id
	flag         uint8         // 包含flag和state
	service      *Service      // 所属service
	remoteNodeId string        // 远端结点ID
	session      *Session      // 最终建立的会话
	chDone       chan struct{} // 建立完成通道
	err          error         // 记录错误原因
}

func newSessionEs(service *Service, remoteNodeId string) *sessionEs {
	se := &sessionEs{
		id:           establishId.Add(1),
		service:      service,
		remoteNodeId: remoteNodeId,
		chDone:       make(chan struct{}, 1),
	}
	return se
}

func (se *sessionEs) getState() uint8 {
	return se.flag & esStateMask
}

func (se *sessionEs) setState(st uint8) {
	se.flag &= ^uint8(esStateMask)
	se.flag |= st
}

func (se *sessionEs) setFlag(f uint8) {
	se.flag |= 1 << (f + 4)
}

func (se *sessionEs) testFlag(f uint8) bool {
	return se.flag&(1<<(f+4)) != 0
}

// testAllEnd 测试是否所有建立均结束
func (se *sessionEs) testAllEnd() bool {
	a := se.testFlag(esFlagActiveStart)
	if a && !se.testFlag(esFlagActiveEnd) {
		return false
	}

	p := se.testFlag(esFlagPassiveStart)
	if p && !se.testFlag(esFlagPassiveEnd) {
		return false
	}

	return a || p
}

// startActive 开始主动建立
func (se *sessionEs) startActive(remoteAddr string) error {
	se.mtx.Lock()
	defer se.mtx.Unlock()

	if se.getState() != esStateDoing {
		return errEsEnd
	}

	if se.testFlag(esFlagActiveStart) {
		return errActiveEsStarted
	}

	active := &activeSessionEs{sessionEs: se}
	se.setFlag(esFlagActiveStart)
	go active.run(se.remoteNodeId, remoteAddr)
	return nil
}

// startPassive 开始被动建立
func (se *sessionEs) startPassive(conn net.Conn, hs *msgHandshake) error {
	se.mtx.Lock()
	defer se.mtx.Unlock()

	if se.getState() != esStateDoing {
		return errEsEnd
	}

	if se.testFlag(esFlagPassiveStart) {
		return errPassiveEsStarted
	}

	passive := &passiveSessionEs{sessionEs: se}
	se.setFlag(esFlagPassiveStart)
	go passive.run(conn, hs)
	return nil
}

func (se *sessionEs) onEsEnd(flag uint8, session *Session, err error) {
	se.mtx.Lock()

	if se.getState() != esStateDoing {
		se.mtx.Unlock()
		return
	}

	se.setFlag(flag)

	done := false
	if err == nil {
		// 建立成功
		se.session = session
		se.setState(esStateSuccess)
		done = true
	} else if se.testAllEnd() {
		// 所有建立均结束才失败
		se.err = err
		se.setState(esStateFailed)
		done = true
	}

	se.mtx.Unlock()

	if err != nil {
		se.service.logger.ErrorFields(
			"session establish failed",
			zap.Dict(
				"establish",
				zap.String("Flag", getEsFlagString(esFlagActiveStart)),
				zap.Int32("Id", se.id),
				zap.String("NodeId", se.remoteNodeId),
				zap.Error(err),
			),
		)
	} else {
		se.service.logger.InfoFields(
			"session establish success",
			zap.Dict(
				"establish",
				zap.String("Flag", getEsFlagString(esFlagActiveStart)),
				zap.Int32("Id", se.id),
				zap.String("NodeId", se.remoteNodeId),
				zap.Any("Remote", session.RemoteAddr()),
			),
		)
	}

	if done {
		se.service.onSessionEsEnd(se)
		close(se.chDone)
	}
}

// waitEnd 同步等待结束
func (se *sessionEs) waitEnd() (*Session, error) {
	_, _ = <-se.chDone
	return se.session, se.err
}

func (se *sessionEs) isFailed() bool {
	se.mtx.Lock()
	defer se.mtx.Unlock()
	return se.getState() == esStateFailed
}

func (se *sessionEs) isSuccess() bool {
	se.mtx.Lock()
	defer se.mtx.Unlock()
	return se.getState() == esStateSuccess
}

// stop 停止建立
func (se *sessionEs) stop(reason error) {
	se.mtx.Lock()
	defer se.mtx.Unlock()

	if se.getState() != esStateDoing {
		return
	}

	se.setState(esStateFailed)
	se.err = reason
	close(se.chDone)
}

// activeSessionEs 会话主动建立过程
type activeSessionEs struct {
	*sessionEs
}

func (e *activeSessionEs) run(remoteNodeId string, remoteAddr string) {
	// 发起连接
	conn, err := net.Dial("tcp", remoteAddr)
	if err != nil {
		e.fail(errors.WithMessagef(err, "connect %s", remoteAddr))
		return
	}

	config := e.service.config

	// 发送握手请求
	if err := e.service.writeMessage(conn, &msgHandshake{NodeId: e.service.NodeId, Token: config.Token}, config.GetHandshakeTimeout()); err != nil {
		e.fail(errors.WithMessage(err, "write handshake"))
		return
	}

	// 读取握手响应
	msg, err := e.service.readMessage(conn, config.GetHandshakeTimeout())
	if err != nil {
		e.fail(errors.WithMessage(err, "client read handshake response"))
		return
	}

	defer msg.Recycle()

	switch msg.msgType() {
	case mtHandshakeAck:
		// 收到握手确认

		ack := msg.(*msgHandshakeAck)
		if ack.NodeId != remoteNodeId {
			e.service.writeMessage(conn, &msgHandshakeReject{Reason: "nodeId wrong"}, config.GetHandshakeTimeout())
			e.fail(errEsAckNodeIdWrong)
			return
		}
		if ack.Token != config.Token {
			e.service.writeMessage(conn, &msgHandshakeReject{Reason: "token wrong"}, config.GetHandshakeTimeout())
			e.fail(errEsAckTokenWrong)
			return
		}

		// 回复握手完成
		if err := e.service.writeMessage(conn, &msgHandshakeCompleted{}, config.GetHandshakeTimeout()); err != nil {
			e.fail(errors.WithMessage(err, "write handshakeCompleted"))
			return
		}

		// 建立会话
		session := newSession(e.id, e.service, true, remoteNodeId, gnet.NewTCPSession(conn.(*net.TCPConn)), e.service.logger)
		if err := session.start(e.service.sessionOption); err != nil {
			e.fail(errors.WithMessage(err, "session start"))
			return
		}

		e.success(session)

	case mtHandshakeReject:
		// 握手被拒绝
		reject := msg.(*msgHandshakeReject)
		e.fail(fmt.Errorf("handshake rejected: %s", reject.Reason))

	default:
		e.fail(fmt.Errorf("invalid handshake response type: %d", msg.msgType()))
	}
}

func (e *activeSessionEs) success(session *Session) {
	e.onEsEnd(esFlagActiveEnd, session, nil)
}

func (e *activeSessionEs) fail(err error) {
	e.onEsEnd(esFlagActiveEnd, nil, err)
}

// passiveSessionEs 会话被动建立过程
type passiveSessionEs struct {
	*sessionEs
}

func (e *passiveSessionEs) run(conn net.Conn, handshake *msgHandshake) {
	config := e.service.config
	remoteNodeId := handshake.NodeId

	// 校验token
	if handshake.Token != config.Token {
		// token比对失败, 拒绝握手
		e.service.writeMessage(conn, &msgHandshakeReject{Reason: "token wrong"}, config.GetHandshakeTimeout())
		e.fail(errEsTokenWrong)
		return
	}

	// 回复握手确认
	if err := e.service.writeMessage(conn, &msgHandshakeAck{NodeId: e.service.NodeId, Token: config.Token}, config.GetHandshakeTimeout()); err != nil {
		e.fail(errors.WithMessage(err, "write handshakeAck"))
		return
	}

	// 读取回复
	response, err := e.service.readMessage(conn, config.GetHandshakeTimeout())
	if err != nil {
		e.fail(errors.WithMessage(err, "server read handshake response"))
		return
	}

	defer response.Recycle()

	switch response.msgType() {
	case mtHandshakeCompleted:
		// 握手完成
		session := newSession(e.id, e.service, false, remoteNodeId, gnet.NewTCPSession(conn.(*net.TCPConn)), e.service.logger)
		if err := session.start(e.service.sessionOption); err != nil {
			e.fail(errors.WithMessage(err, "session start"))
			return
		}

		e.success(session)

	case mtHandshakeReject:
		// 握手拒绝
		handshakeReject := response.(*msgHandshakeReject)
		e.fail(fmt.Errorf("handshake rejected: %s", handshakeReject.Reason))

	default:
		e.fail(fmt.Errorf("invalid handshake response type: %d", response.msgType()))
	}
}

func (e *passiveSessionEs) success(session *Session) {
	e.onEsEnd(esFlagPassiveEnd, session, nil)
}

func (e *passiveSessionEs) fail(err error) {
	e.onEsEnd(esFlagPassiveEnd, nil, err)
}
