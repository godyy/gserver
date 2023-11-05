package actor

import (
	"fmt"
	"github.com/godyy/gserver/cluster/session"
)

func (s *Service) handleActorMsg(actorId int64, msg *cmdMsg) {
	ac, ai := s.getActorOrInitializerWithId(actorId)
	if ac != nil {
		ac.pushMsg(msg)
	} else {
		ai.pushMsg(msg)
	}
}

// Actor模块消息处理单例
var moduleMsgHandlerSingleton = newModuleMsgHandler()

// moduleMsgCallback Actor模块消息处理回调
type moduleMsgCallback interface {
	msgType() int8
	call(*Service, cmdMsg)
}

// moduleMsgCallbackWrapper Actor模块消息处理回调包装器
type moduleMsgCallbackWrapper[Msg msg] struct {
	callback func(*Service, *session.Session, Msg)
}

func (w moduleMsgCallbackWrapper[Msg]) msgType() int8 {
	return (*new(Msg)).msgType()
}

func (w moduleMsgCallbackWrapper[Msg]) call(m *Service, am cmdMsg) {
	w.callback(m, am.session, am.msg.(Msg))
}

func wrapModuleMsgCallback[Msg msg](callback func(*Service, *session.Session, Msg)) moduleMsgCallback {
	return moduleMsgCallbackWrapper[Msg]{callback: callback}
}

// moduleMsgHandler Actor模块消息处理器
type moduleMsgHandler struct {
	callbacks []moduleMsgCallback
}

func newModuleMsgHandler() *moduleMsgHandler {
	h := &moduleMsgHandler{callbacks: make([]moduleMsgCallback, 0, mtMax)}
	h.registerCallback(wrapModuleMsgCallback(h.handleMsgRequest))
	h.registerCallback(wrapModuleMsgCallback(h.handleMsgResponse))
	h.registerCallback(wrapModuleMsgCallback(h.handleMsgRPCRequest))
	h.registerCallback(wrapModuleMsgCallback(h.handleMsgRPCResponse))
	h.registerCallback(wrapModuleMsgCallback(h.handleMsgForward))
	h.registerCallback(wrapModuleMsgCallback(h.handleMsgConn))
	return h
}

// registerCallback 注册消息处理回调
func (h *moduleMsgHandler) registerCallback(callback moduleMsgCallback) {
	msgType := callback.msgType()
	if msgType <= 0 || msgType >= mtMax {
		panic(fmt.Errorf("invalid msgType %v", msgType))
	}

	if h.callbacks[msgType] != nil {
		panic(fmt.Errorf("duplicate moduleMsgCallback of msg type %v", msgType))
	}

	h.callbacks[msgType] = callback
}

// handleMsg 处理消息
func (h *moduleMsgHandler) handleMsg(m *Service, msg cmdMsg) {
	mt := msg.msg.msgType()
	callback := h.callbacks[mt]
	if callback == nil {
		// todo
		return
	}

	callback.call(m, msg)
}

// handleMsgRequest MsgRequest消息处理回调
func (*moduleMsgHandler) handleMsgRequest(m *Service, session *session.Session, msg *MsgRequest) {
	m.handleActorMsg(
		msg.ActorId,
		newCmdMsg(session, msg),
	)
}

// handleMsgResponse MsgResponse消息处理回调
func (*moduleMsgHandler) handleMsgResponse(m *Service, session *session.Session, msg *MsgResponse) {
	// todo
}

// handleMsgRPCRequest MsgRPCRequest消息处理回调
func (*moduleMsgHandler) handleMsgRPCRequest(m *Service, session *session.Session, msg *MsgRPCRequest) {
	m.handleActorMsg(
		msg.ToId,
		newCmdMsg(session, msg),
	)
}

// handleMsgRPCResponse MsgRPCResponse消息处理回调
func (*moduleMsgHandler) handleMsgRPCResponse(m *Service, session *session.Session, msg *MsgRPCResponse) {
	// todo
	m.onRPCResponse(msg)
}

// handleMsgForward MsgForward消息处理回调
func (*moduleMsgHandler) handleMsgForward(m *Service, session *session.Session, msg *MsgForward) {}

// handleMsgConn MsgConn消息处理回调
func (*moduleMsgHandler) handleMsgConn(m *Service, session *session.Session, msg *MsgConn) {}
