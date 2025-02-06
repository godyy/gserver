package actor

// import (
// 	"fmt"
// 	"time"

// 	"github.com/godyy/gserver/cluster/session"
// )

// func (a *Actor) handleMsg(msg *cmdMsg) {
// 	actorMsgHandlerSingleton.handleMsg(a, msg)
// }

// // Actor消息处理器单例
// var actorMsgHandlerSingleton = newActorMsgHandlers()

// // actorMsgCallback Actor消息处理回调
// type actorMsgCallback interface {
// 	msgType() int8
// 	call(*Actor, *cmdMsg)
// }

// // actorMsgCallbackWrapper Actor消息处理回调包装器
// type actorMsgCallbackWrapper[Msg msg] struct {
// 	callback func(*Actor, session.Session, Msg)
// }

// func (w actorMsgCallbackWrapper[Msg]) msgType() int8 {
// 	return (*new(Msg)).msgType()
// }

// func (w actorMsgCallbackWrapper[Msg]) call(a *Actor, am *cmdMsg) {
// 	w.callback(a, am.session, am.msg.(Msg))
// }

// func wrapActorMsgCallback[Msg msg](callback func(*Actor, session.Session, Msg)) actorMsgCallback {
// 	return actorMsgCallbackWrapper[Msg]{callback: callback}
// }

// // actorMsgHandler Actor消息处理器
// type actorMsgHandler struct {
// 	callbacks map[int8]actorMsgCallback
// }

// func newActorMsgHandlers() *actorMsgHandler {
// 	h := &actorMsgHandler{
// 		callbacks: map[int8]actorMsgCallback{},
// 	}
// 	h.registerCallback(wrapActorMsgCallback(h.handleMsgRequest))
// 	h.registerCallback(wrapActorMsgCallback(h.handleMsgRPCRequest))
// 	h.registerCallback(wrapActorMsgCallback(h.handleMsgForward))
// 	h.registerCallback(wrapActorMsgCallback(h.handleMsgConn))
// 	return h
// }

// // registerCallback 注册消息处理回调
// func (h *actorMsgHandler) registerCallback(callback actorMsgCallback) {
// 	msgType := callback.msgType()
// 	if _, ok := h.callbacks[msgType]; ok {
// 		panic(fmt.Errorf("duplicate actorMsgCallback of msgType %d", msgType))
// 	}
// 	h.callbacks[msgType] = callback
// }

// // handleMsg 处理消息
// func (h *actorMsgHandler) handleMsg(a *Actor, msg *cmdMsg) {
// 	mt := msg.msg.msgType()
// 	callback := h.callbacks[mt]
// 	if callback == nil {
// 		// todo
// 		return
// 	}

// 	callback.call(a, msg)
// }

// // handleMsgRequest MsgRequest消息处理回调
// func (h *actorMsgHandler) handleMsgRequest(a *Actor, session session.Session, m *MsgRequest) {
// 	conn := newConn(m.ConnId, session)
// 	if !a.updateConn(conn) {
// 		return
// 	}

// 	req := newRequest(a, m)
// 	if err := a.entity.HandleRequest(req); err != nil {
// 		// todo
// 	}
// }

// // handleMsgRPCRequest MsgRPCRequest消息处理回调
// func (h *actorMsgHandler) handleMsgRPCRequest(a *Actor, session session.Session, m *MsgRPCRequest) {
// 	if m.ExpiredAt <= time.Now().UnixMilli() {
// 		// todo 请求已超时，无需处理
// 		return
// 	}

// 	req := newRPCRequest(a, newRPCConn(session), m)
// 	if err := a.entity.HandleRequest(req); err != nil {
// 		// todo
// 	}
// }

// // handleMsgForward MsgForward消息处理回调
// func (h *actorMsgHandler) handleMsgForward(a *Actor, session session.Session, m *MsgForward) {
// 	a.conn.sendMsg(NewMsgResponse(a.conn.connId, a.Id(), m.Payload))
// }

// // handleMsgConn MsgConn消息处理回调
// func (h *actorMsgHandler) handleMsgConn(a *Actor, session session.Session, m *MsgConn) {
// 	switch m.Status {
// 	case ConnConnected:
// 		// 连接成功
// 		conn := newConn(m.ConnId, session)
// 		a.updateConn(conn)

// 	case ConnDisconnected:
// 		// 连接断开
// 		if a.conn.isConnected() && m.ConnId == a.conn.connId {
// 			a.conn.disconnect()
// 		}

// 	default:
// 		// todo 无效的状态
// 	}
// }
