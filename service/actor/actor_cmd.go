package actor

// import (
// 	"container/list"
// 	"errors"
// 	"time"

// 	"github.com/godyy/gserver/cluster/session"
// )

// var ErrActorStop = errors.New("actor stop")

// func (a *Actor) pushMsg(msg *cmdMsg) {
// 	a.locker.Lock()
// 	defer a.locker.Unlock()
// 	if a.isRunning() {
// 		a.scheduleExpireTask(time.Now().UnixNano())
// 		a.cmdQueue.push(msg)
// 		a.cmdCond.Signal()
// 	} else {
// 		msg.replyError(ErrActorStop)
// 	}
// }

// func (a *Actor) pushCmd(cmd cmd) {
// 	a.locker.Lock()
// 	defer a.locker.Unlock()
// 	if a.isRunning() {
// 		a.cmdQueue.push(cmd)
// 		a.cmdCond.Signal()
// 	}
// }

// type cmd interface {
// 	cmdType() int8
// 	do(*Actor)
// }

// type cmdQueue struct {
// 	l *list.List
// }

// func newCmdQueue() *cmdQueue {
// 	return &cmdQueue{
// 		l: list.New(),
// 	}
// }

// func (cq *cmdQueue) len() int { return cq.l.Len() }

// func (cq *cmdQueue) push(cmd cmd) {
// 	cq.l.PushBack(cmd)
// }

// func (cq *cmdQueue) pop() cmd {
// 	if cq.l.Len() <= 0 {
// 		return nil
// 	}
// 	return cq.l.Remove(cq.l.Front()).(cmd)
// }

// func (cq *cmdQueue) clear() {
// 	cq.l.Init()
// }

// const (
// 	_ = int8(iota)
// 	ctMsg
// 	ctTimer
// )

// type cmdMsg struct {
// 	session session.Session
// 	msg     msg
// }

// func newCmdMsg(session session.Session, msg msg) *cmdMsg {
// 	return &cmdMsg{
// 		session: session,
// 		msg:     msg,
// 	}
// }

// func (sm *cmdMsg) cmdType() int8 {
// 	return ctMsg
// }

// func (sm *cmdMsg) do(actor *Actor) {
// 	actor.handleMsg(sm)
// }

// func (sm *cmdMsg) replyError(err error) {
// 	var errResponse msg
// 	switch sm.msg.msgType() {
// 	case MTRequest:
// 		// todo
// 		// msg := sm.msg.(*MsgRequest)
// 	case MTRPCRequest:
// 		msg := sm.msg.(*MsgRPCRequest)
// 		errResponse = NewMsgRPCResponseWithError(msg.ReqId, msg.ToId, msg.FromId, err.Error())
// 	}

// 	if errResponse != nil {
// 		_ = sm.session.SendMsg(errResponse)
// 	}
// }

// type cmdTimer struct{}

// func newCmdTimer() *cmdTimer {
// 	return &cmdTimer{}
// }

// func (cmdTimer) cmdType() int8 {
// 	return ctTimer
// }

// func (cmdTimer) do(actor *Actor) {
// 	actor.updateScheduledTask(time.Now())
// }
