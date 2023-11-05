package actor

import (
	"container/list"
	"errors"
	"time"

	"github.com/godyy/gserver/cluster/session"
)

var ErrActorStop = errors.New("actor stop")

func (a *Actor) pushMsg(msg *cmdMsg) {
	a.mtx.Lock()
	defer a.mtx.Unlock()
	if a.isRunning() {
		a.scheduleExpireTask(time.Now().UnixNano())
		a.cmdQueue.push(msg)
	} else {
		msg.replyError(ErrActorStop)
	}
}

func (a *Actor) pushCmd(cmd cmd) {
	a.mtx.Lock()
	defer a.mtx.Unlock()
	if a.isRunning() {
		a.cmdQueue.push(cmd)
	}
}

type cmd interface {
	cmdType() int8
	do(*Actor)
}

type cmdQueue struct {
	in  *list.List
	out *list.List
}

func newCmdQueue() *cmdQueue {
	return &cmdQueue{
		in:  list.New(),
		out: list.New(),
	}
}

func (cq *cmdQueue) available() bool {
	if cq.in.Len() <= 0 {
		return false
	}

	if cq.out.Len() > 0 {
		panic("actor.cmdQueue: there are cmd not popped")
	}

	cq.in, cq.out = cq.out, cq.in
	return true
}

func (cq *cmdQueue) push(cmd cmd) {
	cq.in.PushBack(cmd)
}

func (cq *cmdQueue) pop() cmd {
	front := cq.out.Front()
	if front == nil {
		return nil
	}
	return cq.out.Remove(front).(cmd)
}

func (cq *cmdQueue) clear() {
	cq.in.Init()
	cq.out.Init()
}

const (
	_ = int8(iota)
	ctMsg
	ctTimer
)

type cmdMsg struct {
	session *session.Session
	msg     msg
}

func newCmdMsg(session *session.Session, msg msg) *cmdMsg {
	return &cmdMsg{
		session: session,
		msg:     msg,
	}
}

func (sm *cmdMsg) cmdType() int8 {
	return ctMsg
}

func (sm *cmdMsg) do(actor *Actor) {
	actor.handleMsg(sm)
}

func (sm *cmdMsg) replyError(err error) {
	var errResponse msg
	switch sm.msg.msgType() {
	case MTRequest:
		msg := sm.msg.(*MsgRequest)
	case MTRPCRequest:
		msg := sm.msg.(*MsgRPCRequest)
		errResponse = NewMsgRPCResponseWithError(msg.ReqId, msg.ToId, msg.FromId, err.Error())
	}

	if errResponse != nil {
		_ = sm.session.SendMsg(errResponse)
	}
}

type cmdTimer struct{}

func newCmdTimer() *cmdTimer {
	return &cmdTimer{}
}

func (cmdTimer) cmdType() int8 {
	return ctTimer
}

func (cmdTimer) do(actor *Actor) {
	actor.updateScheduledTask(time.Now())
}
