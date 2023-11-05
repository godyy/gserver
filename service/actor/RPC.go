package actor

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/godyy/gnet"

	"github.com/godyy/gserver/cluster/session"
	"github.com/godyy/gutils/container/heap"
	pkg_errors "github.com/pkg/errors"
)

func (s *Service) RPCCall(callerId int64, calleeUuid string, args *gnet.Packet, timeoutMS ...int) (*rpcCall, error) {
	timeout := defaultRPCTimeout
	if len(timeoutMS) > 0 {
		timeout = timeoutMS[0]
	}

	// 获取被调用者元信息
	var calleeMeta Meta
	if err := s.md.GetMeta(calleeUuid, &calleeMeta); err != nil {
		return nil, pkg_errors.WithMessage(err, "get callee meta")
	}

	// 连接目标结点
	session, err := s.connectNodeByMeta(&calleeMeta)
	if err != nil {
		return nil, err
	}

	return s.call(session, callerId, calleeMeta.Id, args, timeout)
}

func (s *Service) RPCCast(callerId int64, calleeUuid string, args *gnet.Packet) error {
	// 获取被调用者元信息
	var toMeta Meta
	if err := s.md.GetMeta(calleeUuid, &toMeta); err != nil {
		return pkg_errors.WithMessage(err, "get callee meta")
	}

	// 连接目标结点
	session, err := s.connectNodeByMeta(&toMeta)
	if err != nil {
		return err
	}

	return s.cast(session, callerId, toMeta.Id, args)
}

func (s *Service) connectNodeByMeta(meta *Meta) (*session.Session, error) {
	session, err := s.cluster.ConnectNode(meta.NodeId)
	if err != nil {
		return nil, pkg_errors.WithMessagef(err, "connect node %v", meta.NodeId)
	}
	return session, nil
}

func (s *Service) call(session *session.Session, fromId, toId int64, args *gnet.Packet, timeoutMS ...int) (*rpcCall, error) {
	rpcManager := s.rpcManager

	timeout := s.config.GetDefaultRPCTimeout()
	if len(timeoutMS) > 0 && timeoutMS[0] > 0 {
		timeout = int64(timeoutMS[0])
	}

	req := NewMsgRPCRequestCall(rpcManager.genReqId(), fromId, toId, time.Now().UnixMilli()+timeout, args)

	call := &rpcCall{
		reqId:  req.ReqId,
		req:    req,
		index:  -1,
		chDone: make(chan struct{}, 1),
	}

	rpcManager.addCall(call)

	if err := newRPCConn(session).sendMsg(req); err != nil {
		rpcManager.delCall(call.reqId)
		// todo 回收call
		return nil, err
	}

	return call, nil
}

func (s *Service) cast(session *session.Session, fromId, toId int64, args *gnet.Packet) error {
	req := NewMsgRPCRequestCast(s.genReqId(), fromId, toId, args)
	if err := newRPCConn(session).sendMsg(req); err != nil {
		return err
	}
	return nil
}

func (s *Service) updateRPCCalls(now time.Time) {
	rpcManager := s.rpcManager
	rpcManager.mtx.Lock()
	defer rpcManager.mtx.Unlock()

	nowMilli := now.UnixMilli()
	for rpcManager.callHeap.Len() > 0 {
		call := rpcManager.callHeap.Top().(*rpcCall)
		if call.isExpired(nowMilli) {
			rpcManager.callHeap.Pop()
			delete(rpcManager.callMap, call.reqId)
			call.onError(errRPCCallTimeout)
		} else {
			rpcManager.resetExpiredTimer(call.expiredAt())
			break
		}
	}
}

func (s *Service) onRPCResponse(rsp *MsgRPCResponse) {
	call := s.delCall(rsp.ReqId)
	if call == nil {
		// call 已经过期
		return
	}

	call.onResponse(rsp)
}

var errRPCCallTimeout = errors.New("rpc call timeout")

type rpcCallResult struct {
	Reply *gnet.Packet
	Err   error
}

type rpcCall struct {
	reqId  int32
	req    *MsgRPCRequest
	rsp    *MsgRPCResponse
	err    error
	index  int
	chDone chan struct{}
}

func (rc *rpcCall) expiredAt() int64 {
	return rc.req.ExpiredAt
}

func (rc *rpcCall) isExpired(now int64) bool {
	return rc.req.ExpiredAt <= now
}

func (rc *rpcCall) waitDone() rpcCallResult {
	<-rc.chDone
	result := rpcCallResult{Err: rc.err}
	if result.Err == nil {
		result.Reply = rc.rsp.Reply
	}
	// todo 回收call
	return rpcCallResult{}
}

func (rc *rpcCall) onResponse(rsp *MsgRPCResponse) {
	rc.rsp = rsp
	rc.chDone <- struct{}{}
}

func (rc *rpcCall) onError(err error) {
	rc.err = err
	rc.chDone <- struct{}{}
}

func (rc *rpcCall) Less(o heap.Element) bool {
	return rc.expiredAt() < o.(*rpcCall).expiredAt()
}

func (rc *rpcCall) SetIndex(i int) {
	rc.index = i
}

func (rc *rpcCall) Index() int {
	return rc.index
}

type rpcManager struct {
	mtx          sync.Mutex
	reqId        int32
	callMap      map[int32]*rpcCall
	callHeap     *heap.Heap
	expiredTimer *time.Timer
}

func newRpcManager() *rpcManager {
	rm := &rpcManager{
		callMap:  map[int32]*rpcCall{},
		callHeap: heap.NewHeap(),
	}
	rm.expiredTimer = time.NewTimer(0)
	rm.expiredTimer.Stop()
	select {
	case <-rm.expiredTimer.C:
	default:
	}
	return rm
}

func (m *rpcManager) genReqId() int32 {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.reqId++
	reqId := m.reqId
	if m.reqId == math.MaxInt32 {
		m.reqId = 0
	}
	return reqId
}

func (m *rpcManager) addCall(call *rpcCall) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if _, ok := m.callMap[call.reqId]; ok {
		panic(fmt.Errorf("add duplicate rpcCall %d", call.reqId))
	}
	m.callMap[call.reqId] = call
	m.callHeap.Push(call)
	if call.Index() == 0 {
		m.resetExpiredTimer(call.expiredAt())
	}
}

func (m *rpcManager) delCall(reqId int32) *rpcCall {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	call, ok := m.callMap[reqId]
	if ok {
		isTop := call.Index() == 0
		delete(m.callMap, reqId)
		m.callHeap.Remove(call.Index())
		if isTop {
			if m.callHeap.Len() > 0 {
				call := m.callHeap.Top().(*rpcCall)
				m.resetExpiredTimer(call.expiredAt())
			} else {
				m.stopExpiredTimer()
			}
		}
	}
	return call
}

func (m *rpcManager) resetExpiredTimer(expiredAt int64) {
	d := time.Duration(expiredAt)*time.Millisecond - time.Duration(time.Now().UnixNano())
	m.expiredTimer.Reset(d)
}

func (m *rpcManager) stopExpiredTimer() {
	m.expiredTimer.Stop()
}

func (m *rpcManager) expiredTimerC() <-chan time.Time {
	return m.expiredTimer.C
}
