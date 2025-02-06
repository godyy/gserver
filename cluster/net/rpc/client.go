package rpc

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/godyy/gserver/cluster/net"
	"github.com/godyy/gutils/container/heap"
	pkg_errors "github.com/pkg/errors"
)

var ErrTimeout = errors.New("rpc: timeout")

type ClientHandler interface {
	RPCEncodeArgs(methodId uint16, args any) ([]byte, error)
	RPCDecodeReply(methodId uint16, replyBytes []byte) (any, error)
	RPCNewReqMsg(len int) *net.Msg
}

type Client struct {
	locker       *sync.Mutex
	reqId        uint64
	pending      map[uint64]*Call
	timeoutHeap  *heap.Heap[*Call]
	timeoutTimer *time.Timer

	handler ClientHandler
}

func NewClient(h ClientHandler) *Client {
	if h == nil {
		panic("rpc.NewClient: client handler nil")
	}

	cli := &Client{
		locker:      new(sync.Mutex),
		reqId:       0,
		pending:     make(map[uint64]*Call),
		timeoutHeap: heap.NewHeap[*Call](),
		handler:     h,
	}

	return cli
}

func (c *Client) genReqId() uint64 {
	return atomic.AddUint64(&c.reqId, 1)
}

func (c *Client) addPending(call *Call) {
	if _, ok := c.pending[call.id()]; ok {
		panic("rpc.Client: add duplicate pending call")
	}
	c.pending[call.id()] = call
}

func (c *Client) remPending(call *Call) {
	delete(c.pending, call.id())
}

func (c *Client) getPending(id uint64) *Call {
	return c.pending[id]
}

func (c *Client) addTimeout(call *Call) bool {
	if call != c.getPending(call.id()) {
		return false
	}
	c.timeoutHeap.Push(call)
	if topCall := c.timeoutHeap.Top(); topCall == call {
		c.resetTimeoutTimer(call.timeout)
	}
	return true
}

func (c *Client) topTimeout() *Call {
	if c.timeoutHeap.Len() <= 0 {
		return nil
	}
	return c.timeoutHeap.Top()
}

func (c *Client) remCall(call *Call) {
	c.timeoutHeap.Remove(call.HeapIndex())
	delete(c.pending, call.id())
}

func (c *Client) popPending(id uint64) *Call {
	call := c.pending[id]
	if call == nil {
		return nil
	}

	resetTimer := call == c.timeoutHeap.Top()

	c.remCall(call)

	if resetTimer {
		if c.timeoutHeap.Len() > 0 {
			c.resetTimeoutTimer(c.timeoutHeap.Top().timeout)
		} else {
			c.stopTimeoutTimer()
		}
	}

	return call
}

func (c *Client) resetTimeoutTimer(timeoutMs int64) {
	d := time.Duration(timeoutMs-time.Now().UnixMilli()) * time.Millisecond
	if c.timeoutTimer == nil {
		c.timeoutTimer = time.AfterFunc(d, c.onCallTimeout)
	} else {
		c.timeoutTimer.Reset(d)
	}
}

func (c *Client) stopTimeoutTimer() {
	if c.timeoutTimer == nil {
		return
	}
	c.timeoutTimer.Stop()
}

func (c *Client) onCallTimeout() {
	for {
		c.locker.Lock()

		call := c.topTimeout()
		if call == nil {
			c.locker.Unlock()
			break
		}

		if !call.isTimeout(time.Now().UnixMilli()) {
			c.resetTimeoutTimer(call.timeout)
			c.locker.Unlock()
			break
		}

		c.remCall(call)

		c.locker.Unlock()

		call.onError(ErrTimeout)
	}
}

func (c *Client) sendCall(conn net.Session, methodId uint16, args any, callback DoneCallback, timeout int64) error {
	if conn == nil {
		return errors.New("rpc: conn nil")
	}

	if args == nil {
		return errors.New("rpc: args nil")
	}

	if callback == nil {
		return errors.New("rpc: callback nil")
	}

	if timeout <= 0 {
		return ErrTimeout
	}

	var err error

	var argsBytes []byte
	if argsBytes, err = c.handler.RPCEncodeArgs(methodId, args); err != nil {
		return pkg_errors.WithMessagef(err, "encode method:%d args", methodId)
	}

	msg := c.handler.RPCNewReqMsg(reqIdLen + methodIdLen + len(argsBytes))

	reqId := c.genReqId()

	if err = encodeRequestMsg(msg, reqId, methodId, argsBytes); err != nil {
		return err
	}

	call := newCall(reqId, methodId, args, callback, timeout)

	c.locker.Lock()
	c.addPending(call)
	c.locker.Unlock()

	if err = conn.Send(msg); err != nil {
		c.locker.Lock()
		c.remPending(call)
		c.locker.Unlock()
		return pkg_errors.WithMessage(err, "send msg")
	} else {
		c.locker.Lock()
		c.addTimeout(call)
		c.locker.Unlock()
		return nil
	}
}

func (c *Client) AsyncCall(conn net.Session, methodId uint16, args any, timeout int64, callback DoneCallback) error {
	return c.sendCall(conn, methodId, args, callback, timeout)
}

func (c *Client) Call(conn net.Session, methodId uint16, args any, timeout int64) (any, error) {
	callback := syncDoneCallback{done: make(chan *Call, 1)}
	defer callback.close()
	if err := c.AsyncCall(conn, methodId, args, timeout, callback.onCallback); err != nil {
		return nil, err
	}
	call := <-callback.done
	return call.Reply, call.Error
}

func encodeRequestMsg(msg *net.Msg, reqId uint64, methodId uint16, argsBytes []byte) (err error) {
	if err = msg.WriteUint64(reqId); err != nil {
		return pkg_errors.WithMessage(err, "write reqId")
	}

	if err = msg.WriteUint16(methodId); err != nil {
		return pkg_errors.WithMessage(err, "write methodId")
	}

	if _, err = msg.Write(argsBytes); err != nil {
		return pkg_errors.WithMessage(err, "write args")
	}

	return nil
}

var ErrResponseNoReply = errors.New("rpc: response has no reply")

func (c *Client) HandleResponse(msg *net.Msg) (err error) {
	var rsp Response
	var call *Call

	if rsp.ReqId, err = msg.ReadUint64(); err != nil {
		return pkg_errors.WithMessage(err, "read reqId")
	}

	c.locker.Lock()
	call = c.popPending(rsp.ReqId)
	if call == nil {
		// Call already timeout
		c.locker.Unlock()
		return nil
	}
	c.locker.Unlock()

	if rsp.MethodId, err = msg.ReadUint16(); err != nil {
		return pkg_errors.WithMessage(err, "read methodId")
	}

	errorFlag := false
	if errorFlag, err = msg.ReadBool(); err != nil {
		return pkg_errors.WithMessage(err, "read error flag")
	}

	if errorFlag {

		if rsp.Error, err = msg.ReadString(); err != nil {
			return pkg_errors.WithMessage(err, "read error")
		}

		call.onError(errors.New(rsp.Error))

	} else {

		replyBytes := msg.UnreadData()
		if len(replyBytes) <= 0 {
			return ErrResponseNoReply
		}

		if rsp.Reply, err = c.handler.RPCDecodeReply(rsp.MethodId, replyBytes); err != nil {
			return pkg_errors.WithMessage(err, "decode reply")
		}

		call.onReply(rsp.Reply)

	}

	return nil
}

type DoneCallback func(*Call)

type Call struct {
	MethodId uint16 // 调用目标方法ID
	Args     any    // 参数
	Reply    any    // 返回值
	Error    error  // error
	//Done     chan *Call // channel for calls completed or timeout.

	reqId     uint64       // 请求ID
	timeout   int64        // 超时，毫秒
	heapIndex int          // 堆索引
	doneFlag  int32        // done flag
	callback  DoneCallback // callback
}

func newCall(reqId uint64, methodId uint16, args any, callback DoneCallback, timeout int64) *Call {
	if args == nil {
		panic("rpc.newCall: args nil")
	}

	if callback == nil {
		panic("rpc.newCall: callback nil")
	}

	call := &Call{
		MethodId:  methodId,
		Args:      args,
		reqId:     reqId,
		timeout:   time.Now().UnixMilli() + timeout,
		heapIndex: -1,
		doneFlag:  0,
		callback:  callback,
	}

	return call
}

func (c *Call) HeapLess(element heap.Element) bool {
	othCall := element.(*Call)
	return c.timeout < othCall.timeout
}

func (c *Call) SetHeapIndex(i int) {
	c.heapIndex = i
}

func (c *Call) HeapIndex() int {
	return c.heapIndex
}

func (c *Call) id() uint64 { return c.reqId }

func (c *Call) isTimeout(nowMilli int64) bool {
	return c.timeout <= nowMilli
}

func (c *Call) invokeCallback() {
	c.callback(c)
}

func (c *Call) done() bool {
	return atomic.CompareAndSwapInt32(&c.doneFlag, 0, 1)
}

func (c *Call) onReply(reply any) {
	if c.done() {
		c.Reply = reply
		c.invokeCallback()
	}
}

func (c *Call) onError(err error) {
	if c.done() {
		c.Error = err
		c.invokeCallback()
	}
}

type syncDoneCallback struct {
	done chan *Call
}

func (c syncDoneCallback) onCallback(call *Call) {
	c.done <- call
}

func (c syncDoneCallback) close() {
	close(c.done)
}
