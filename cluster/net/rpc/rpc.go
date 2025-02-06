package rpc

import (
	"errors"
	"github.com/godyy/gserver/cluster/net"
	pkg_errors "github.com/pkg/errors"
)

var ErrAlreadyResponded = errors.New("rpc: request already responded")

const (
	reqIdLen    = 8
	methodIdLen = 2
)

type Request struct {
	ReqId    uint64
	MethodId uint16
	Args     any

	server    *Server
	conn      net.Session
	responded bool
}

func (req *Request) checkResponded() error {
	if req.responded {
		return ErrAlreadyResponded
	}
	return nil
}

func (req *Request) newRspMsg(errFlag bool, payloadLen int) *net.Msg {
	msg := req.server.newRspMsg(reqIdLen + methodIdLen + 1 + payloadLen)
	_ = msg.WriteUint64(req.ReqId)
	_ = msg.WriteUint16(req.MethodId)
	_ = msg.WriteBool(errFlag)
	return msg
}

func (req *Request) respond(msg *net.Msg) error {
	if err := req.conn.Send(msg); err != nil {
		return pkg_errors.WithMessage(err, "send response")
	}
	req.responded = true
	return nil
}

func (req *Request) Reply(reply any) (err error) {
	if err = req.checkResponded(); err != nil {
		return err
	}

	var replyBytes []byte
	if replyBytes, err = req.server.encodeReply(req.MethodId, reply); err != nil {
		return pkg_errors.WithMessage(err, "encode reply")
	}

	msg := req.newRspMsg(false, len(replyBytes))
	_, _ = msg.Write(replyBytes)

	return req.respond(msg)
}

func (req *Request) ReplyError(err error) error {
	if err == nil {
		panic("rpc.Request: Reply error nil")
	}

	if err := req.checkResponded(); err != nil {
		return err
	}

	errString := err.Error()
	msg := req.newRspMsg(true, len(errString))
	_ = msg.WriteString(errString)

	return req.respond(msg)
}

type Response struct {
	ReqId    uint64
	MethodId uint16
	Reply    any
	Error    string
}

type Method interface {
	NewArgs() any
	NewReply() any
	CheckArgs(args any) error
	CheckReply(reply any) error
	Call(args any) (reply any, err error)
}

var ErrInvalidArgsType = errors.New("rpc: invalid args type")
var ErrInvalidReplyType = errors.New("rpc: invalid reply type")

type MethodFunc[Args, Reply any] func(*Args) (*Reply, error)

type MethodT[Args, Reply any] struct {
	fn MethodFunc[Args, Reply]
}

func (m *MethodT[Args, Reply]) NewArgs() any {
	return new(Args)
}

func (m *MethodT[Args, Reply]) NewReply() any {
	return new(Reply)
}

func (m *MethodT[Args, Reply]) CheckArgs(args any) error {
	if _, ok := args.(*Args); !ok {
		return ErrInvalidArgsType
	}
	return nil
}

func (m *MethodT[Args, Reply]) CheckReply(reply any) error {
	if _, ok := reply.(*Reply); !ok {
		return ErrInvalidReplyType
	}
	return nil
}

func (m *MethodT[Args, Reply]) Call(args any) (reply any, err error) {
	return m.fn(args.(*Args))
}

func NewMethod[Args, Reply any](fn MethodFunc[Args, Reply]) Method {
	if fn == nil {
		panic("rpc.NewMethod: fn nil")
	}
	return &MethodT[Args, Reply]{
		fn: fn,
	}
}
