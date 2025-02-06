package rpc

import (
	"errors"
	"github.com/godyy/gserver/cluster/net"
	pkg_errors "github.com/pkg/errors"
)

var ErrRequestNoArgs = errors.New("rpc: request has no args")

type ServerHandler interface {
	RPCDecodeArgs(methodId uint16, argsBytes []byte) (any, error)
	RPCEncodeReply(methodId uint16, reply any) ([]byte, error)
	RPCNewRspMsg(len int) *net.Msg
	RPCHandleRequest(req *Request) error
}

type Server struct {
	handler ServerHandler
}

func NewServer(h ServerHandler) *Server {
	if h == nil {
		panic("rpc.NewServer: server handler nil")
	}

	srv := &Server{
		handler: h,
	}

	return srv
}

func (s *Server) newRspMsg(len int) *net.Msg {
	return s.handler.RPCNewRspMsg(len)
}

func (s *Server) encodeReply(methodId uint16, reply any) ([]byte, error) {
	return s.handler.RPCEncodeReply(methodId, reply)
}

func (s *Server) HandleRequest(conn net.Session, msg *net.Msg) (err error) {
	var req Request

	if req.ReqId, err = msg.ReadUint64(); err != nil {
		return pkg_errors.WithMessage(err, "read reqId")
	}

	if req.MethodId, err = msg.ReadUint16(); err != nil {
		return pkg_errors.WithMessage(err, "read methodId")
	}

	argsBytes := msg.UnreadData()
	if len(argsBytes) <= 0 {
		return ErrRequestNoArgs
	}

	if req.Args, err = s.handler.RPCDecodeArgs(req.MethodId, argsBytes); err != nil {
		return pkg_errors.WithMessage(err, "decode args")
	}

	req.server = s
	req.conn = conn
	return s.handler.RPCHandleRequest(&req)
}
