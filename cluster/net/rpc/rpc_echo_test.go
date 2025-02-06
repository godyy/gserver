package rpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/godyy/gnet"
	"github.com/godyy/gserver/cluster/net"
	"github.com/godyy/gutils/log"
	pkg_errors "github.com/pkg/errors"
	"math/rand"
	"sync"
	"testing"
	"time"
)

const (
	testRpcRequest  = 1
	testRpcResponse = 2
)

type tEchoArgs struct {
	Msg string
}

type tEchoReply struct {
	Msg string
}

type tEchoClient struct {
	*Client
	methods      map[uint16]Method
	serverNodeId string
	chAsyncDone  chan *Call
}

func (c *tEchoClient) OnNewSession(s net.Session) {
}

func (c *tEchoClient) OnSessionMsg(conn net.Session, msg *net.Msg) error {
	switch msg.MsgType() {
	case net.protoTypeRaw:
		if conn.RemoteNodeId() != c.serverNodeId {
			return fmt.Errorf("invalid server node %s", conn.RemoteNodeId())
		}
		rpcMsgType, err := msg.ReadInt8()
		if err != nil {
			return pkg_errors.WithMessage(err, "read rpc msg type")
		}
		if rpcMsgType != testRpcResponse {
			return errors.New("invalid rpc msg type")
		}
		if err := c.Client.HandleResponse(msg); err != nil {
			return pkg_errors.WithMessage(err, "handle rpc response")
		}

	default:
		return fmt.Errorf("invalid msg type: %v", msg.MsgType())
	}

	return nil
}

func (c *tEchoClient) OnSessionClosed(s net.Session, err error) {
}

func (c *tEchoClient) RPCEncodeArgs(methodId uint16, args any) ([]byte, error) {
	method := c.methods[methodId]
	if method == nil {
		return nil, errors.New("method not found")
	}
	if err := method.CheckArgs(args); err != nil {
		return nil, err
	}
	return json.Marshal(args)
}

func (c *tEchoClient) RPCDecodeReply(methodId uint16, replyBytes []byte) (any, error) {
	method := c.methods[methodId]
	if method == nil {
		return nil, errors.New("method not found")
	}
	reply := method.NewReply()
	if err := json.Unmarshal(replyBytes, reply); err != nil {
		return nil, pkg_errors.WithMessage(err, "unmarshal reply")
	}
	return reply, nil
}

func (c *tEchoClient) RPCNewReqMsg(len int) *net.Msg {
	msg := net.GetMsg(net.protoTypeRaw, 1+len)
	_ = msg.WriteInt8(testRpcRequest)
	return msg
}

func (c *tEchoClient) Echo(conn net.Session, args *tEchoArgs, timeout int64) (*tEchoReply, error) {
	reply, err := c.Call(conn, 0, args, timeout)
	if err != nil {
		return nil, err
	}
	return reply.(*tEchoReply), nil
}

type tEchoServer struct {
	*Server
	methods map[uint16]Method
	chReq   chan *Request
}

func (s *tEchoServer) OnNewSession(conn net.Session) {
}

func (s *tEchoServer) OnSessionMsg(conn net.Session, msg *net.Msg) error {
	switch msg.MsgType() {
	case net.protoTypeRaw:
		rpcMsgType, err := msg.ReadInt8()
		if err != nil {
			return pkg_errors.WithMessage(err, "read rpc msg type")
		}
		if rpcMsgType != testRpcRequest {
			return errors.New("invalid rpc msg type")
		}
		if err := s.Server.HandleRequest(conn, msg); err != nil {
			return pkg_errors.WithMessage(err, "handle rpc request")
		}

	default:
		return fmt.Errorf("invalid msg type: %v", msg.MsgType())
	}

	return nil
}

func (s *tEchoServer) OnSessionClosed(conn net.Session, err error) {
}

func (s *tEchoServer) RPCDecodeArgs(methodId uint16, argsBytes []byte) (any, error) {
	method := s.methods[methodId]
	if method == nil {
		return nil, errors.New("method not found")
	}
	args := method.NewArgs()
	if err := json.Unmarshal(argsBytes, args); err != nil {
		return nil, pkg_errors.WithMessage(err, "unmarshal args")
	}
	return args, nil
}

func (s *tEchoServer) RPCEncodeReply(methodId uint16, reply any) ([]byte, error) {
	method := s.methods[methodId]
	if method == nil {
		return nil, errors.New("method not found")
	}
	if err := method.CheckReply(reply); err != nil {
		return nil, err
	}
	return json.Marshal(reply)
}

func (s *tEchoServer) RPCNewRspMsg(len int) *net.Msg {
	msg := net.GetMsg(net.protoTypeRaw, 1+len)
	_ = msg.WriteInt8(testRpcResponse)
	return msg
}

func (s *tEchoServer) RPCHandleRequest(req *Request) error {
	s.chReq <- req
	return nil
}

func (s *tEchoServer) handleRequest() {
	for req := range s.chReq {
		method := s.methods[req.MethodId]
		if method == nil {
			_ = req.ReplyError(errors.New("method not found"))
			continue
		}

		//_ = req.ReplyError(errors.New("123"))
		//continue

		reply, err := method.Call(req.Args)
		if err != nil {
			_ = req.ReplyError(err)
		} else {
			_ = req.Reply(reply)
		}
	}
}

func TestRpcEcho(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	logger, err := log.CreateLogger(&log.Config{
		Level:           log.DebugLevel,
		EnableCaller:    true,
		CallerSkip:      0,
		Development:     true,
		EnableStdOutput: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	serviceCfg := net.ServiceConfig{
		ListeningRetryDelay: 5000,
		HandshakeToken:      "handshake",
		HandshakeTimeout:    1000000,
		Session: net.SessionCfg{
			HeartbeatInterval: 50000000,
			InactiveTimeout:   50000000,
			Net: gnet.TcpSessionCfg{
				ReceiveTimeout:    100000000,
				SendTimeout:       100000000,
				SendBufferSize:    8192,
				ReceiveBufferSize: 8192,
				MaxPacketSize:     64 * 1024,
			},
		},
	}

	clientName := "rpc_client"
	serverName := "rpc_server"
	clientAddr := ":50001"
	serverAddr := ":50002"
	methods := map[uint16]Method{
		0: NewMethod[tEchoArgs, tEchoReply](func(args *tEchoArgs) (*tEchoReply, error) {
			return &tEchoReply{Msg: args.Msg}, nil
		}),
	}
	client := &tEchoClient{
		methods:      methods,
		serverNodeId: serverName,
	}
	client.Client = NewClient(client)
	server := &tEchoServer{
		methods: methods,
		chReq:   make(chan *Request, 100),
	}
	server.Server = NewServer(server)
	go server.handleRequest()

	clientService := net.CreateService(clientName, clientAddr, &serviceCfg, client, logger)
	serverService := net.CreateService(serverName, serverAddr, &serviceCfg, server, logger)

	if err := clientService.Start(); err != nil {
		t.Fatalf("client start failed: %v", err)
	}

	if err := serverService.Start(); err != nil {
		t.Fatalf("server start failed: %v", err)
	}

	time.Sleep(2 * time.Second)

	conn, err := clientService.Connect(serverName, serverAddr)
	if err != nil {
		t.Fatalf("client connect server: %v", err)
	}

	logger.Info("calling...")

	n := 100
	wg := &sync.WaitGroup{}

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			timeout := (50 + rand.Int63n(51)) * 100
			logger.Infof("call %d will timeout by %dms", i, timeout)
			reply, err := client.Echo(conn, &tEchoArgs{Msg: fmt.Sprintf("call %d", i)}, timeout)
			if err != nil {
				t.Fatalf("call %d failed: %v", i, err)
			} else {
				logger.Infof("call %d msg %s", i, reply.Msg)
			}
		}(i)
	}

	wg.Wait()

	clientService.Close()
	serverService.Close()
}
