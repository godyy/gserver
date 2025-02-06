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

type tAsyncArgs struct {
	Msg string
}

type tAsyncReply struct {
	Msg string
}

type tAsyncClient struct {
	*Client
	methods map[uint16]Method
}

func (t *tAsyncClient) RPCEncodeArgs(methodId uint16, args any) ([]byte, error) {
	method := t.methods[methodId]
	if method == nil {
		return nil, errors.New("method not found")
	}
	if err := method.CheckArgs(args); err != nil {
		return nil, err
	}
	return json.Marshal(args)
}

func (t *tAsyncClient) RPCDecodeReply(methodId uint16, replyBytes []byte) (any, error) {
	method := t.methods[methodId]
	if method == nil {
		return nil, errors.New("method not found")
	}
	reply := method.NewReply()
	if err := json.Unmarshal(replyBytes, reply); err != nil {
		return nil, pkg_errors.WithMessage(err, "unmarshal reply")
	}
	return reply, nil
}

func (t *tAsyncClient) RPCNewReqMsg(len int) *net.Msg {
	msg := net.GetMsg(net.protoTypeRaw, 1+len)
	_ = msg.WriteInt8(testRpcRequest)
	return msg
}

func (t *tAsyncClient) OnNewSession(session net.Session) {
}

func (t *tAsyncClient) OnSessionMsg(conn net.Session, msg *net.Msg) error {
	switch msg.MsgType() {
	case net.protoTypeRaw:
		rpcMsgType, err := msg.ReadInt8()
		if err != nil {
			return pkg_errors.WithMessage(err, "read rpc msg type")
		}
		if rpcMsgType != testRpcResponse {
			return errors.New("invalid rpc msg type")
		}
		if err := t.Client.HandleResponse(msg); err != nil {
			return pkg_errors.WithMessage(err, "handle rpc response")
		}

	default:
		return fmt.Errorf("invalid msg type: %v", msg.MsgType())
	}

	return nil
}

func (t *tAsyncClient) OnSessionClosed(session net.Session, err error) {
}

func (t *tAsyncClient) Async(conn net.Session, args *tAsyncArgs, timeout int64, callback func(reply *tAsyncReply, err error)) error {
	return t.Client.AsyncCall(conn, 0, args, timeout, func(call *Call) {
		if call.Error != nil {
			callback(nil, call.Error)
		} else {
			callback(call.Reply.(*tAsyncReply), nil)
		}
	})
}

type tAsyncServer struct {
	*Server
	methods map[uint16]Method
	chReq   chan *Request
}

func (t *tAsyncServer) RPCDecodeArgs(methodId uint16, argsBytes []byte) (any, error) {
	method := t.methods[methodId]
	if method == nil {
		return nil, errors.New("method not found")
	}
	args := method.NewArgs()
	if err := json.Unmarshal(argsBytes, args); err != nil {
		return nil, pkg_errors.WithMessage(err, "unmarshal args")
	}
	return args, nil
}

func (t *tAsyncServer) RPCEncodeReply(methodId uint16, reply any) ([]byte, error) {
	method := t.methods[methodId]
	if method == nil {
		return nil, errors.New("method not found")
	}
	if err := method.CheckReply(reply); err != nil {
		return nil, err
	}
	return json.Marshal(reply)
}

func (t *tAsyncServer) RPCNewRspMsg(len int) *net.Msg {
	msg := net.GetMsg(net.protoTypeRaw, 1+len)
	_ = msg.WriteInt8(testRpcResponse)
	return msg
}

func (t *tAsyncServer) RPCHandleRequest(req *Request) error {
	t.chReq <- req
	return nil
}

func (t *tAsyncServer) handleRequest() {
	for req := range t.chReq {
		method := t.methods[req.MethodId]
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

func (t *tAsyncServer) OnNewSession(s net.Session) {
}

func (t *tAsyncServer) OnSessionMsg(conn net.Session, msg *net.Msg) error {
	switch msg.MsgType() {
	case net.protoTypeRaw:
		rpcMsgType, err := msg.ReadInt8()
		if err != nil {
			return pkg_errors.WithMessage(err, "read rpc msg type")
		}
		if rpcMsgType != testRpcRequest {
			return errors.New("invalid rpc msg type")
		}
		if err := t.Server.HandleRequest(conn, msg); err != nil {
			return pkg_errors.WithMessage(err, "handle rpc request")
		}

	default:
		return fmt.Errorf("invalid msg type: %v", msg.MsgType())
	}

	return nil
}

func (t *tAsyncServer) OnSessionClosed(s net.Session, err error) {
}

func TestRpcAsync(t *testing.T) {
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
		0: NewMethod[tAsyncArgs, tAsyncReply](func(args *tAsyncArgs) (*tAsyncReply, error) {
			return &tAsyncReply{Msg: args.Msg}, nil
		}),
	}
	client := &tAsyncClient{
		methods: methods,
	}
	client.Client = NewClient(client)

	server := &tAsyncServer{
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

			call := true
			ch := make(chan func(), 1)

			for {
				if call {
					timeout := (50 + rand.Int63n(51)) * 100
					logger.Infof("async call %d will timeout by %dms", i, timeout)
					if err := client.Async(conn, &tAsyncArgs{Msg: fmt.Sprintf("async call %d", i)}, timeout, func(reply *tAsyncReply, err error) {
						ch <- func() {
							if err != nil {
								t.Fatalf("async call %d failed: %v", i, err)
							} else {
								logger.Infof("async call %d msg %s", i, reply.Msg)
							}
						}
					}); err != nil {
						t.Fatalf("async call %d failed: %v", i, err)
					}
					call = false
				} else {
					fn := <-ch
					fn()
					break
				}
			}
		}(i)
	}

	wg.Wait()

	clientService.Close()
	serverService.Close()
}
