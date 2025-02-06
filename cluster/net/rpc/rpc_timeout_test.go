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

type tTimeoutArgs struct {
	Ts int64
}

type tTimeoutReply struct {
	Ts int64
}

type tTimeoutClient struct {
	*Client
	methods      map[uint16]Method
	serverNodeId string
}

func (t *tTimeoutClient) OnNewSession(s net.Session) {
}

func (t *tTimeoutClient) OnSessionMsg(conn net.Session, msg *net.Msg) error {
	switch msg.MsgType() {
	case net.protoTypeRaw:
		if conn.NodeId() != t.serverNodeId {
			return fmt.Errorf("invalid server node %s", conn.RemoteNodeId())
		}
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

func (t *tTimeoutClient) OnSessionClosed(s net.Session, err error) {
}

func (t *tTimeoutClient) RPCEncodeArgs(methodId uint16, args any) ([]byte, error) {
	method := t.methods[methodId]
	if method == nil {
		return nil, errors.New("method not found")
	}

	if err := method.CheckArgs(args); err != nil {
		return nil, err
	}

	return json.Marshal(args)
}

func (t *tTimeoutClient) RPCDecodeReply(methodId uint16, replyBytes []byte) (any, error) {
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

func (t *tTimeoutClient) RPCNewReqMsg(len int) *net.Msg {
	msg := net.GetMsg(net.protoTypeRaw, 1+len)
	_ = msg.WriteInt8(testRpcRequest)
	return msg
}

type tTimeoutServer struct {
	*Server
	methods map[uint16]Method
}

func (t *tTimeoutServer) OnNewSession(s net.Session) {
}

func (t *tTimeoutServer) OnSessionMsg(conn net.Session, msg *net.Msg) error {
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

func (t *tTimeoutServer) OnSessionClosed(s net.Session, err error) {
}

func (t *tTimeoutServer) RPCDecodeArgs(methodId uint16, argsBytes []byte) (any, error) {
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

func (t *tTimeoutServer) RPCEncodeReply(methodId uint16, reply any) ([]byte, error) {
	method := t.methods[methodId]
	if method == nil {
		return nil, errors.New("method not found")
	}

	if err := method.CheckReply(reply); err != nil {
		return nil, err
	}

	return json.Marshal(reply)
}

func (t *tTimeoutServer) RPCNewRspMsg(len int) *net.Msg {
	msg := net.GetMsg(net.protoTypeRaw, 1+len)
	_ = msg.WriteInt8(testRpcResponse)
	return msg
}

func (t *tTimeoutServer) RPCHandleRequest(req *Request) error {
	return nil
}

func TestRpcTimeout(t *testing.T) {
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
		0: NewMethod[tTimeoutArgs, tTimeoutReply](func(t *tTimeoutArgs) (*tTimeoutReply, error) {
			return nil, nil
		}),
	}
	client := &tTimeoutClient{
		methods:      methods,
		serverNodeId: serverName,
	}
	client.Client = NewClient(client)
	server := &tTimeoutServer{
		methods: methods,
	}
	server.Server = NewServer(server)

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

	t.Log("calling...")

	n := 10
	wg := &sync.WaitGroup{}

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			timeout := (50 + rand.Int63n(51)) * 100
			t.Logf("call %d will timeout by %dms", i, timeout)
			_, err := client.Call(conn, 0, &tTimeoutArgs{
				Ts: time.Now().UnixMilli(),
			}, timeout)
			if !errors.Is(err, ErrTimeout) {
				t.Fatalf("call %d not timeout", i)
			} else {
				t.Logf("call %d timouet", i)
			}
		}(i)
	}

	wg.Wait()

	clientService.Close()
	serverService.Close()
}
