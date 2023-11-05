package actor

import (
	"errors"
	"fmt"

	"github.com/godyy/gutils/buffer/bytes"

	cmsg "github.com/godyy/gserver/cluster/msg"

	"github.com/godyy/gnet"
	pkg_errors "github.com/pkg/errors"
)

const (
	_             = int8(iota)
	MTRequest     // Request，来自客户端的请求
	MTResponse    // Response，对应Request
	MTRPCRequest  // RPCRequest，actor之间的RPC请求
	MTRPCResponse // RPCResponse, actor之间的RPC响应
	MTForward     // Forward，消息透传，例如actor主动推送予客户端
	MTConn        // Conn，连接状态同步
	mtMax
)

type msg interface {
	cmsg.Msg
	msgType() int8
}

var msgCreators = map[int8]func() msg{
	MTRequest: func() msg {
		return &MsgRequest{}
	},
	MTResponse: func() msg {
		return &MsgResponse{}
	},
	MTRPCRequest: func() msg {
		return &MsgRPCRequest{}
	},
	MTRPCResponse: func() msg {
		return &MsgRPCResponse{}
	},
	MTForward: func() msg {
		return &MsgForward{}
	},
	MTConn: func() msg {
		return &MsgConn{}
	},
}

func createMsg(msgType int8) msg {
	creator := msgCreators[msgType]
	if creator() == nil {
		return nil
	}
	return creator()
}

type msgCodec struct{}

func (c *msgCodec) EncodeMsg(m cmsg.Msg, packet *gnet.Packet) error {
	mo, ok := m.(msg)
	if !ok {
		return errors.New("invalid msg")
	}

	if err := packet.WriteInt8(mo.msgType()); err != nil {
		return pkg_errors.WithMessage(err, "encode msg type")
	}

	return mo.Encode(nil, packet)
}

func (c *msgCodec) DecodeMsg(packet *gnet.Packet) (cmsg.Msg, error) {
	msgType, err := packet.ReadInt8()
	if err != nil {
		return nil, pkg_errors.WithMessage(err, "read msg type")
	}

	mo := createMsg(msgType)
	if mo == nil {
		return nil, fmt.Errorf("invalid msg type %d", msgType)
	}

	return mo, mo.Decode(nil, packet)
}

type MsgRequest struct {
	ConnId  int64
	ActorId int64
	Payload *gnet.Packet
}

func (m *MsgRequest) msgType() int8 {
	return MTRequest
}

func (m *MsgRequest) Size() int {
	return 8 + 8 + m.Payload.Readable()
}

func (m *MsgRequest) Encode(codec cmsg.Codec, packet *gnet.Packet) error {
	if err := packet.WriteInt64(m.ConnId); err != nil {
		return pkg_errors.WithMessage(err, "encode ConnId")
	}
	if err := packet.WriteInt64(m.ActorId); err != nil {
		return pkg_errors.WithMessage(err, "encode ActorId")
	}
	if _, err := m.Payload.WriteTo(packet); err != nil {
		return pkg_errors.WithMessage(err, "encode Payload")
	}
	return nil
}

func (m *MsgRequest) Decode(codec cmsg.Codec, packet *gnet.Packet) error {
	var err error

	m.ConnId, err = packet.ReadInt64()
	if err != nil {
		return pkg_errors.WithMessage(err, "decode ConnId")
	}

	m.ActorId, err = packet.ReadInt64()
	if err != nil {
		return pkg_errors.WithMessage(err, "decode ActorId")
	}

	m.Payload = packet
	return nil
}

func (m *MsgRequest) Recycle() {}

type MsgResponse struct {
	ConnId  int64
	ActorId int64
	Payload *gnet.Packet
}

func NewMsgResponse(connId int64, actorId int64, payload *gnet.Packet) *MsgResponse {
	return &MsgResponse{
		ConnId:  connId,
		ActorId: actorId,
		Payload: payload,
	}
}

func (m *MsgResponse) msgType() int8 {
	return MTResponse
}

func (m *MsgResponse) Size() int {
	return 8 + 8 + m.Payload.Readable()
}

func (m *MsgResponse) Encode(codec cmsg.Codec, packet *gnet.Packet) error {
	if err := packet.WriteInt64(m.ConnId); err != nil {
		return pkg_errors.WithMessage(err, "encode ConnId")
	}
	if err := packet.WriteInt64(m.ActorId); err != nil {
		return pkg_errors.WithMessage(err, "encode ActorId")
	}
	if _, err := m.Payload.WriteTo(packet); err != nil {
		return pkg_errors.WithMessage(err, "encode Payload")
	}
	return nil
}

func (m *MsgResponse) Decode(codec cmsg.Codec, packet *gnet.Packet) error {
	var err error

	m.ConnId, err = packet.ReadInt64()
	if err != nil {
		return pkg_errors.WithMessage(err, "decode ConnId")
	}

	m.ActorId, err = packet.ReadInt64()
	if err != nil {
		return pkg_errors.WithMessage(err, "decode ActorId")
	}

	m.Payload = packet
	return nil
}

func (m *MsgResponse) Recycle() {
	gnet.PutPacket(m.Payload)
}

type MsgRPCRequest struct {
	ReqId     int32
	FromId    int64
	ToId      int64
	IsCast    bool
	ExpiredAt int64
	Args      *gnet.Packet
}

func NewMsgRPCRequestCall(reqId int32, fromId, toId int64, expiredAt int64, args *gnet.Packet) *MsgRPCRequest {
	return &MsgRPCRequest{
		ReqId:     reqId,
		FromId:    fromId,
		ToId:      toId,
		ExpiredAt: expiredAt,
		Args:      args,
	}
}

func NewMsgRPCRequestCast(reqId int32, fromId, toId int64, args *gnet.Packet) *MsgRPCRequest {
	return &MsgRPCRequest{
		ReqId:  reqId,
		FromId: fromId,
		ToId:   toId,
		IsCast: true,
		Args:   args,
	}
}

func (m *MsgRPCRequest) msgType() int8 {
	return MTRPCRequest
}

func (m *MsgRPCRequest) Size() int {
	s := 4 + 8 + 8 + 1
	if !m.IsCast {
		s += 8
	}
	s += m.Args.Readable()
	return s
}

func (m *MsgRPCRequest) Encode(codec cmsg.Codec, packet *gnet.Packet) error {
	if err := packet.WriteInt32(m.ReqId); err != nil {
		return pkg_errors.WithMessage(err, "encode ReqId")
	}
	if err := packet.WriteInt64(m.FromId); err != nil {
		return pkg_errors.WithMessage(err, "encode FromId")
	}
	if err := packet.WriteInt64(m.ToId); err != nil {
		return pkg_errors.WithMessage(err, "encode ToId")
	}
	if err := packet.WriteBool(m.IsCast); err != nil {
		return pkg_errors.WithMessage(err, "encode IsCast")
	}
	if !m.IsCast {
		if err := packet.WriteInt64(m.ExpiredAt); err != nil {
			return pkg_errors.WithMessage(err, "encode ExpiredAt")
		}
	}
	if _, err := m.Args.WriteTo(packet); err != nil {
		return pkg_errors.WithMessage(err, "encode Args")
	}
	return nil
}

func (m *MsgRPCRequest) Decode(codec cmsg.Codec, packet *gnet.Packet) error {
	var err error

	m.ReqId, err = packet.ReadInt32()
	if err != nil {
		return pkg_errors.WithMessage(err, "decode ReqId")
	}

	m.FromId, err = packet.ReadInt64()
	if err != nil {
		return pkg_errors.WithMessage(err, "decode FromId")
	}

	m.ToId, err = packet.ReadInt64()
	if err != nil {
		return pkg_errors.WithMessage(err, "decode ToId")
	}

	m.IsCast, err = packet.ReadBool()
	if err != nil {
		return pkg_errors.WithMessage(err, "decode IsCast")
	}

	if !m.IsCast {
		m.ExpiredAt, err = packet.ReadInt64()
		if err != nil {
			return pkg_errors.WithMessage(err, "decode ExpiredAt")
		}
	}

	m.Args = packet
	return nil
}

func (m *MsgRPCRequest) Recycle() {}

type MsgRPCResponse struct {
	ReqId  int32
	FromId int64
	ToId   int64
	Reply  *gnet.Packet
	Error  string
}

func NewMsgRPCResponseWithReply(reqId int32, fromId, toId int64, reply *gnet.Packet) *MsgRPCResponse {
	return &MsgRPCResponse{
		FromId: fromId,
		ToId:   toId,
		ReqId:  reqId,
		Reply:  reply,
	}
}

func NewMsgRPCResponseWithError(reqId int32, fromId, toId int64, err string) *MsgRPCResponse {
	if err == "" {
		panic("err empty")
	}
	return &MsgRPCResponse{
		ReqId:  reqId,
		FromId: fromId,
		ToId:   toId,
		Error:  err,
	}
}

func (m *MsgRPCResponse) msgType() int8 {
	return MTRPCResponse
}

func (m *MsgRPCResponse) Size() int {
	size := 4 + 8 + 8 + 1
	if m.Error != "" {
		size += bytes.MaxStringLenLen + len(m.Error)
	} else {
		size += m.Reply.Readable()
	}
	return size
}

func (m *MsgRPCResponse) Encode(codec cmsg.Codec, packet *gnet.Packet) error {
	if err := packet.WriteInt32(m.ReqId); err != nil {
		return pkg_errors.WithMessage(err, "encode ReqId")
	}
	if err := packet.WriteInt64(m.FromId); err != nil {
		return pkg_errors.WithMessage(err, "encode FromId")
	}
	if err := packet.WriteInt64(m.ToId); err != nil {
		return pkg_errors.WithMessage(err, "encode ToId")
	}
	isError := m.Error != ""
	if err := packet.WriteBool(isError); err != nil {
		return pkg_errors.WithMessage(err, "encode Error flag")
	}
	if isError {
		if err := packet.WriteString(m.Error); err != nil {
			return pkg_errors.WithMessage(err, "encode Error")
		}
	} else {
		if _, err := m.Reply.WriteTo(packet); err != nil {
			return pkg_errors.WithMessage(err, "encode Reply")
		}
	}
	return nil
}

func (m *MsgRPCResponse) Decode(codec cmsg.Codec, packet *gnet.Packet) error {
	var err error

	m.ReqId, err = packet.ReadInt32()
	if err != nil {
		return pkg_errors.WithMessage(err, "decode ReqId")
	}

	m.FromId, err = packet.ReadInt64()
	if err != nil {
		return pkg_errors.WithMessage(err, "decode FromId")
	}

	m.ToId, err = packet.ReadInt64()
	if err != nil {
		return pkg_errors.WithMessage(err, "decode ToId")
	}

	isError := false
	isError, err = packet.ReadBool()
	if err != nil {
		return pkg_errors.WithMessage(err, "decode Error flag")
	}

	if isError {
		m.Error, err = packet.ReadString()
		if err != nil {
			return pkg_errors.WithMessage(err, "decode Error")
		}
	} else {
		m.Reply = packet
	}

	return nil
}

func (m *MsgRPCResponse) Recycle() {}

type MsgForward struct {
	FromId  int64
	ToId    int64
	Payload *gnet.Packet
}

func (m *MsgForward) msgType() int8 {
	return MTForward
}

func (m *MsgForward) Size() int {
	return 8 + 8 + m.Payload.Readable()
}

func (m *MsgForward) Encode(codec cmsg.Codec, packet *gnet.Packet) error {
	if err := packet.WriteInt64(m.FromId); err != nil {
		return pkg_errors.WithMessage(err, "encode FromId")
	}
	if err := packet.WriteInt64(m.ToId); err != nil {
		return pkg_errors.WithMessage(err, "encode ToId")
	}
	if _, err := m.Payload.WriteTo(packet); err != nil {
		return pkg_errors.WithMessage(err, "encode PayLoad")
	}
	return nil
}

func (m *MsgForward) Decode(codec cmsg.Codec, packet *gnet.Packet) error {
	var err error

	m.FromId, err = packet.ReadInt64()
	if err != nil {
		return pkg_errors.WithMessage(err, "decode FromId")
	}

	m.ToId, err = packet.ReadInt64()
	if err != nil {
		return pkg_errors.WithMessage(err, "decode ToId")
	}

	m.Payload = packet
	return nil
}

func (m *MsgForward) Recycle() {}

const (
	ConnConnected    = 1 // 连接成功
	ConnDisconnected = 2 // 连接断开
	ConnKicked       = 3 // 连接被踢出
)

type MsgConn struct {
	ConnId  int64
	ActorId int64
	Status  int8
}

func NewMsgConn(connId int64, actorId int64, status int8) *MsgConn {
	if status < ConnConnected || status > ConnKicked {
		panic("invalid status")
	}
	return &MsgConn{
		ConnId:  connId,
		ActorId: actorId,
		Status:  status,
	}
}

func (m *MsgConn) msgType() int8 {
	return MTConn
}

func (m *MsgConn) Size() int {
	return 8 + 8 + 1
}

func (m *MsgConn) Encode(codec cmsg.Codec, packet *gnet.Packet) error {
	if err := packet.WriteInt64(m.ConnId); err != nil {
		return pkg_errors.WithMessage(err, "encode ConnId")
	}
	if err := packet.WriteInt64(m.ActorId); err != nil {
		return pkg_errors.WithMessage(err, "encode ActorId")
	}
	if err := packet.WriteInt8(m.Status); err != nil {
		return pkg_errors.WithMessage(err, "encode Status")
	}
	return nil
}

func (m *MsgConn) Decode(codec cmsg.Codec, packet *gnet.Packet) error {
	var err error

	m.ConnId, err = packet.ReadInt64()
	if err != nil {
		return pkg_errors.WithMessage(err, "decode ConnId")
	}

	m.ActorId, err = packet.ReadInt64()
	if err != nil {
		return pkg_errors.WithMessage(err, "decode ActorId")
	}

	m.Status, err = packet.ReadInt8()
	if err != nil {
		return pkg_errors.WithMessage(err, "decode connected")
	}

	return nil
}

func (m *MsgConn) Recycle() {}
