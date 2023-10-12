package session

import (
	"fmt"

	cmsg "github.com/godyy/gserver/cluster/msg"

	"github.com/godyy/gnet"
	"github.com/pkg/errors"
)

const (
	mtHandshake          = 0 // 握手
	mtHandshakeAck       = 1 // 握手确认
	mtHandshakeCompleted = 2 // 完成握手
	mtHandshakeReject    = 3 // 握手拒绝
	mtHeartbeat          = 4 // 心跳
	mtPayload            = 5 // 负载消息，实际的应用消息
)

// msg 定义一个消息需要实现的方法
type msg interface {
	msgType() int8
	size() int
	encode(ctx *Service, p *gnet.Packet) error
	decode(ctx *Service, p *gnet.Packet) error
	recycle()
}

var msgCreators = map[int8]func() msg{
	mtHandshake: func() msg {
		return &msgHandshake{}
	},
	mtHandshakeAck: func() msg {
		return &msgHandshakeAck{}
	},
	mtHandshakeCompleted: func() msg {
		return &msgHandshakeCompleted{}
	},
	mtHandshakeReject: func() msg {
		return &msgHandshakeReject{}
	},
	mtHeartbeat: func() msg {
		return &msgHeartbeat{}
	},
	mtPayload: func() msg {
		return &msgPayload{}
	},
}

// createMsg 根据类型创建消息
func createMsg(msgType int8) msg {
	creator := msgCreators[msgType]
	if creator == nil {
		return nil
	}
	return creator()
}

// msgHandshake 握手请求
type msgHandshake struct {
	NodeId string // 自身结点ID
	Token  string // 令牌
}

func (m *msgHandshake) msgType() int8 {
	return mtHandshake
}

func (m *msgHandshake) size() int {
	return 1 + 2 + len(m.NodeId) + 2 + len(m.Token)
}

func (m *msgHandshake) encode(ctx *Service, packet *gnet.Packet) error {
	if err := packet.WriteString(m.NodeId); err != nil {
		return errors.WithMessage(err, "encode NodeId")
	}

	if err := packet.WriteString(m.Token); err != nil {
		return errors.WithMessage(err, "encode Token")
	}

	return nil
}

func (m *msgHandshake) decode(ctx *Service, packet *gnet.Packet) error {
	var err error

	m.NodeId, err = packet.ReadString()
	if err != nil {
		return errors.WithMessage(err, "decode NodeId")
	}

	m.Token, err = packet.ReadString()
	if err != nil {
		return errors.WithMessage(err, "decode Token")
	}

	return nil
}

func (m *msgHandshake) recycle() {}

// msgHandshakeAck 握手确认
type msgHandshakeAck struct {
	NodeId string // 确认方结点ID
	Token  string // 令牌
}

func (m *msgHandshakeAck) msgType() int8 {
	return mtHandshakeAck
}

func (m *msgHandshakeAck) size() int {
	return 1 + 2 + len(m.NodeId) + 2 + len(m.Token)
}

func (m *msgHandshakeAck) encode(ctx *Service, packet *gnet.Packet) error {
	if err := packet.WriteString(m.NodeId); err != nil {
		return errors.WithMessage(err, "encode NodeId")
	}

	if err := packet.WriteString(m.Token); err != nil {
		return errors.WithMessage(err, "encode Token")
	}

	return nil
}

func (m *msgHandshakeAck) decode(ctx *Service, packet *gnet.Packet) error {
	var err error

	m.NodeId, err = packet.ReadString()
	if err != nil {
		return errors.WithMessage(err, "decode NodeId")
	}

	m.Token, err = packet.ReadString()
	if err != nil {
		return errors.WithMessage(err, "decode Token")
	}

	return nil
}

func (m *msgHandshakeAck) recycle() {}

// msgHandshakeCompleted 握手完成
type msgHandshakeCompleted struct {
}

func (m *msgHandshakeCompleted) msgType() int8 {
	return mtHandshakeCompleted
}

func (m *msgHandshakeCompleted) size() int {
	return 1
}

func (m *msgHandshakeCompleted) encode(ctx *Service, packet *gnet.Packet) error {
	return nil
}

func (m *msgHandshakeCompleted) decode(ctx *Service, packet *gnet.Packet) error {
	return nil
}

func (m *msgHandshakeCompleted) recycle() {}

// msgHandshakeReject 握手拒绝
type msgHandshakeReject struct {
	Reason string // 决绝的原因
}

func (m *msgHandshakeReject) msgType() int8 {
	return mtHandshakeReject
}

func (m *msgHandshakeReject) size() int {
	return 1 + 2 + len(m.Reason)
}

func (m *msgHandshakeReject) encode(ctx *Service, packet *gnet.Packet) error {
	if err := packet.WriteString(m.Reason); err != nil {
		return errors.WithMessage(err, "encode reason")
	}
	return nil
}

func (m *msgHandshakeReject) decode(ctx *Service, packet *gnet.Packet) error {
	var err error

	m.Reason, err = packet.ReadString()
	if err != nil {
		return errors.WithMessage(err, "decode reason")
	}

	return nil
}

func (m *msgHandshakeReject) recycle() {}

// msgHeartbeat 心跳
type msgHeartbeat struct {
	Ping bool
}

func (m *msgHeartbeat) msgType() int8 {
	return mtHeartbeat
}

func (m *msgHeartbeat) size() int {
	return 1 + 1
}

func (m *msgHeartbeat) encode(ctx *Service, packet *gnet.Packet) error {
	if err := packet.WriteBool(m.Ping); err != nil {
		return errors.WithMessage(err, "encode PingPong")
	}

	return nil
}

func (m *msgHeartbeat) decode(ctx *Service, packet *gnet.Packet) error {
	var err error

	m.Ping, err = packet.ReadBool()
	if err != nil {
		return errors.WithMessage(err, "decode PingPong")
	}

	return nil
}

func (m *msgHeartbeat) recycle() {}

// msgPayload 负载数据
type msgPayload struct {
	Payload cmsg.Msg // 消息负载
}

func (m *msgPayload) msgType() int8 {
	return mtPayload
}

func (m *msgPayload) size() int {
	return 1 + m.Payload.Size()
}

func (m *msgPayload) encode(ctx *Service, packet *gnet.Packet) error {
	if err := ctx.msgCodec.EncodeMsg(m.Payload, packet); err != nil {
		return errors.WithMessage(err, "encode payload")
	}
	return nil
}

func (m *msgPayload) decode(ctx *Service, packet *gnet.Packet) error {
	var err error

	m.Payload, err = ctx.msgCodec.DecodeMsg(packet)
	if err != nil {
		return errors.WithMessage(err, "decode payload")
	}

	return nil
}

func (m *msgPayload) recycle() {}

func (s *Service) encodeMsg(msg msg) (*gnet.Packet, error) {
	p := s.getPacket(msg.size())

	if err := p.WriteInt8(msg.msgType()); err != nil {
		return nil, errors.WithMessage(err, "encode msg type")
	}

	if err := msg.encode(s, p); err != nil {
		return nil, err
	}

	return p, nil
}

func (s *Service) decodeMsg(p *gnet.Packet) (msg, error) {
	var err error

	msgType := int8(0)
	msgType, err = p.ReadInt8()
	if err != nil {
		return nil, errors.WithMessage(err, "decode msg type")
	}

	mo := createMsg(msgType)
	if mo == nil {
		return nil, fmt.Errorf("invalid msg type %d", msgType)
	}

	if err = mo.decode(s, p); err != nil {
		return nil, err
	}

	return mo, nil
}
