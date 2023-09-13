package session

import (
	"fmt"

	"github.com/godyy/gnet"
	"github.com/pkg/errors"
)

const (
	msgHandshake          = 0 // 握手
	msgHandshakeAck       = 1 // 握手确认
	msgHandshakeCompleted = 2 // 完成握手
	msgHandshakeReject    = 3 // 握手拒绝
	msgHeartbeat          = 4 // 心跳
	msgApplication        = 5 // 应用消息
)

// message 定义一个消息需要实现的方法
type message interface {
	msgType() int8
	size() int
	encodePacket(*gnet.Packet) error
	decodePacket(*gnet.Packet) (bool, error)
}

var msgCreators = map[int8]func() message{
	msgHandshake: func() message {
		return &handshakeMsg{}
	},
	msgHandshakeAck: func() message {
		return &handshakeAckMsg{}
	},
	msgHandshakeCompleted: func() message {
		return &handshakeCompletedMsg{}
	},
	msgHandshakeReject: func() message {
		return &handshakeRejectMsg{}
	},
	msgHeartbeat: func() message {
		return &heartbeatMsg{}
	},
	msgApplication: func() message {
		return &applicationMsg{}
	},
}

// createMessage 根据类型创建消息
func createMessage(msgType int8) message {
	creator := msgCreators[msgType]
	if creator == nil {
		return nil
	}
	return creator()
}

// handshakeMsg 握手请求
type handshakeMsg struct {
	NodeId string // 自身结点ID
	Token  string // 令牌
}

func (m *handshakeMsg) msgType() int8 {
	return msgHandshake
}

func (m *handshakeMsg) size() int {
	return 1 + 2 + len(m.NodeId) + 2 + len(m.Token)
}

func (m *handshakeMsg) encodePacket(packet *gnet.Packet) error {
	if err := packet.WriteString(m.NodeId); err != nil {
		return errors.WithMessage(err, "encode NodeId")
	}

	if err := packet.WriteString(m.Token); err != nil {
		return errors.WithMessage(err, "encode Token")
	}

	return nil
}

func (m *handshakeMsg) decodePacket(packet *gnet.Packet) (bool, error) {
	var err error

	m.NodeId, err = packet.ReadString()
	if err != nil {
		return false, errors.WithMessage(err, "decode NodeId")
	}

	m.Token, err = packet.ReadString()
	if err != nil {
		return false, errors.WithMessage(err, "decode Token")
	}

	return false, nil
}

// handshakeAckMsg 握手确认
type handshakeAckMsg struct {
	NodeId string // 确认方结点ID
	Token  string // 令牌
}

func (m *handshakeAckMsg) msgType() int8 {
	return msgHandshakeAck
}

func (m *handshakeAckMsg) size() int {
	return 1 + 2 + len(m.NodeId) + 2 + len(m.Token)
}

func (m *handshakeAckMsg) encodePacket(packet *gnet.Packet) error {
	if err := packet.WriteString(m.NodeId); err != nil {
		return errors.WithMessage(err, "encode NodeId")
	}

	if err := packet.WriteString(m.Token); err != nil {
		return errors.WithMessage(err, "encode Token")
	}

	return nil
}

func (m *handshakeAckMsg) decodePacket(packet *gnet.Packet) (bool, error) {
	var err error

	m.NodeId, err = packet.ReadString()
	if err != nil {
		return false, errors.WithMessage(err, "decode NodeId")
	}

	m.Token, err = packet.ReadString()
	if err != nil {
		return false, errors.WithMessage(err, "decode Token")
	}

	return false, nil
}

// handshakeCompletedMsg 握手完成
type handshakeCompletedMsg struct {
}

func (m *handshakeCompletedMsg) msgType() int8 {
	return msgHandshakeCompleted
}

func (m *handshakeCompletedMsg) size() int {
	return 1
}

func (m *handshakeCompletedMsg) encodePacket(packet *gnet.Packet) error {
	return nil
}

func (m *handshakeCompletedMsg) decodePacket(packet *gnet.Packet) (bool, error) {
	return false, nil
}

// handshakeRejectMsg 握手拒绝
type handshakeRejectMsg struct {
	Reason string // 决绝的原因
}

func (m *handshakeRejectMsg) msgType() int8 {
	return msgHandshakeReject
}

func (m *handshakeRejectMsg) size() int {
	return 1 + 2 + len(m.Reason)
}

func (m *handshakeRejectMsg) encodePacket(packet *gnet.Packet) error {
	if err := packet.WriteString(m.Reason); err != nil {
		return errors.WithMessage(err, "encode reason")
	}
	return nil
}

func (m *handshakeRejectMsg) decodePacket(packet *gnet.Packet) (bool, error) {
	var err error

	m.Reason, err = packet.ReadString()
	if err != nil {
		return false, errors.WithMessage(err, "decode reason")
	}

	return false, nil
}

// heartbeatMsg 心跳
type heartbeatMsg struct {
	Ping bool
}

func (m *heartbeatMsg) msgType() int8 {
	return msgHeartbeat
}

func (m *heartbeatMsg) size() int {
	return 1 + 1
}

func (m *heartbeatMsg) encodePacket(packet *gnet.Packet) error {
	if err := packet.WriteBool(m.Ping); err != nil {
		return errors.WithMessage(err, "encode PingPong")
	}

	return nil
}

func (m *heartbeatMsg) decodePacket(packet *gnet.Packet) (bool, error) {
	var err error

	m.Ping, err = packet.ReadBool()
	if err != nil {
		return false, errors.WithMessage(err, "decode PingPong")
	}

	return false, nil
}

// applicationMsg 应用数据包
type applicationMsg struct {
	Payload *gnet.Packet // 数据负载
}

func (m *applicationMsg) msgType() int8 {
	return msgApplication
}

func (m *applicationMsg) size() int {
	return 1 + m.Payload.Readable()
}

func (m *applicationMsg) encodePacket(packet *gnet.Packet) error {
	if _, err := packet.ReadFrom(m.Payload); err != nil {
		return errors.WithMessage(err, "encode payload")
	}
	return nil
}

func (m *applicationMsg) decodePacket(packet *gnet.Packet) (bool, error) {
	m.Payload = packet
	return true, nil
}

func encodeMessage(msg message, p *gnet.Packet) error {
	p.Grow(msg.size())

	if err := p.WriteInt8(msg.msgType()); err != nil {
		return errors.WithMessage(err, "encode message type")
	}

	if err := msg.encodePacket(p); err != nil {
		return err
	}

	return nil
}

func decodeMessage(p *gnet.Packet) (message, bool, error) {
	var err error

	msgType := int8(0)
	msgType, err = p.ReadInt8()
	if err != nil {
		return nil, false, errors.WithMessage(err, "decode message type")
	}

	msg := createMessage(msgType)
	if msg == nil {
		return nil, false, fmt.Errorf("invalid message type %d", msgType)
	}

	var b bool
	b, err = msg.decodePacket(p)
	if err != nil {
		return nil, false, err
	}

	return msg, b, nil
}
