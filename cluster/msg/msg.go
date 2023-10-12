package msg

import "github.com/godyy/gnet"

type Msg interface {
	Size() int
	Encode(*gnet.Packet) error
	Decode(*gnet.Packet) error
	Recycle()
}

type Codec interface {
	EncodeMsg(Msg, *gnet.Packet) error
	DecodeMsg(*gnet.Packet) (Msg, error)
}
