package msg

import "github.com/godyy/gnet"

// Codec Msg编解码器
type Codec interface {
	// EncodeMsg Msg编码
	EncodeMsg(Msg, *gnet.Packet) error

	// DecodeMsg Msg解码
	DecodeMsg(*gnet.Packet) (Msg, error)
}

// Msg 接口定义
type Msg interface {
	// Size 返回Msg大消息(字节)
	Size() int

	// Encode 将Msg编码并写入Packet
	// 若Msg内部嵌套了其它Msg实现，Codec可提供其编码支持
	Encode(Codec, *gnet.Packet) error

	// Decode 依据Packet中的数据解码Msg
	// 若Msg内部嵌套了其它Msg实现，Codec可提供解码支持
	Decode(Codec, *gnet.Packet) error

	// Recycle 回收Msg对象
	// 例如Msg使用对象池管理
	Recycle()
}
