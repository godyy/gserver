package actor

// import (
// 	"time"

// 	"github.com/godyy/gnet"
// )

// type Request interface {
// 	Actor() *Actor
// 	isTimeout(time.Time) bool
// 	Payload() *gnet.Packet
// 	Response(*gnet.Packet) error
// }

// type request struct {
// 	actor *Actor
// 	msg   *MsgRequest
// }

// func newRequest(actor *Actor, msg *MsgRequest) *request {
// 	return &request{
// 		actor: actor,
// 		msg:   msg,
// 	}
// }

// func (r *request) Actor() *Actor { return r.actor }

// func (r *request) isTimeout(_ time.Time) bool {
// 	return false
// }

// func (r *request) Payload() *gnet.Packet {
// 	return r.msg.Payload
// }

// func (r *request) Response(payload *gnet.Packet) error {
// 	rsp := NewMsgResponse(r.msg.ConnId, r.msg.ActorId, payload)
// 	return r.actor.conn.sendMsg(rsp)
// }

// type rpcRequest struct {
// 	actor *Actor
// 	conn  rpcConn
// 	msg   *MsgRPCRequest
// }

// func newRPCRequest(actor *Actor, conn rpcConn, msg *MsgRPCRequest) *rpcRequest {
// 	return &rpcRequest{
// 		conn: conn,
// 		msg:  msg,
// 	}
// }

// func (r *rpcRequest) Actor() *Actor { return r.actor }

// func (r *rpcRequest) isTimeout(now time.Time) bool {
// 	return now.UnixMilli() <= r.msg.ExpiredAt
// }

// func (r *rpcRequest) Payload() *gnet.Packet {
// 	return r.msg.Args
// }

// func (r *rpcRequest) Response(payload *gnet.Packet) error {
// 	rsp := NewMsgRPCResponseWithReply(r.msg.ReqId, r.msg.FromId, r.msg.ToId, payload)
// 	return r.conn.sendMsg(rsp)
// }
