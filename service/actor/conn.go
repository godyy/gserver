package actor

// import (
// 	"github.com/godyy/gserver/cluster/session"
// )

// type Conn interface {
// 	sendMsg(msg) error
// }

// type conn struct {
// 	connId  int64           // 连接唯一ID
// 	session session.Session // 底层网络会话
// }

// func newConn(id int64, session session.Session) conn {
// 	return conn{
// 		connId:  id,
// 		session: session,
// 	}
// }

// func (c *conn) isConnected() bool {
// 	return c.session != nil
// }

// func (c *conn) disconnect() {
// 	if c.session == nil {
// 		return
// 	}
// 	c.session = nil
// }

// func (c *conn) sendMsg(m msg) error {
// 	if !c.isConnected() {
// 		return nil
// 	}
// 	return c.session.SendMsg(m)
// }

// type rpcConn struct {
// 	session session.Session // 底层网络会话
// }

// func newRPCConn(session session.Session) rpcConn {
// 	return rpcConn{session: session}
// }

// func (c rpcConn) sendMsg(m msg) error {
// 	return c.session.SendMsg(m)
// }
