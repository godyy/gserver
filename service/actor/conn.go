package actor

import (
	"github.com/godyy/gserver/cluster/session"
	"github.com/godyy/gserver/service"
)

type conn struct {
	connId  int64            // 连接唯一ID
	session *session.Session // 底层网络会话
}

func newConn(id int64, session *session.Session) *conn {
	return &conn{
		connId:  id,
		session: session,
	}
}

func (c *conn) sendMsg(m msg) error {
	return c.session.SendMsg(service.NewMsg(service.ModuleActor, m))
}

type rpcConn struct {
	session *session.Session // 底层网络会话
}

func newRPCConn(session *session.Session) rpcConn {
	return rpcConn{session: session}
}

func (c rpcConn) sendMsg(m msg) error {
	return c.session.SendMsg(service.NewMsg(service.ModuleActor, m))
}
