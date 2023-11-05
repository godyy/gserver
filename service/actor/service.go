package actor

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/godyy/gserver/cluster"

	cmsg "github.com/godyy/gserver/cluster/msg"
	"github.com/godyy/gserver/cluster/session"
)

// Service Actor Service
type Service struct {
	cluster *cluster.Cluster
	config  *ServiceConfig

	msgCodec     cmsg.Codec
	constructors map[string]*Constructor

	mtx            sync.RWMutex
	initializers   map[int64]*actorInitializer
	actors         map[int64]*Actor
	stoppingActors map[int64]*Actor

	*rpcManager

	md MetaDriver
}

func (s *Service) Start() error {
	s.cluster = cluster.CreateCluster()
}

func (s *Service) Stop() error {
	//TODO implement me
	panic("implement me")
}

func (s *Service) MsgCodec() cmsg.Codec { return s.msgCodec }

func (s *Service) loop() {
	for {
		select {
		case <-s.rpcManager.expiredTimerC():
			s.updateRPCCalls(time.Now())
		}
	}
}

func (s *Service) OnSessionMsg(session *session.Session, cm cmsg.Msg) error {
	mo, ok := cm.(msg)
	if !ok {
		return fmt.Errorf("invalid msg: %v", reflect.TypeOf(cm))
	}

	moduleMsgHandlerSingleton.handleMsg(s, cmdMsg{
		session: session,
		msg:     mo,
	})

	return nil
}

func (s *Service) OnSessionClosed(session *session.Session) {
	//TODO implement me
	panic("implement me")
}
