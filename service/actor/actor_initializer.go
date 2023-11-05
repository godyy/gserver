package actor

import (
	"container/list"
	"sync"

	pkg_errors "github.com/pkg/errors"
)

func (s *Service) getInitializerById(id int64) *actorInitializer {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.initializers[id]
}

func (s *Service) getActorOrInitializerWithMeta(meta *Meta) (actor *Actor, ai *actorInitializer) {
	if actor = s.getActorById(meta.Id); actor != nil {
		return
	}

	if ai = s.getInitializerById(meta.Id); ai != nil {
		return
	}

	s.mtx.Lock()
	if actor = s.actors[meta.Id]; actor != nil {
		s.mtx.Unlock()
		return
	}
	ai = s.startInitializerWithMeta(meta)
	s.mtx.Unlock()

	return
}

func (s *Service) getActorOrInitializerWithId(id int64) (actor *Actor, ai *actorInitializer) {
	if actor = s.getActorById(id); actor != nil {
		return
	}

	if ai = s.getInitializerById(id); ai != nil {
		return
	}

	s.mtx.Lock()
	if actor = s.actors[id]; actor != nil {
		s.mtx.Unlock()
		return
	}
	ai = s.startInitializerWithId(id)
	s.mtx.Unlock()

	return
}

func (s *Service) startInitializerWithMeta(meta *Meta) *actorInitializer {
	ai := s.initializers[meta.Id]
	if ai != nil {
		return ai
	}

	ai = &actorInitializer{
		state:  initializing,
		id:     meta.Id,
		meta:   meta,
		chDone: make(chan struct{}),
	}
	go ai.initialize(s)
	s.initializers[meta.Id] = ai
	return ai
}

func (s *Service) startInitializerWithId(id int64) *actorInitializer {
	ai := s.initializers[id]
	if ai != nil {
		return ai
	}

	ai = &actorInitializer{
		state:  initializing,
		id:     id,
		chDone: make(chan struct{}),
	}
	go ai.initialize(s)
	s.initializers[id] = ai
	return ai
}

func (s *Service) onInitializerDone(ai *actorInitializer) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	delete(s.initializers, ai.id)
	if ai.err == nil {
		s.actors[ai.id] = ai.actor
	}
}

const (
	initializing      = 0
	initializeFailed  = 1
	initializeSuccess = 2
)

type actorInitializer struct {
	mtx      sync.Mutex
	state    int32
	id       int64
	meta     *Meta
	msgQueue *list.List
	actor    *Actor
	err      error
	chDone   chan struct{}
}

func (ai *actorInitializer) pushMsg(msg *cmdMsg) {
	ai.mtx.Lock()
	defer ai.mtx.Unlock()
	switch ai.state {
	case initializing:
		ai.msgQueue.PushBack(msg)
	case initializeFailed:
		msg.replyError(ai.err)
	case initializeSuccess:
		ai.actor.pushMsg(msg)
	}
}

func (ai *actorInitializer) initialize(service *Service) {
	meta := ai.meta

	if meta == nil {
		if err := service.GetMetaById(ai.id, meta); err != nil {
			ai.fail(service, pkg_errors.WithMessage(err, "get meta"))
			return
		}
	}

	if meta.NodeId == "" {
		// 未部署
		ai.fail(service, ErrActorNeedDeployed)
		return
	}

	if meta.NodeId != service.cluster.NodeId() {
		// 结点不匹配
		ai.fail(service, ErrActorDeployedOnOtherNode)
		return
	}

	cstr := service.getConstructor(meta.Category)
	if cstr == nil {
		ai.fail(service, ErrActorCategoryNotExists)
		return
	}

	actor := cstr.createActor(meta)
	if err := actor.Init(); err != nil {
		ai.fail(service, pkg_errors.WithMessage(err, "actor init"))
		return
	}

	ai.success(service, actor)
}

func (ai *actorInitializer) success(service *Service, actor *Actor) {
	ai.mtx.Lock()
	elem := ai.msgQueue.Front()
	for elem != nil {
		msg := elem.Value.(*cmdMsg)
		actor.pushMsg(msg)
		rElem := elem
		elem = elem.Next()
		ai.msgQueue.Remove(rElem)
	}
	ai.state = initializeSuccess
	ai.actor = actor
	close(ai.chDone)
	ai.mtx.Unlock()

	service.onInitializerDone(ai)
}

func (ai *actorInitializer) fail(service *Service, err error) {
	ai.mtx.Lock()
	ai.state = initializeFailed
	close(ai.chDone)
	ai.err = err
	ai.mtx.Unlock()

	service.onInitializerDone(ai)

	elem := ai.msgQueue.Front()
	for elem != nil {
		msg := elem.Value.(*cmdMsg)
		msg.replyError(err)
		rElem := elem
		elem = elem.Next()
		ai.msgQueue.Remove(rElem)
	}
}

func (ai *actorInitializer) waitDone() (*Actor, error) {
	<-ai.chDone
	return ai.actor, ai.err
}
