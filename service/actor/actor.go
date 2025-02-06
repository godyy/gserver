package actor

// import (
// 	"errors"
// 	"sync"
// 	"sync/atomic"
// 	"time"
// )

// var (
// 	ErrActorCategoryNotExists   = errors.New("actor category not exists")
// 	ErrActorDeployedOnOtherNode = errors.New("actor deployed on other node")
// )

// func (s *Service) IsCategoryExists(category string) bool {
// 	return s.getConstructor(category) != nil
// }

// func (s *Service) StartActor(meta *Meta) (actor *Actor, err error) {
// 	// 尝试添加Actor
// 	added := false
// 	added, err = s.AddMeta(meta)
// 	if err != nil {
// 		return nil, err
// 	}

// 	// 尝试部署
// 	if added || !meta.IsDeployed() {
// 		if err := s.DeployMeta(meta); err != nil {
// 			return nil, err
// 		}
// 	}

// 	// 检查结点
// 	if meta.NodeId != s.cluster.NodeId() {
// 		return nil, ErrActorDeployedOnOtherNode
// 	}

// 	ac, ai := s.getActorOrInitializerWithMeta(meta)
// 	if ac != nil {
// 		return ac, nil
// 	}
// 	return ai.waitDone()
// }

// func (s *Service) getActorById(id int64) *Actor {
// 	s.mtx.RLock()
// 	defer s.mtx.RUnlock()
// 	return s.actors[id]
// }

// func (s *Service) getConstructor(category string) *Constructor {
// 	return s.constructors[category]
// }

// func (s *Service) onActorStopping(a *Actor) {
// 	s.mtx.Lock()
// 	defer s.mtx.Unlock()
// 	s.stoppingActors[a.Id()] = a
// }

// func (s *Service) onActorStopped(a *Actor) {
// 	s.mtx.Lock()
// 	defer s.mtx.Unlock()
// 	delete(s.stoppingActors, a.Id())
// }

// // Entity Actor实体
// type Entity interface {
// 	OnInit(*Actor) error
// 	OnStop() error
// 	Persistent() bool
// 	OnPersist() error
// 	HandleRequest(Request) error
// }

// // EntityCreator Entity创建器
// type EntityCreator func() Entity

// // Constructor Actor构造器
// type Constructor struct {
// 	Category      string        // Actor类别
// 	EntityCreator EntityCreator // Entity创建器
// }

// func (c *Constructor) createActor(meta *Meta) *Actor {
// 	entity := c.createEntity()
// 	actor := newActor(meta, entity)
// 	return actor
// }

// func (c *Constructor) createEntity() Entity {
// 	return c.EntityCreator()
// }

// const (
// 	_ = iota
// 	actorInitialized
// 	actorStopping
// 	actorStopped
// )

// // Actor Actor的核心实现
// type Actor struct {
// 	service *Service

// 	locker                sync.Mutex
// 	meta                  *Meta        // actor元信息
// 	state                 atomic.Int32 // 状态
// 	*scheduledTaskManager              // 集成定时任务管理器
// 	entity                Entity       // 实体
// 	conn                  conn         // 逻辑连接
// 	cmdCond               *sync.Cond   // 命令条件
// 	cmdQueue              *cmdQueue    // 命令队列
// 	activeAt              int64        // 最近激活的时间
// 	persistAt             int64        // 最近持久化的时间
// 	persistTaskId         int32        // 持久化任务ID
// 	expireTaskId          int32        // 过期任务ID
// }

// func newActor(meta *Meta, entity Entity) *Actor {
// 	a := &Actor{
// 		meta:   meta,
// 		entity: entity,
// 	}

// 	a.scheduledTaskManager = newScheduledTaskManager(a)

// 	return a
// }

// func (a *Actor) Id() int64 { return a.meta.Id }

// func (a *Actor) Uuid() string { return a.meta.Uuid }

// func (a *Actor) Init() error {
// 	if a.state.CompareAndSwap(0, actorInitialized) {
// 		if err := a.entity.OnInit(a); err != nil {
// 			return err
// 		}

// 		a.cmdCond = sync.NewCond(&a.locker)
// 		a.cmdQueue = newCmdQueue()

// 		nowNano := time.Now().UnixNano()
// 		a.schedulePersistTask(nowNano)
// 		a.scheduleExpireTask(nowNano)

// 		go a.loop()
// 	}
// 	return nil
// }

// func (a *Actor) isRunning() bool {
// 	return a.state.Load() == actorInitialized
// }

// func (a *Actor) Close() error {
// 	if a.state.CompareAndSwap(actorInitialized, actorStopping) {
// 		a.service.onActorStopping(a)
// 		a.cmdCond.Signal()
// 	}
// 	return nil
// }

// func (a *Actor) doStop() {
// 	// todo

// 	if err := a.persist(); err != nil {
// 		// todo
// 	}

// 	if err := a.entity.OnStop(); err != nil {
// 		// todo
// 	}

// 	a.state.Store(actorStopped)

// 	a.service.onActorStopped(a)
// }

// func (a *Actor) updateConn(conn conn) bool {
// 	if !a.conn.isConnected() || a.conn.connId < conn.connId {
// 		if a.conn.isConnected() {
// 			a.conn.sendMsg(NewMsgConn(a.conn.connId, a.Id(), ConnKicked))
// 			a.conn.disconnect()
// 		}
// 		a.conn = conn
// 		return true
// 	} else {
// 		conn.sendMsg(NewMsgConn(conn.connId, a.Id(), ConnKicked))
// 		return false
// 	}
// }

// func (a *Actor) swapCmdQueue() *cmdQueue {
// 	a.locker.Lock()
// 	defer a.locker.Unlock()
// 	for {
// 		if a.cmdQueue.len() > 0 {
// 			cq := a.cmdQueue
// 			a.cmdQueue = newCmdQueue()
// 			return cq
// 		}

// 		if !a.isRunning() {
// 			return nil
// 		}

// 		a.cmdCond.WaitReply()
// 	}
// }

// func (a *Actor) loop() {
// 	for isRunning := true; isRunning; isRunning = a.isRunning() {
// 		cq := a.swapCmdQueue()
// 		if cq == nil {
// 			break
// 		}

// 		for cmd := cq.pop(); cmd != nil; cmd = cq.pop() {
// 			cmd.do(a)
// 		}
// 	}

// 	a.doStop()
// }

// func (a *Actor) persist() error {
// 	if !a.entity.Persistent() {
// 		return nil
// 	}
// 	return a.entity.OnPersist()
// }
