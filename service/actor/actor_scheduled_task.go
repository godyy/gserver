package actor

import (
	"math/rand"
	"time"
)

func (a *Actor) AddScheduledTask(name string, scheduledTime int64, callback ScheduledTaskCallback) int32 {
	a.mtx.Lock()
	defer a.mtx.Unlock()
	return a.scheduledTaskManager.addTask(name, scheduledTime, callback)
}

func (a *Actor) DelScheduledTask(id int32) {
	a.mtx.Lock()
	defer a.mtx.Unlock()
	a.scheduledTaskManager.delTask(id)
}

func (a *Actor) updateScheduledTask(now time.Time) {
	a.mtx.Lock()
	defer a.mtx.Unlock()
	a.scheduledTaskManager.updateTasks(a.entity, now)
}

func (a *Actor) schedulePersistTask(lastPersistAt int64) {
	a.persistAt = lastPersistAt
	if !a.entity.Persistent() {
		return
	}
	a.scheduledTaskManager.delTask(a.persistTaskId)
	persistAt := a.persistAt + a.service.config.GetActorPersistDelay() + rand.Int63n(5)*time.Second.Nanoseconds()
	a.persistTaskId = a.scheduledTaskManager.addTask("actor_persist", persistAt, a.onPersist)
}

func (a *Actor) onPersist(entity Entity, st *ScheduledTask) {
	if err := a.persist(); err != nil {
		// todo
	}

	a.mtx.Lock()
	defer a.mtx.Unlock()
	a.schedulePersistTask(st.scheduledTime)
}

func (a *Actor) scheduleExpireTask(lastActiveAt int64) {
	a.activeAt = lastActiveAt
	// todo 判断actor是否会过期
	a.scheduledTaskManager.delTask(a.expireTaskId)
	expiredAt := a.activeAt + a.service.config.GetActorExpireDelay()
	a.expireTaskId = a.scheduledTaskManager.addTask("actor_expire", expiredAt, a.onExpired)
}

func (a *Actor) onExpired(entity Entity, st *ScheduledTask) {
	// todo 过期，actor停机

	a.Stop()
}
