package actor

import (
	"time"

	"github.com/godyy/gutils/container/heap"
)

// ScheduledTask 定时任务
type ScheduledTask struct {
	id            int32                 // 任务ID
	name          string                // 任务名称
	scheduledTime int64                 // 计划时间
	callback      ScheduledTaskCallback // 回调
	index         int
}

func (t *ScheduledTask) Id() int32 {
	return t.id
}

func (t *ScheduledTask) Name() string {
	return t.name
}

func (t *ScheduledTask) ScheduledTime() int64 {
	return t.scheduledTime
}

func (t *ScheduledTask) Less(element heap.Element) bool {
	tt := element.(*ScheduledTask)
	return t.scheduledTime < tt.scheduledTime
}

func (t *ScheduledTask) SetIndex(i int) {
	t.index = i
}

func (t *ScheduledTask) Index() int {
	return t.index
}

func (t *ScheduledTask) isExpired(now int64) bool {
	return t.scheduledTime <= now
}

func (t *ScheduledTask) call(entity Entity) {
	t.callback(entity, t)
}

// ScheduledTaskCallback 定时任务回调
type ScheduledTaskCallback func(Entity, *ScheduledTask)

// scheduledTaskManager 定时任务管理器
type scheduledTaskManager struct {
	actor     *Actor
	taskIdGen int32                    // 任务ID自增键
	taskMap   map[int32]*ScheduledTask // ScheduledTask map
	taskHeap  *heap.Heap               // ScheduledTask heap
	timer     *time.Timer              // 系统定时器
}

func newScheduledTaskManager(a *Actor) *scheduledTaskManager {
	tm := &scheduledTaskManager{
		actor:    a,
		taskMap:  map[int32]*ScheduledTask{},
		taskHeap: heap.NewHeap(),
	}
	return tm
}

func (tm *scheduledTaskManager) genTaskId() int32 {
	if tm.taskIdGen < 0 {
		tm.taskIdGen = 0
	}
	tm.taskIdGen++
	return tm.taskIdGen
}

// addTask 添加定时任务
func (tm *scheduledTaskManager) addTask(name string, scheduledTime int64, callback ScheduledTaskCallback) int32 {
	t := &ScheduledTask{
		id:            tm.genTaskId(),
		name:          name,
		scheduledTime: scheduledTime,
		callback:      callback,
		index:         -1,
	}
	tm.taskHeap.Push(t)
	tm.taskMap[t.Id()] = t
	if t.Index() == 0 {
		tm.resetTimer(scheduledTime)
	}
	return t.Id()
}

// delTask 删除定时任务
func (tm *scheduledTaskManager) delTask(id int32) {
	t := tm.taskMap[id]
	if t == nil {
		return
	}
	isTop := t.Index() == 0
	tm.taskHeap.Remove(t.Index())
	delete(tm.taskMap, id)
	if isTop {
		if tm.taskHeap.Len() > 0 {
			t = tm.taskHeap.Top().(*ScheduledTask)
			tm.resetTimer(t.scheduledTime)
		} else {
			tm.stopTimer()
		}
	}
}

// numOfTasks 获取任务数量
func (tm *scheduledTaskManager) numOfTasks() int {
	return tm.taskHeap.Len()
}

// updateTasks 更新任务
func (tm *scheduledTaskManager) updateTasks(entity Entity, now time.Time) {
	nowNano := now.UnixNano()
	for tm.taskHeap.Len() > 0 {
		timer := tm.taskHeap.Top().(*ScheduledTask)
		if timer.isExpired(nowNano) {
			tm.taskHeap.Pop()
			delete(tm.taskMap, timer.id)
			timer.call(entity)
		} else {
			tm.resetTimer(timer.scheduledTime)
			break
		}
	}
}

// resetTimer 重置定时器
func (tm *scheduledTaskManager) resetTimer(expiredAt int64) {
	d := time.Duration(expiredAt - time.Now().UnixNano())
	if tm.timer == nil {
		tm.timer = time.AfterFunc(d, tm.onTimer)
	} else {
		tm.timer.Reset(d)
	}
}

// stopTimer 停止定时器
func (tm *scheduledTaskManager) stopTimer() {
	if tm.timer != nil {
		tm.timer.Stop()
		tm.timer = nil
	}
}

func (tm *scheduledTaskManager) onTimer() {
	tm.actor.pushCmd(newCmdTimer())
}
