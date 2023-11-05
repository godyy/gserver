package actor

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

type testEntity struct {
	t *testing.T
}

func (t testEntity) OnInit(actor *Actor) error {
	return nil
}

func (t testEntity) OnStop() error {
	t.t.Log("OnStop", time.Now())
	return nil
}

func (t testEntity) Persistent() bool {
	return true
}

func (t testEntity) OnPersist() error {
	t.t.Log("OnPersist", time.Now())
	return nil
}

type testCmd struct {
	t    *testing.T
	name string
}

func (c testCmd) Do() {
	c.t.Logf("do cmd %s", c.name)
}

func TestActorScheduledTask(t *testing.T) {
	actor := newActor(&testEntity{t: t})
	wg := &sync.WaitGroup{}
	nowNano := time.Now().UnixNano()
	for i := 0; i < 1e2; i++ {
		wg.Add(1)
		actor.addTask(fmt.Sprintf("timer:%d", i), nowNano+int64(i)*int64(time.Millisecond), func(entity Entity, st *ScheduledTask) {
			t.Logf("timer id:%d name:%s expired, now:%v, scheduled:%v", st.Id(), st.Name(), time.Now(), time.Unix(0, st.ScheduledTime()))
			wg.Done()
		})
	}

	actor.Init()
	wg.Wait()

	time.Sleep(10 * time.Second)
}

func TestActorStop(t *testing.T) {
	actor := newActor(&testEntity{t: t})
	actor.Init()

	for i := 0; i < 50; i++ {
		actor.handleMsg(testCmd{t: t, name: fmt.Sprintf("cmd:%d", i)})
	}

	actor.Stop()
	for actor.state.Load() != actorStopped {
		time.Sleep(time.Millisecond)
	}
}
