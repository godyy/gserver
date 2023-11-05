package actor

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkNewZeroDelayTimer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tt := time.NewTimer(0)
		tt.Stop()
		select {
		case <-tt.C:
		default:
		}
	}
}

func TestScheduledTaskManager(t *testing.T) {
	stm := newScheduledTaskManager()

	for i := 0; i < 2; i++ {
		nowNano := time.Now().UnixNano()
		for j := 0; j < 1e2; j++ {
			timerId := fmt.Sprintf("timer:%d-%d", i, j)
			stm.addTask(timerId, nowNano+int64(j)*int64(time.Millisecond), func(entity Entity, st *ScheduledTask) {
				t.Logf("timer id:%d name:%s expired, now:%v, scheduled:%v", st.Id(), st.Name(), time.Now(), time.Unix(0, st.ScheduledTime()))
			})
		}
		for stm.numOfTasks() > 0 {
			now := <-stm.timerC()
			stm.updateTasks(nil, now)
		}
	}
}
