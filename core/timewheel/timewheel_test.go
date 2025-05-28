package timewheel

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 默认执行器
func defaultExecutor(f TimerFunc, args TimerArgs) {
	go f(args)
}

func TestNewTimeWheel(t *testing.T) {
	// 秒、分、时三层
	configs := []LevelConfig{
		{Name: "second", Span: time.Second, Slots: 60}, // 1分钟
		{Name: "minute", Span: time.Minute, Slots: 60}, // 1小时
		{Name: "hour", Span: time.Hour, Slots: 24},     // 24小时
	}

	_, err := NewTimeWheel(configs, defaultExecutor)
	if err != nil {
		t.Fatalf("Failed to create time wheel: %v", err)
	}

	// 测试无效配置
	invalidConfigs := []LevelConfig{
		{Name: "second", Span: time.Second, Slots: 0}, // 无效的槽位数
	}
	_, err = NewTimeWheel(invalidConfigs, defaultExecutor)
	if err == nil {
		t.Error("Expected error for invalid slots, got nil")
	}

	invalidConfigs = []LevelConfig{
		{Name: "second", Span: 0, Slots: 60}, // 无效的时间跨度
	}
	_, err = NewTimeWheel(invalidConfigs, defaultExecutor)
	if err == nil {
		t.Error("Expected error for invalid span, got nil")
	}

	// 测试层级跨度配置
	invalidLevelConfigs := []LevelConfig{
		{Name: "second", Span: time.Second, Slots: 60},
		{Name: "minute", Span: 2 * time.Minute, Slots: 60}, // 无效的层级跨度
	}
	_, err = NewTimeWheel(invalidLevelConfigs, defaultExecutor)
	if err == nil {
		t.Error("Expected error for invalid level span, got nil")
	}

	// 测试nil执行器
	_, err = NewTimeWheel(configs, nil)
	if err == nil {
		t.Error("Expected error for nil executor, got nil")
	}
}

func TestTimeWheelBasic(t *testing.T) {
	configs := []LevelConfig{
		{Name: "second", Span: time.Second, Slots: 60},
		{Name: "minute", Span: time.Minute, Slots: 60},
		{Name: "hour", Span: time.Hour, Slots: 24},
	}

	tw, err := NewTimeWheel(configs, defaultExecutor)
	if err != nil {
		t.Fatalf("Failed to create time wheel: %v", err)
	}

	// 测试添加无效定时器
	_, err = tw.AddTimer(TimerOptions{
		Delay:    0,
		Periodic: false,
		Func:     func(args TimerArgs) {},
	})
	if err != ErrDelayMustGreaterThanZero {
		t.Errorf("Expected ErrDelayMustGreaterThanZero, got %v", err)
	}

	_, err = tw.AddTimer(TimerOptions{
		Delay:    25 * time.Hour, // 超过最大跨度
		Periodic: false,
		Func:     func(args TimerArgs) {},
	})
	if err != ErrDelayExceedMaxSpan {
		t.Errorf("Expected ErrDelayExceedMaxSpan, got %v", err)
	}

	_, err = tw.AddTimer(TimerOptions{
		Delay:    time.Second,
		Periodic: false,
		Func:     nil,
	})
	if err != ErrTimerFuncIsNil {
		t.Errorf("Expected ErrTimerFuncIsNil, got %v", err)
	}
}

func TestTimeWheelTimerExecution(t *testing.T) {
	configs := []LevelConfig{
		{Name: "second", Span: time.Second, Slots: 60},
		{Name: "minute", Span: time.Minute, Slots: 60},
		{Name: "hour", Span: time.Hour, Slots: 24},
	}

	tw, err := NewTimeWheel(configs, defaultExecutor)
	if err != nil {
		t.Fatalf("Failed to create time wheel: %v", err)
	}

	// 测试一次性定时器
	var wg sync.WaitGroup
	wg.Add(1)
	timerExecuted := false

	timerId, err := tw.AddTimer(TimerOptions{
		Delay:    2 * time.Second,
		Periodic: false,
		Func: func(args TimerArgs) {
			timerExecuted = true
			wg.Done()
		},
	})
	if err != nil {
		t.Fatalf("Failed to add timer: %v", err)
	}

	// 推进时间
	for i := 0; i < 2; i++ {
		tw.Tick()
	}

	// 等待定时器执行
	wg.Wait()
	if !timerExecuted {
		t.Error("Timer was not executed")
	}

	// 测试定时器删除
	timerId, err = tw.AddTimer(TimerOptions{
		Delay:    2 * time.Second,
		Periodic: false,
		Func: func(args TimerArgs) {
			t.Error("Timer should not be executed")
		},
	})
	if err != nil {
		t.Fatalf("Failed to add timer: %v", err)
	}

	if !tw.RemoveTimer(timerId) {
		t.Error("Failed to remove timer")
	}

	for i := 0; i < 2; i++ {
		tw.Tick()
	}
}

func TestTimeWheelPeriodicTimer(t *testing.T) {
	configs := []LevelConfig{
		{Name: "second", Span: time.Second, Slots: 60},
		{Name: "minute", Span: time.Minute, Slots: 60},
		{Name: "hour", Span: time.Hour, Slots: 24},
	}

	tw, err := NewTimeWheel(configs, defaultExecutor)
	if err != nil {
		t.Fatalf("Failed to create time wheel: %v", err)
	}

	// 测试周期性定时器
	var wg sync.WaitGroup
	executionCount := 0
	expectedExecutions := 3

	wg.Add(expectedExecutions)
	_, err = tw.AddTimer(TimerOptions{
		Delay:    time.Second,
		Periodic: true,
		Func: func(args TimerArgs) {
			executionCount++
			wg.Done()
		},
	})
	if err != nil {
		t.Fatalf("Failed to add periodic timer: %v", err)
	}

	// 推进时间并等待执行
	for i := 0; i < expectedExecutions; i++ {
		tw.Tick()
		time.Sleep(100 * time.Millisecond) // 添加小延迟确保定时器执行完成
	}

	wg.Wait()
	if executionCount != expectedExecutions {
		t.Errorf("Expected %d executions, got %d", expectedExecutions, executionCount)
	}
}

func TestTimeWheelMultipleTimers(t *testing.T) {
	configs := []LevelConfig{
		{Name: "second", Span: time.Second, Slots: 60},
		{Name: "minute", Span: time.Minute, Slots: 60},
		{Name: "hour", Span: time.Hour, Slots: 24},
	}

	tw, err := NewTimeWheel(configs, defaultExecutor)
	if err != nil {
		t.Fatalf("Failed to create time wheel: %v", err)
	}

	// 测试多个定时器
	var wg sync.WaitGroup
	executionCount := int32(0)
	timerCount := 5

	wg.Add(timerCount)
	for i := 0; i < timerCount; i++ {
		_, err := tw.AddTimer(TimerOptions{
			Delay:    time.Duration(i+1) * time.Second,
			Periodic: false,
			Func: func(args TimerArgs) {
				atomic.AddInt32(&executionCount, 1)
				wg.Done()
			},
		})
		if err != nil {
			t.Fatalf("Failed to add timer %d: %v", i, err)
		}
	}

	// 推进时间并等待所有定时器执行
	for i := 0; i < timerCount; i++ {
		tw.Tick()
	}

	wg.Wait()
	if int(executionCount) != timerCount {
		t.Errorf("Expected %d executions, got %d", timerCount, executionCount)
	}
}

func TestTimeWheelRandomTimers(t *testing.T) {
	configs := []LevelConfig{
		{Name: "second", Span: time.Second, Slots: 60},
		{Name: "minute", Span: time.Minute, Slots: 60},
		{Name: "hour", Span: time.Hour, Slots: 24},
	}

	tw, err := NewTimeWheel(configs, defaultExecutor)
	if err != nil {
		t.Fatalf("Failed to create time wheel: %v", err)
	}

	const totalTicks = 2000000 // 大约5.5小时的tick数
	var executedTimers int32
	var addedTimers int32

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < totalTicks; i++ {
		// 50%概率添加新定时器
		if r.Float64() < 0.5 {
			remainingTicks := totalTicks - i

			// 在10分钟到5小时之间随机
			minDelay := 10 * time.Minute
			maxDelay := 5 * time.Hour
			delay := minDelay + time.Duration(r.Int63n(int64(maxDelay-minDelay)))

			// 如果随机出来的延时大于剩余时间，则修正延时
			if delay > time.Duration(remainingTicks)*time.Second {
				randomReduce := time.Duration(r.Intn(4)+1) * time.Second
				if randomReduce >= time.Duration(remainingTicks)*time.Second {
					delay = time.Duration(remainingTicks) * time.Second
				} else {
					delay = time.Duration(remainingTicks)*time.Second - randomReduce
				}
			}

			_, err := tw.AddTimer(TimerOptions{
				Delay:    delay,
				Periodic: false,
				Func: func(args TimerArgs) {
					atomic.AddInt32(&executedTimers, 1)
				},
			})
			if err != nil {
				t.Errorf("Failed to add timer at tick %d: %v", i, err)
				continue
			}
			atomic.AddInt32(&addedTimers, 1)
		}

		// 推进时间轮
		tw.Tick()
	}

	// 等待一小段时间确保所有定时器都执行完成
	time.Sleep(100 * time.Millisecond)

	addedCount := atomic.LoadInt32(&addedTimers)
	executedCount := atomic.LoadInt32(&executedTimers)

	if executedCount != addedCount {
		t.Errorf("Not all timers were executed. Added: %d, Executed: %d", addedCount, executedCount)
	}
}

func TestTimeWheelReset(t *testing.T) {
	configs := []LevelConfig{
		{Name: "second", Span: time.Second, Slots: 60},
		{Name: "minute", Span: time.Minute, Slots: 60},
		{Name: "hour", Span: time.Hour, Slots: 24},
	}

	tw, err := NewTimeWheel(configs, defaultExecutor)
	if err != nil {
		t.Fatalf("Failed to create time wheel: %v", err)
	}

	// 添加一些定时器
	var wg sync.WaitGroup
	timerCount := 5
	wg.Add(timerCount)

	// 添加一次性定时器
	for i := 0; i < timerCount; i++ {
		_, err := tw.AddTimer(TimerOptions{
			Delay:    time.Duration(i+1) * time.Second,
			Periodic: false,
			Func: func(args TimerArgs) {
				wg.Done()
			},
		})
		if err != nil {
			t.Fatalf("Failed to add timer %d: %v", i, err)
		}
	}

	// 添加一个周期性定时器
	periodicTimerId, err := tw.AddTimer(TimerOptions{
		Delay:    time.Second,
		Periodic: true,
		Func:     func(args TimerArgs) {},
	})
	if err != nil {
		t.Fatalf("Failed to add periodic timer: %v", err)
	}

	// 推进一些时间
	for i := 0; i < 3; i++ {
		tw.Tick()
	}

	// 重置时间轮
	tw.Reset()

	// 验证重置后的状态
	if tw.ticks != 0 {
		t.Errorf("Expected ticks to be 0 after reset, got %d", tw.ticks)
	}

	if tw.totalTickTime != 0 {
		t.Errorf("Expected totalTickTime to be 0 after reset, got %v", tw.totalTickTime)
	}

	// 验证定时器池是否被清空
	for _, pool := range tw.timerPools {
		pool.mtx.Lock()
		if len(pool.m) != 0 {
			t.Errorf("Expected timer pool to be empty after reset, got %d timers", len(pool.m))
		}
		pool.mtx.Unlock()
	}

	// 验证周期性定时器是否被清除
	if tw.RemoveTimer(periodicTimerId) {
		t.Error("Expected periodic timer to be removed after reset")
	}

	// 验证是否可以添加新定时器
	newTimerId, err := tw.AddTimer(TimerOptions{
		Delay:    time.Second,
		Periodic: false,
		Func:     func(args TimerArgs) {},
	})
	if err != nil {
		t.Errorf("Failed to add new timer after reset: %v", err)
	}

	// 验证新定时器的ID是否从1开始
	if newTimerId != 1 {
		t.Errorf("Expected new timer ID to be 1 after reset, got %d", newTimerId)
	}

	// 验证新定时器是否可以被正确执行
	var execWg sync.WaitGroup
	execWg.Add(1)
	_, err = tw.AddTimer(TimerOptions{
		Delay:    time.Second,
		Periodic: false,
		Func: func(args TimerArgs) {
			execWg.Done()
		},
	})
	if err != nil {
		t.Fatalf("Failed to add test timer after reset: %v", err)
	}

	tw.Tick()
	execWg.Wait()
}

func TestTimeWheelConcurrent(t *testing.T) {
	configs := []LevelConfig{
		{Name: "second", Span: time.Second, Slots: 60},
		{Name: "minute", Span: time.Minute, Slots: 60},
		{Name: "hour", Span: time.Hour, Slots: 24},
	}

	tw, err := NewTimeWheel(configs, defaultExecutor)
	if err != nil {
		t.Fatalf("Failed to create time wheel: %v", err)
	}

	const (
		goroutineCount    = 1000 // 并发goroutine数量
		timerPerGoroutine = 10   // 每个goroutine添加的定时器数
		totalTimers       = goroutineCount * timerPerGoroutine
	)

	var (
		wg             sync.WaitGroup
		executedTimers int32
		addedTimers    int32
		removedTimers  int32
		timerIds       = make([]uint64, totalTimers)
		timerIdsMtx    sync.Mutex
	)

	// 启动多个goroutine并发添加定时器
	wg.Add(goroutineCount)
	for i := 0; i < goroutineCount; i++ {
		go func(goroutineId int) {
			defer wg.Done()

			for j := 0; j < timerPerGoroutine; j++ {
				// 随机生成1-5秒的延迟
				delay := time.Duration(rand.Intn(5)+1) * time.Second

				timerId, err := tw.AddTimer(TimerOptions{
					Delay:    delay,
					Periodic: false,
					Func: func(args TimerArgs) {
						atomic.AddInt32(&executedTimers, 1)
					},
				})

				if err != nil {
					t.Errorf("Failed to add timer in goroutine %d: %v", goroutineId, err)
					continue
				}

				atomic.AddInt32(&addedTimers, 1)

				// 保存定时器ID用于后续删除
				timerIdsMtx.Lock()
				timerIds[goroutineId*timerPerGoroutine+j] = timerId
				timerIdsMtx.Unlock()
			}
		}(i)
	}
	wg.Wait()

	// 验证添加的定时器数量
	if atomic.LoadInt32(&addedTimers) != totalTimers {
		t.Errorf("Expected %d timers to be added, got %d", totalTimers, atomic.LoadInt32(&addedTimers))
	}

	// 并发删除一半的定时器
	wg.Add(goroutineCount / 2)
	for i := 0; i < goroutineCount/2; i++ {
		go func(goroutineId int) {
			defer wg.Done()

			// 每个goroutine删除两个定时器
			for j := 0; j < 2; j++ {
				timerId := timerIds[goroutineId*2+j]
				if tw.RemoveTimer(timerId) {
					atomic.AddInt32(&removedTimers, 1)
				}
			}
		}(i)
	}
	wg.Wait()

	// 验证删除的定时器数量
	expectedRemoved := goroutineCount
	if atomic.LoadInt32(&removedTimers) != int32(expectedRemoved) {
		t.Errorf("Expected %d timers to be removed, got %d", expectedRemoved, atomic.LoadInt32(&removedTimers))
	}

	// 推进时间轮直到所有定时器执行完成
	expectedExecuted := totalTimers - expectedRemoved
	for i := 0; i < 10; i++ { // 最多推进10秒
		tw.Tick()
		time.Sleep(time.Second)

		if atomic.LoadInt32(&executedTimers) == int32(expectedExecuted) {
			break
		}
	}

	// 验证执行的定时器数量
	if atomic.LoadInt32(&executedTimers) != int32(expectedExecuted) {
		t.Errorf("Expected %d timers to be executed, got %d", expectedExecuted, atomic.LoadInt32(&executedTimers))
	}

	// 验证所有定时器池是否为空
	for _, pool := range tw.timerPools {
		pool.mtx.Lock()
		if len(pool.m) != 0 {
			t.Errorf("Expected timer pool to be empty, got %d timers", len(pool.m))
		}
		pool.mtx.Unlock()
	}
}

func TestTimeWheelExtremeConcurrent(t *testing.T) {
	configs := []LevelConfig{
		{Name: "second", Span: time.Second, Slots: 60},
		{Name: "minute", Span: time.Minute, Slots: 60},
		{Name: "hour", Span: time.Hour, Slots: 24},
	}

	tw, err := NewTimeWheel(configs, defaultExecutor)
	if err != nil {
		t.Fatalf("Failed to create time wheel: %v", err)
	}

	const (
		goroutineCount    = 10000 // 更大的并发goroutine数量
		timerPerGoroutine = 100   // 每个goroutine添加更多定时器
		totalTimers       = goroutineCount * timerPerGoroutine
	)

	var (
		wg             sync.WaitGroup
		executedTimers int32
		addedTimers    int32
		removedTimers  int32
		timerIds       = make([]uint64, totalTimers)
		timerIdsMtx    sync.Mutex
	)

	// 启动大量goroutine并发添加定时器
	wg.Add(goroutineCount)
	for i := 0; i < goroutineCount; i++ {
		go func(goroutineId int) {
			defer wg.Done()

			for j := 0; j < timerPerGoroutine; j++ {
				// 随机生成1-10秒的延迟
				delay := time.Duration(rand.Intn(10)+1) * time.Second

				timerId, err := tw.AddTimer(TimerOptions{
					Delay:    delay,
					Periodic: false,
					Func: func(args TimerArgs) {
						atomic.AddInt32(&executedTimers, 1)
					},
				})

				if err != nil {
					t.Errorf("Failed to add timer in goroutine %d: %v", goroutineId, err)
					continue
				}

				atomic.AddInt32(&addedTimers, 1)

				timerIdsMtx.Lock()
				timerIds[goroutineId*timerPerGoroutine+j] = timerId
				timerIdsMtx.Unlock()
			}
		}(i)
	}
	wg.Wait()

	// 验证添加的定时器数量
	if atomic.LoadInt32(&addedTimers) != totalTimers {
		t.Errorf("Expected %d timers to be added, got %d", totalTimers, atomic.LoadInt32(&addedTimers))
	}

	// 并发删除一半的定时器
	wg.Add(goroutineCount / 2)
	for i := 0; i < goroutineCount/2; i++ {
		go func(goroutineId int) {
			defer wg.Done()

			// 每个goroutine删除200个定时器
			for j := 0; j < 200; j++ {
				timerId := timerIds[goroutineId*200+j]
				if tw.RemoveTimer(timerId) {
					atomic.AddInt32(&removedTimers, 1)
				}
			}
		}(i)
	}
	wg.Wait()

	// 验证删除的定时器数量
	expectedRemoved := goroutineCount * 100
	if atomic.LoadInt32(&removedTimers) != int32(expectedRemoved) {
		t.Errorf("Expected %d timers to be removed, got %d", expectedRemoved, atomic.LoadInt32(&removedTimers))
	}

	// 推进时间轮直到所有定时器执行完成
	expectedExecuted := totalTimers - expectedRemoved
	for i := 0; i < 20; i++ { // 最多推进20秒
		tw.Tick()
		time.Sleep(time.Second)

		if atomic.LoadInt32(&executedTimers) == int32(expectedExecuted) {
			break
		}
	}

	// 验证执行的定时器数量
	if atomic.LoadInt32(&executedTimers) != int32(expectedExecuted) {
		t.Errorf("Expected %d timers to be executed, got %d", expectedExecuted, atomic.LoadInt32(&executedTimers))
	}

	// 验证所有定时器池是否为空
	for _, pool := range tw.timerPools {
		pool.mtx.Lock()
		if len(pool.m) != 0 {
			t.Errorf("Expected timer pool to be empty, got %d timers", len(pool.m))
		}
		pool.mtx.Unlock()
	}
}

func TestTimeWheelEdgeCases(t *testing.T) {
	configs := []LevelConfig{
		{Name: "second", Span: time.Second, Slots: 60},
		{Name: "minute", Span: time.Minute, Slots: 60},
		{Name: "hour", Span: time.Hour, Slots: 24},
	}

	tw, err := NewTimeWheel(configs, defaultExecutor)
	if err != nil {
		t.Fatalf("Failed to create time wheel: %v", err)
	}

	// 测试边界时间
	testCases := []struct {
		name     string
		delay    time.Duration
		periodic bool
	}{
		{"最小延迟", time.Second, false},
		{"最大延迟", 24 * time.Hour, false},
		{"周期性最小延迟", time.Second, true},
		{"周期性最大延迟", 24 * time.Hour, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			timerId, err := tw.AddTimer(TimerOptions{
				Delay:    tc.delay,
				Periodic: tc.periodic,
				Func:     func(args TimerArgs) {},
			})
			if err != nil {
				t.Errorf("Failed to add timer with %s: %v", tc.name, err)
				return
			}

			// 验证定时器是否被正确添加
			if !tw.RemoveTimer(timerId) {
				t.Errorf("Failed to remove timer with %s", tc.name)
			}
		})
	}

	// 测试重复删除
	timerId, _ := tw.AddTimer(TimerOptions{
		Delay:    time.Second,
		Periodic: false,
		Func:     func(args TimerArgs) {},
	})

	if !tw.RemoveTimer(timerId) {
		t.Error("Failed to remove timer first time")
	}
	if tw.RemoveTimer(timerId) {
		t.Error("Should not be able to remove timer second time")
	}

	// 测试并发添加和删除同一个定时器
	timerId, _ = tw.AddTimer(TimerOptions{
		Delay:    time.Second,
		Periodic: false,
		Func:     func(args TimerArgs) {},
	})

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		tw.RemoveTimer(timerId)
	}()

	go func() {
		defer wg.Done()
		tw.RemoveTimer(timerId)
	}()

	wg.Wait()
}

// 性能基准测试
func BenchmarkTimeWheel(b *testing.B) {
	configs := []LevelConfig{
		{Name: "second", Span: time.Second, Slots: 60},
		{Name: "minute", Span: time.Minute, Slots: 60},
		{Name: "hour", Span: time.Hour, Slots: 24},
	}

	tw, err := NewTimeWheel(configs, defaultExecutor)
	if err != nil {
		b.Fatalf("Failed to create time wheel: %v", err)
	}

	// 基准测试：添加定时器
	b.Run("AddTimer", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tw.AddTimer(TimerOptions{
				Delay:    time.Duration(i%10+1) * time.Second,
				Periodic: false,
				Func:     func(args TimerArgs) {},
			})
		}
	})
	tw.Reset()

	// 基准测试：删除定时器
	b.Run("RemoveTimer", func(b *testing.B) {
		// 预先添加一些定时器
		timerIds := make([]uint64, b.N)
		for i := 0; i < b.N; i++ {
			timerId, _ := tw.AddTimer(TimerOptions{
				Delay:    time.Second,
				Periodic: false,
				Func:     func(args TimerArgs) {},
			})
			timerIds[i] = timerId
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tw.RemoveTimer(timerIds[i])
		}
	})
	tw.Reset()

	// 基准测试：并发添加定时器
	b.Run("ConcurrentAddTimer", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				tw.AddTimer(TimerOptions{
					Delay:    time.Second,
					Periodic: false,
					Func:     func(args TimerArgs) {},
				})
			}
		})
	})
	tw.Reset()

	// 基准测试：并发删除定时器
	b.Run("ConcurrentRemoveTimer", func(b *testing.B) {
		// 预先添加一些定时器
		timerIds := make([]uint64, b.N)
		for i := 0; i < b.N; i++ {
			timerId, _ := tw.AddTimer(TimerOptions{
				Delay:    time.Second,
				Periodic: false,
				Func:     func(args TimerArgs) {},
			})
			timerIds[i] = timerId
		}

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				tw.RemoveTimer(timerIds[i])
				i++
			}
		})
	})
	tw.Reset()

	// 基准测试：高并发负载下的Tick性能
	b.Run("TickUnderLoad", func(b *testing.B) {
		const (
			initialTimerCount = 1000000 // 初始100万个定时器
			goroutineCount    = 100000  // 10万个并发goroutine
		)

		// 使用原子计数器来追踪定时器数量
		var (
			activeTimers  int64
			addedTimers   int64
			removedTimers int64
		)

		// 使用sync.Map来存储定时器ID，避免并发map访问问题
		var timerMap sync.Map

		// 添加初始定时器
		for i := 0; i < initialTimerCount; i++ {
			delay := time.Duration(rand.Intn(10)+1) * time.Second
			timerId, err := tw.AddTimer(TimerOptions{
				Delay:    delay,
				Periodic: false,
				Func:     func(args TimerArgs) {},
			})
			if err != nil {
				b.Fatalf("Failed to add initial timer: %v", err)
			}
			timerMap.Store(timerId, struct{}{})
			atomic.AddInt64(&activeTimers, 1)
			atomic.AddInt64(&addedTimers, 1)
		}

		// 创建停止信号
		stopCh := make(chan struct{})
		var wg sync.WaitGroup

		// 启动goroutine持续添加定时器
		wg.Add(goroutineCount)
		for i := 0; i < goroutineCount; i++ {
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stopCh:
						return
					default:
						delay := time.Duration(rand.Intn(10)+1) * time.Second
						timerId, err := tw.AddTimer(TimerOptions{
							Delay:    delay,
							Periodic: false,
							Func:     func(args TimerArgs) {},
						})
						if err == nil {
							timerMap.Store(timerId, struct{}{})
							atomic.AddInt64(&activeTimers, 1)
							atomic.AddInt64(&addedTimers, 1)
						}
						time.Sleep(100 * time.Millisecond)
					}
				}
			}()
		}

		// 启动goroutine持续删除定时器
		wg.Add(goroutineCount)
		for i := 0; i < goroutineCount; i++ {
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stopCh:
						return
					default:
						// 随机遍历并删除一个定时器
						timerMap.Range(func(key, value interface{}) bool {
							timerId := key.(uint64)
							if tw.RemoveTimer(timerId) {
								timerMap.Delete(timerId)
								atomic.AddInt64(&activeTimers, -1)
								atomic.AddInt64(&removedTimers, 1)
							}
							return false // 只处理一个定时器
						})
						time.Sleep(100 * time.Millisecond)
					}
				}
			}()
		}

		// 执行Tick基准测试
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tw.Tick()
		}
		b.StopTimer()

		// 停止所有goroutine
		close(stopCh)
		wg.Wait()
	})
	tw.Reset()
}

// 基准测试：均匀分布的循环定时器下的Tick性能
func BenchmarkTickWithEvenlyDistributedTimers(b *testing.B) {
	configs := []LevelConfig{
		{Name: "second", Span: time.Second, Slots: 60},
		{Name: "minute", Span: time.Minute, Slots: 60},
		{Name: "hour", Span: time.Hour, Slots: 24},
		{Name: "day", Span: 24 * time.Hour, Slots: 1},
	}

	tw, err := NewTimeWheel(configs, defaultExecutor)
	if err != nil {
		b.Fatalf("Failed to create time wheel: %v", err)
	}

	const (
		totalTimers = 1000000 // 100万个定时器
		maxDelay    = 86400   // 最大延迟24小时（86400秒）
	)

	// 使用原子计数器来追踪定时器数量
	var (
		activeTimers int64
		addedTimers  int64
	)

	// 添加均匀分布的定时器
	for i := 0; i < totalTimers; i++ {
		// 计算均匀分布的延迟时间（1秒到24小时之间）
		delay := time.Duration((i%maxDelay)+1) * time.Second

		_, err := tw.AddTimer(TimerOptions{
			Delay:    delay,
			Periodic: true, // 设置为循环定时器
			Func: func(args TimerArgs) {
				// 空函数，只用于测试性能
			},
		})
		if err != nil {
			b.Fatalf("Failed to add timer: %v", err)
		}
		atomic.AddInt64(&activeTimers, 1)
		atomic.AddInt64(&addedTimers, 1)
	}

	// 验证添加的定时器数量
	if atomic.LoadInt64(&addedTimers) != totalTimers {
		b.Fatalf("Expected %d timers to be added, got %d", totalTimers, atomic.LoadInt64(&addedTimers))
	}

	// 预热
	for i := 0; i < 10; i++ {
		tw.Tick()
	}

	// 重置计时器
	b.ResetTimer()

	// 执行基准测试
	for i := 0; i < b.N; i++ {
		tw.Tick()
	}
}
