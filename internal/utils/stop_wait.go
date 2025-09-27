package utils

import "sync"

// StopWait 停机等待工具.
type StopWait struct {
	C chan struct{}  // 停机信号.
	W sync.WaitGroup // 封装 WaitGroup, 用于等待所有 goroutine 退出.
}

func NewStopWait() *StopWait {
	return &StopWait{
		C: make(chan struct{}),
	}
}

// Stop 停机.
//
//	wait: 是否等待所有 goroutine 退出.
func (s *StopWait) Stop(wait bool) {
	close(s.C)
	s.W.Wait()
}
