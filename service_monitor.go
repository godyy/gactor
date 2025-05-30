package gactor

import (
	"context"
)

// ServiceMonitor 封装用于更新 Service 监控数据的接口.
type ServiceMonitor interface {
	// MonitorActorAmount Actor 数量.
	// 参数 action 表示 Actor 动作, category 表示 Actor 分类, amount 表示增加的数量.
	MonitorActorAmount(action MonitorActorAction, category uint16, amount int64)

	// MonitorTimerAmount 定时器数量.
	// 参数 action 表示定时器动作, 参数 amount 表示数量.
	MonitorTimerAmount(action MonitorTimerAction, amount int64)

	// MonitorRPCAction RPC 调用动作.
	MonitorRPCAction(action MonitorCAAction)

	// MonitorCastAction Cast 调用动作.
	MonitorCastAction(action MonitorCAAction)
}

// MonitorKey 监控关键字.
type MonitorKey interface {
	MonitorKey() string
}

// MonitorActorAction Actor 动作.
type MonitorActorAction int

const (
	MonitorActorStart      = MonitorActorAction(iota + 1) // Actor 启动.
	MonitorActorStop                                      // Actor 停止.
	MonitorActorOnStartErr                                // Actor 启动回调错误.
	MonitorActorOnStopErr                                 // Actor 停止回调错误.
	MonitorActorPanic                                     // Actor panic.
)

var monitorActorActionKeys = [...]string{
	MonitorActorStart:      "start",
	MonitorActorStop:       "stop",
	MonitorActorOnStartErr: "on_start_err",
	MonitorActorOnStopErr:  "on_stop_err",
	MonitorActorPanic:      "panic",
}

func (a MonitorActorAction) MonitorKey() string {
	return monitorActorActionKeys[a]
}

// MonitorTimerAction 定时器动作.
type MonitorTimerAction int

const (
	MonitorTimerStart   = MonitorTimerAction(iota + 1) // 启动定时器.
	MonitorTimerStop                                   // 停止定时器.
	MonitorTimerTrigger                                // 触发定时器.
)

var monitorTimerActionKeys = [...]string{
	MonitorTimerStart:   "start",
	MonitorTimerStop:    "stop",
	MonitorTimerTrigger: "trigger",
}

func (a MonitorTimerAction) MonitorKey() string {
	return monitorTimerActionKeys[a]
}

// MonitorCAAction 与 Actor 通信动作.
type MonitorCAAction int

const (
	MonitorCARPC             = MonitorCAAction(iota + 1) // RPC 调用完成.
	MonitorCARPCTimeout                                  // RPC 调用超时.
	MonitorCACast                                        // Cast 调用完成.
	MonitorCANodeInfoErr                                 // 获取节点信息错误.
	MonitorCASend2RemoteErr                              // 发送到远端错误.
	MonitorCASend2LocalErr                               // 发送到本地错误.
	MonitorCAResponseErr                                 // 响应错误.
	MonitorCAContextTimeout                              // 上下文超时.
	MonitorCAContextCanceled                             // 上下文被取消.
	MonitorCAContextErr                                  // 其它上下文错误.
)

// monitorCAActionKeys MoniterCAAction 关键字映射.
var monitorCAActionKeys = [...]string{
	MonitorCARPC:             "rpc",
	MonitorCARPCTimeout:      "rpc_timeout",
	MonitorCACast:            "cast",
	MonitorCANodeInfoErr:     "node_info_err",
	MonitorCASend2RemoteErr:  "send_to_remote_err",
	MonitorCASend2LocalErr:   "send_to_local_err",
	MonitorCAResponseErr:     "response_err",
	MonitorCAContextTimeout:  "context_timeout",
	MonitorCAContextCanceled: "context_canceled",
	MonitorCAContextErr:      "context_err",
}

func (a MonitorCAAction) MonitorKey() string {
	return monitorCAActionKeys[a]
}

// contextErr2CAAction 上下文错误到MonitorCAAction的映射.
var contextErr2CAAction = map[error]MonitorCAAction{
	context.DeadlineExceeded: MonitorCAContextTimeout,
	context.Canceled:         MonitorCAContextCanceled,
}

func (s *Service) monitorActorStart(category uint16) {
	if monitor := s.cfg.Handler.GetMonitor(); monitor != nil {
		monitor.MonitorActorAmount(MonitorActorStart, category, 1)
	}
}

func (s *Service) monitorActorStop(category uint16) {
	if monitor := s.cfg.Handler.GetMonitor(); monitor != nil {
		monitor.MonitorActorAmount(MonitorActorStop, category, 1)
	}
}

func (s *Service) monitorActorOnStartErr(category uint16) {
	if monitor := s.cfg.Handler.GetMonitor(); monitor != nil {
		monitor.MonitorActorAmount(MonitorActorOnStartErr, category, 1)
	}
}

func (s *Service) monitorActorOnStopErr(category uint16) {
	if monitor := s.cfg.Handler.GetMonitor(); monitor != nil {
		monitor.MonitorActorAmount(MonitorActorOnStopErr, category, 1)
	}
}

func (s *Service) monitorActorPanic(category uint16) {
	if monitor := s.cfg.Handler.GetMonitor(); monitor != nil {
		monitor.MonitorActorAmount(MonitorActorPanic, category, 1)
	}
}

func (s *Service) monitorStartTimerAmount(amount int64) {
	if monitor := s.cfg.Handler.GetMonitor(); monitor != nil {
		monitor.MonitorTimerAmount(MonitorTimerStart, amount)
	}
}

func (s *Service) monitorStopTimerAmount(amount int64) {
	if monitor := s.cfg.Handler.GetMonitor(); monitor != nil {
		monitor.MonitorTimerAmount(MonitorTimerStop, amount)
	}
}

func (s *Service) monitorTriggerTimerAmount(amount int64) {
	if monitor := s.cfg.Handler.GetMonitor(); monitor != nil {
		monitor.MonitorTimerAmount(MonitorTimerTrigger, amount)
	}
}

func (s *Service) monitorRPCAction(action MonitorCAAction) {
	if monitor := s.cfg.Handler.GetMonitor(); monitor != nil {
		monitor.MonitorRPCAction(action)
	}
}

func (s *Service) monitorRPCActionContextErr(err error) {
	if err == nil {
		return
	}
	if monitor := s.cfg.Handler.GetMonitor(); monitor != nil {
		if action, ok := contextErr2CAAction[err]; ok {
			monitor.MonitorRPCAction(action)
		} else {
			monitor.MonitorRPCAction(MonitorCAContextErr)
		}
	}
}

func (s *Service) monitorRPCActionSend2RemoteErr(err error) {
	if err == nil {
		return
	}
	if monitor := s.cfg.Handler.GetMonitor(); monitor != nil {
		if action, ok := contextErr2CAAction[err]; ok {
			monitor.MonitorRPCAction(action)
		} else {
			monitor.MonitorRPCAction(MonitorCASend2RemoteErr)
		}
	}
}

func (s *Service) monitorRPCActionSend2LocalErr(err error) {
	if err == nil {
		return
	}
	if monitor := s.cfg.Handler.GetMonitor(); monitor != nil {
		if action, ok := contextErr2CAAction[err]; ok {
			monitor.MonitorRPCAction(action)
		} else {
			monitor.MonitorRPCAction(MonitorCASend2LocalErr)
		}
	}
}

func (s *Service) monitorCastAction(action MonitorCAAction) {
	if monitor := s.cfg.Handler.GetMonitor(); monitor != nil {
		monitor.MonitorCastAction(action)
	}
}

func (s *Service) monitorCastActionContextErr(err error) {
	if err == nil {
		return
	}
	if monitor := s.cfg.Handler.GetMonitor(); monitor != nil {
		if action, ok := contextErr2CAAction[err]; ok {
			monitor.MonitorCastAction(action)
		} else {
			monitor.MonitorCastAction(MonitorCAContextErr)
		}
	}
}

func (s *Service) monitorCastActionSend2RemoteErr(err error) {
	if err == nil {
		return
	}
	if monitor := s.cfg.Handler.GetMonitor(); monitor != nil {
		if action, ok := contextErr2CAAction[err]; ok {
			monitor.MonitorCastAction(action)
		} else {
			monitor.MonitorCastAction(MonitorCASend2RemoteErr)
		}
	}
}

func (s *Service) monitorCastActionSend2LocalErr(err error) {
	if err == nil {
		return
	}
	if monitor := s.cfg.Handler.GetMonitor(); monitor != nil {
		if action, ok := contextErr2CAAction[err]; ok {
			monitor.MonitorCastAction(action)
		} else {
			monitor.MonitorCastAction(MonitorCASend2LocalErr)
		}
	}
}
