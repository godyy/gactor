package gactor

import (
	"context"
	"time"
)

// ServiceMonitor 封装用于更新 Service 监控数据的接口.
type ServiceMonitor interface {
	// MonitorActorAmount Actor 数量.
	// 参数 action 表示 Actor 动作, category 表示 Actor 分类, amount 表示增加的数量.
	MonitorActorAmount(action MonitorActorAction, category ActorCategory, amount int64)

	// MonitorTimerAmount 定时器数量.
	// 参数 action 表示定时器动作, 参数 amount 表示数量.
	MonitorTimerAmount(action MonitorTimerAction, amount int64)

	// MonitorRPCAction RPC 调用动作.
	MonitorRPCAction(action MonitorCAAction)

	// MonitorCastAction Cast 调用动作.
	MonitorCastAction(action MonitorCAAction)
}

// ServiceMigrationMonitor 是可选的迁移监控扩展。
// 现有 ServiceMonitor 实现无需修改；只有实现该接口时才会收到迁移事件。
type ServiceMigrationMonitor interface {
	MonitorMigrationAction(action MonitorMigrationAction)
	MonitorMigrationConverged(duration time.Duration)
}

// MonitorMigrationAction 描述迁移运行时事件。
type MonitorMigrationAction int

const (
	MonitorMigrationStart           = MonitorMigrationAction(iota + 1) // 开始一轮迁移。
	MonitorMigrationWaitForSlot                                        // Actor 等待迁移并发槽位。
	MonitorMigrationRetry                                              // Actor 或启动消息安排重试。
	MonitorMigrationTakeOverSuccess                                    // 目标端完成抢占接管。
	MonitorMigrationTakeOverFailure                                    // 目标端接管或恢复失败。
	MonitorMigrationForwardSuccess                                     // 业务消息转发成功。
	MonitorMigrationForwardFailure                                     // 业务消息转发失败。
	MonitorMigrationForwardBusy                                        // 业务消息因转发背压被拒绝。
	MonitorMigrationControlDenied                                      // 迁移控制包来源校验失败。
	MonitorMigrationConverged                                          // 冻结计划收敛完成。
	MonitorMigrationFinalized                                          // 源节点迁移状态已清理。
)

var monitorMigrationActionKeys = [...]string{
	MonitorMigrationStart:           "start",
	MonitorMigrationWaitForSlot:     "wait_for_slot",
	MonitorMigrationRetry:           "retry",
	MonitorMigrationTakeOverSuccess: "takeover_success",
	MonitorMigrationTakeOverFailure: "takeover_failure",
	MonitorMigrationForwardSuccess:  "forward_success",
	MonitorMigrationForwardFailure:  "forward_failure",
	MonitorMigrationForwardBusy:     "forward_busy",
	MonitorMigrationControlDenied:   "control_denied",
	MonitorMigrationConverged:       "converged",
	MonitorMigrationFinalized:       "finalized",
}

func (a MonitorMigrationAction) MonitorKey() string {
	return monitorMigrationActionKeys[a]
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

func (s *Service) monitorActorStart(category ActorCategory) {
	if monitor := s.cfg.Handler.GetMonitor(); monitor != nil {
		monitor.MonitorActorAmount(MonitorActorStart, category, 1)
	}
}

func (s *Service) monitorMigrationAction(action MonitorMigrationAction) {
	monitor, ok := s.cfg.Handler.GetMonitor().(ServiceMigrationMonitor)
	if ok && monitor != nil {
		monitor.MonitorMigrationAction(action)
	}
}

func (s *Service) monitorMigrationConverged(duration time.Duration) {
	monitor, ok := s.cfg.Handler.GetMonitor().(ServiceMigrationMonitor)
	if ok && monitor != nil {
		monitor.MonitorMigrationAction(MonitorMigrationConverged)
		monitor.MonitorMigrationConverged(duration)
	}
}

func (s *Service) monitorActorStop(category ActorCategory) {
	if monitor := s.cfg.Handler.GetMonitor(); monitor != nil {
		monitor.MonitorActorAmount(MonitorActorStop, category, 1)
	}
}

func (s *Service) monitorActorOnStartErr(category ActorCategory) {
	if monitor := s.cfg.Handler.GetMonitor(); monitor != nil {
		monitor.MonitorActorAmount(MonitorActorOnStartErr, category, 1)
	}
}

func (s *Service) monitorActorOnStopErr(category ActorCategory) {
	if monitor := s.cfg.Handler.GetMonitor(); monitor != nil {
		monitor.MonitorActorAmount(MonitorActorOnStopErr, category, 1)
	}
}

func (s *Service) monitorActorPanic(category ActorCategory) {
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
