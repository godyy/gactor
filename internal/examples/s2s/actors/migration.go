package actors

import (
	"sync"

	"github.com/godyy/gactor"
)

// migrationSnapshots 仅用于演示迁移回调的幂等读写方式。
// 真实集群应替换为数据库、KV 或对象存储等源目标节点都可访问的持久化存储。
var migrationSnapshots sync.Map

type migrationSnapshot struct {
	Name string
}

func migrationSnapshotKey(ctx gactor.ActorMigrationContext) string {
	return ctx.UID.String() + ":" + ctx.SourceLeaseID
}

func saveMigrationSnapshot(ctx gactor.ActorMigrationContext, snapshot migrationSnapshot) error {
	// 以 SourceLeaseID 作为迁移代际幂等键，重复 OnMigrateOut 只覆盖同一轮快照。
	migrationSnapshots.Store(migrationSnapshotKey(ctx), snapshot)
	return nil
}

func loadMigrationSnapshot(ctx gactor.ActorMigrationContext) (migrationSnapshot, bool) {
	value, ok := migrationSnapshots.Load(migrationSnapshotKey(ctx))
	if !ok {
		return migrationSnapshot{}, false
	}
	snapshot, ok := value.(migrationSnapshot)
	return snapshot, ok
}
