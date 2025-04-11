package common

import "github.com/godyy/gactor"

type MetaDriver struct {
	metas map[gactor.ActorUID]*gactor.Meta
}

func NewMetaDriver() *MetaDriver {
	return &MetaDriver{
		metas: make(map[gactor.ActorUID]*gactor.Meta),
	}
}

func (m *MetaDriver) AddMeta(uid gactor.ActorUID, meta *gactor.Meta) {
	m.metas[uid] = meta
}

// GetMeta 获取 Actor Meta 数据.
// 当 Meta 数据不存在时返回 ErrMetaNotExists.
func (m *MetaDriver) GetMeta(uid gactor.ActorUID) (*gactor.Meta, error) {
	meta, ok := m.metas[uid]
	if !ok {
		return nil, gactor.ErrMetaNotExists
	}

	return meta, nil
}
