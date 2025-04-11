package utils

import (
	"sync"
)

type ConcurrentMap[K comparable, V any] struct {
	sync.Map
}

func (m *ConcurrentMap[K, V]) Load(key K) (value V, ok bool) {
	if v, ok := m.Map.Load(key); ok {
		return v.(V), true
	} else {
		return value, false
	}
}

func (m *ConcurrentMap[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	if actual, loaded := m.Map.LoadOrStore(key, value); loaded {
		return actual.(V), true
	} else {
		return value, false
	}
}

func (m *ConcurrentMap[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	if val, loaded := m.Map.LoadAndDelete(key); loaded {
		return val.(V), true
	} else {
		return value, false
	}
}

func (m *ConcurrentMap[K, V]) Store(key K, value V) {
	m.Map.Store(key, value)
}

func (m *ConcurrentMap[K, V]) Delete(key K) {
	m.Map.Delete(key)
}

func (m *ConcurrentMap[K, V]) Traverse(f func(key K, value V) bool) {
	m.Map.Range(func(key, value any) bool {
		var k K
		var v V
		k = key.(K)
		if value != nil {
			v = value.(V)
		}
		return f(k, v)
	})
}
