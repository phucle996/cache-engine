package cacheEngine_test

import (
	"context"
	"encoding/gob"
	"time"
)

// MockL1 implements L1Cache interface.
type MockL1 struct {
	data    map[string]any
	version map[string]int64
}

func NewMockL1() *MockL1 {
	return &MockL1{
		data:    make(map[string]any),
		version: make(map[string]int64),
	}
}

func (m *MockL1) Get(key string) (any, int64, bool) {
	val, exists := m.data[key]
	ver := m.version[key]
	return val, ver, exists
}

func (m *MockL1) Set(key string, value any, ttl time.Duration, version int64) {
	m.data[key] = value
	m.version[key] = version
}

func (m *MockL1) Delete(key string, version int64) {
	delete(m.data, key)
	delete(m.version, key)
}

func (m *MockL1) Clear() {
	m.data = make(map[string]any)
	m.version = make(map[string]int64)
}

func (m *MockL1) GetOrLoad(ctx context.Context, key string, loadFn func() (value any, version int64, err error)) (any, error) {
	val, _, exists := m.Get(key)
	if exists {
		return val, nil
	}
	dbVal, version, err := loadFn()
	if err != nil {
		return nil, err
	}
	m.Set(key, dbVal, 5*time.Minute, version)
	return dbVal, nil
}

type TestUser struct {
	ID   int
	Name string
}

type DummyStruct struct {
	Name string
}

func init() {
	gob.Register(TestUser{})
	gob.Register(DummyStruct{})
}
