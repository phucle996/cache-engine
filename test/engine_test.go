package cacheengine_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	cacheengine "cache-engine"
	cacheEngine_bus "cache-engine/bus"
	cacheEngine_local "cache-engine/local"
	cacheEngine_redis "cache-engine/redis"
	cacheEngine_registry "cache-engine/registry"
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

func TestInitEngine_Success(t *testing.T) {
	configContent := `[
		{"key": "user:profile", "ttl": "10m"},
		{"key": "system:settings", "ttl": "30s"}
	]`
	tmpFile, err := os.CreateTemp("", "cache_key_config_*.json")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(configContent); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	// 1. Test InitLocalEngine
	localMgr, err := cacheengine.InitLocalEngine(tmpFile.Name())
	if err != nil {
		t.Fatalf("InitLocalEngine failed: %v", err)
	}
	ttl, exists := localMgr.GetTTL("user:profile")
	if !exists || ttl != 10*time.Minute {
		t.Errorf("expected 10m TTL for user:profile, got %v", ttl)
	}

	// 2. Test InitRedisEngine
	redisMgr, err := cacheengine.InitRedisEngine(nil, tmpFile.Name())
	if err != nil {
		t.Fatalf("InitRedisEngine failed: %v", err)
	}
	ttl, exists = redisMgr.GetTTL("system:settings")
	if !exists || ttl != 30*time.Second {
		t.Errorf("expected 30s TTL for system:settings in redisMgr, got %v", ttl)
	}
}

func TestLocalSyncManager_MonotonicVersionAndCallsiteSync(t *testing.T) {
	l1Mock := NewMockL1()
	syncMgr := cacheEngine_local.NewLocalSyncManager(l1Mock)
	syncMgr.RegisterKeyConfig("test:key", 5*time.Minute)

	ctx := context.Background()

	// 1. Callsite receives a newer update: version 100
	payload100, _ := json.Marshal("Value at version 100")
	var msg cacheEngine_bus.FanoutMessage
	msg = cacheEngine_bus.FanoutMessage{
		Op:      "upsert",
		Key:     "test:key",
		Version: 100,
		Payload: payload100,
	}

	// Callsite logic:
	if msg.Op == "upsert" {
		_, _, _ = syncMgr.SetL1(ctx, msg.Key, msg.Payload, msg.Version)
	}

	_, ver, found := l1Mock.Get("test:key")
	if !found || ver != 100 {
		t.Errorf("expected version 100, got %v, found=%v", ver, found)
	}

	// 2. Callsite receives a stale update: version 90 (older) -> should be filtered by SetL1 logic
	payload90, _ := json.Marshal("Value at version 90")
	msg = cacheEngine_bus.FanoutMessage{
		Op:      "upsert",
		Key:     "test:key",
		Version: 90,
		Payload: payload90,
	}

	if msg.Op == "upsert" {
		_, _, _ = syncMgr.SetL1(ctx, msg.Key, msg.Payload, msg.Version)
	}

	_, ver, found = l1Mock.Get("test:key")
	if ver != 100 {
		t.Errorf("expected key to remain at version 100, got version %d", ver)
	}

	// 3. Callsite receives a delete message: version 105
	msg = cacheEngine_bus.FanoutMessage{
		Op:      "delete",
		Key:     "test:key",
		Version: 105,
	}

	if msg.Op == "delete" {
		_, _, _ = syncMgr.InvalidateL1(ctx, msg.Key, msg.Version)
	}

	_, _, found = l1Mock.Get("test:key")
	if found {
		t.Error("expected key to be deleted from L1 cache")
	}
}

func TestRedisSyncManager_GetSetInvalidateTaxonomy(t *testing.T) {
	// Create RedisSyncManager with l2 = nil
	syncMgr := cacheEngine_redis.NewRedisSyncManager(nil)
	syncMgr.RegisterKeyConfig("test:user", 1*time.Hour)

	ctx := context.Background()

	// 1. Get với unregistered key -> BYPASS
	val, err, errx, outcome := syncMgr.GetL2(ctx, "unregistered:key")
	if val != nil || err != nil || errx.Code != "UNREGISTERED_KEY" || outcome != "BYPASS" {
		t.Errorf("Get key chưa đăng ký bị lỗi: val=%v, err=%v, errx=%v, outcome=%v", val, err, errx, outcome)
	}

	// 2. Get L2 với registered key (L2 Miss)
	val, err, errx, outcome = syncMgr.GetL2(ctx, "test:user")
	if val != nil || err != nil || errx != nil || outcome != "L2_MISS" {
		t.Errorf("Get L2 Miss thất bại: val=%v, err=%v, errx=%v, outcome=%v", val, err, errx, outcome)
	}

	// Giả lập caller load từ DB rồi SetL2 vào cache
	err, errx, outcome = syncMgr.SetL2(ctx, "test:user", []byte("user-data"))
	if err != nil || errx != nil || outcome != "UPDATE" {
		t.Errorf("Caller SetL2 vào cache thất bại: err=%v, errx=%v, outcome=%v", err, errx, outcome)
	}

	// 3. InvalidateL2
	err, errx, outcome = syncMgr.InvalidateL2(ctx, "test:user")
	if err != nil || errx != nil || outcome != "DELETE" {
		t.Errorf("InvalidateL2 thất bại: err=%v, errx=%v, outcome=%v", err, errx, outcome)
	}
}

func TestKeyRegistry_WildcardAndLRU(t *testing.T) {
	reg := cacheEngine_registry.NewKeyRegistry(2) // Capacity = 2
	reg.Register("user:profile:*", 10*time.Minute)
	reg.Register("device:*:status", 5*time.Minute)
	reg.Register("system:settings", 30*time.Second)

	// 1. Test static key
	ttl, exists := reg.Resolve("system:settings")
	if !exists || ttl != 30*time.Second {
		t.Errorf("expected 30s TTL, got %v, exists=%v", ttl, exists)
	}

	// 2. Test wildcard key
	ttl, exists = reg.Resolve("user:profile:12345")
	if !exists || ttl != 10*time.Minute {
		t.Errorf("expected 10m TTL, got %v, exists=%v", ttl, exists)
	}

	// 3. Test another wildcard key
	ttl, exists = reg.Resolve("device:99:status")
	if !exists || ttl != 5*time.Minute {
		t.Errorf("expected 5m TTL, got %v, exists=%v", ttl, exists)
	}

	// 4. Test resolving another key matching the prefix pattern.
	// Since "user:profile:*" is a suffix wildcard, it uses prefix-based fast-path and does not enter LRU.
	ttl, exists = reg.Resolve("user:profile:67890")
	if !exists || ttl != 10*time.Minute {
		t.Errorf("expected 10m TTL, got %v, exists=%v", ttl, exists)
	}
}

func TestBus_PublishTTLFromRegistry(t *testing.T) {
	reg := cacheEngine_registry.NewKeyRegistry(10)
	reg.Register("user:profile:*", 10*time.Minute)
	busInstance := cacheEngine_bus.NewBus(nil, "test-channel", reg, 0) // 0 → dùng DefaultMaxMessageBytes

	// 1. Unregistered key phải bị từ chối
	err := busInstance.Publish(context.Background(), "unregistered:key", "upsert", []byte("{}"), 123)
	if err == nil {
		t.Error("expected error for unregistered key, got nil")
	}

	// 2. Op không hợp lệ phải bị từ chối ngay tại Publish (trước khi chạm Redis)
	err = busInstance.Publish(context.Background(), "user:profile:123", "INVALID_OP", []byte("{}"), 123)
	if err == nil {
		t.Error("expected error for invalid op, got nil")
	}

	// 3. Op hợp lệ nhưng client là nil → panic hoặc error từ Redis client, không phải từ validation
	defer func() {
		if r := recover(); r != nil {
			// Panic expected due to nil redis client — validation đã pass, lỗi đến từ transport
		}
	}()
	err = busInstance.Publish(context.Background(), "user:profile:123", "delete", []byte("{}"), 123)
	if err == nil {
		t.Error("expected error/panic due to nil redis client, got nil")
	}
}
