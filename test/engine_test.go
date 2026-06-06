package cacheEngine_test

import (
	"context"
	"encoding/gob"
	"encoding/json"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cacheengine "cache-engine"
	cacheEngine_bus "cache-engine/bus"
	cacheEngine_codec "cache-engine/codec"
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

func TestLocalSyncManager_GetOrLoad_Singleflight(t *testing.T) {
	l1Mock := NewMockL1()
	syncMgr := cacheEngine_local.NewLocalSyncManager(l1Mock)
	syncMgr.RegisterKeyConfig("user:profile:*", 5*time.Minute)

	ctx := context.Background()
	var callCount int64

	// Tạo 20 goroutines gọi GetOrLoad đồng thời
	const concurrency = 20
	var wg sync.WaitGroup
	wg.Add(concurrency)

	results := make([]any, concurrency)
	errors := make([]error, concurrency)

	for i := 0; i < concurrency; i++ {
		go func(idx int) {
			defer wg.Done()
			val, err := syncMgr.GetOrLoad(ctx, "user:profile:123", func() (any, int64, error) {
				// Tăng counter
				atomic.AddInt64(&callCount, 1)
				// Giả lập độ trễ truy vấn DB
				time.Sleep(50 * time.Millisecond)
				return "DB_DATA", 999, nil
			})
			results[idx] = val
			errors[idx] = err
		}(i)
	}

	wg.Wait()

	// 1. Kiểm tra số lần gọi thực sự xuống loadFn
	if callCount != 1 {
		t.Errorf("expected loadFn to be called exactly 1 time, got %d times", callCount)
	}

	// 2. Tất cả các goroutine phải nhận được cùng một dữ liệu và không bị lỗi
	for i := 0; i < concurrency; i++ {
		if errors[i] != nil {
			t.Errorf("goroutine %d returned error: %v", i, errors[i])
		}
		if results[i] != "DB_DATA" {
			t.Errorf("goroutine %d expected 'DB_DATA', got %v", i, results[i])
		}
	}
}

func TestRedisSyncManager_GetOrLoad_Singleflight(t *testing.T) {
	// Khởi tạo RedisSyncManager với l2 = nil
	syncMgr := cacheEngine_redis.NewRedisSyncManager(nil)
	syncMgr.RegisterKeyConfig("user:profile:*", 5*time.Minute)

	ctx := context.Background()
	var callCount int64

	const concurrency = 20
	var wg sync.WaitGroup
	wg.Add(concurrency)

	results := make([][]byte, concurrency)
	errors := make([]error, concurrency)

	for i := 0; i < concurrency; i++ {
		go func(idx int) {
			defer wg.Done()
			val, err := syncMgr.GetOrLoad(ctx, "user:profile:123", func() ([]byte, error) {
				atomic.AddInt64(&callCount, 1)
				time.Sleep(50 * time.Millisecond)
				return []byte("REDIS_DATA"), nil
			})
			results[idx] = val
			errors[idx] = err
		}(i)
	}

	wg.Wait()

	if callCount != 1 {
		t.Errorf("expected loadFn to be called exactly 1 time, got %d times", callCount)
	}

	for i := 0; i < concurrency; i++ {
		if errors[i] != nil {
			t.Errorf("goroutine %d returned error: %v", i, errors[i])
		}
		if string(results[i]) != "REDIS_DATA" {
			t.Errorf("goroutine %d expected 'REDIS_DATA', got %s", i, string(results[i]))
		}
	}
}

func TestLocalSyncManager_Janitor_Eviction(t *testing.T) {
	l1Cache := cacheEngine_local.NewCOWCache()
	syncMgr := cacheEngine_local.NewLocalSyncManager(l1Cache)
	// Đóng janitor mặc định để chạy janitor test với tần suất nhanh (10ms)
	syncMgr.Close()

	syncMgr.RegisterKeyConfig("temp:*", 10*time.Millisecond)
	syncMgr.RegisterKeyConfig("persist:*", 1*time.Hour)

	ctx := context.Background()

	// Set 1 key chuẩn bị hết hạn nhanh, và 1 key sống lâu
	_, _, _ = syncMgr.SetL1(ctx, "temp:1", "value1", 100)
	_, _, _ = syncMgr.SetL1(ctx, "persist:1", "value2", 100)

	// Bắt đầu janitor quét mỗi 100ms
	syncMgr.StartJanitor(100 * time.Millisecond)
	defer syncMgr.Close()

	// Chờ 1100ms để key temp:1 chắc chắn hết hạn (clamped tối thiểu 1s do Jitter)
	time.Sleep(1100 * time.Millisecond)

	// Kiểm tra xem temp:1 đã biến mất hoàn toàn khỏi cache chưa
	_, _, ok := l1Cache.Get("temp:1")
	if ok {
		t.Error("expected temp:1 to be evicted by janitor, but it still exists")
	}

	// Key persist:1 vẫn phải còn tồn tại
	val, _, ok := l1Cache.Get("persist:1")
	if !ok || val != "value2" {
		t.Errorf("expected persist:1 to still exist with 'value2', got exist=%v, val=%v", ok, val)
	}
}

type TestUser struct {
	ID   int
	Name string
}

func init() {
	gob.Register(TestUser{})
}

func TestCodecs(t *testing.T) {
	user := TestUser{ID: 42, Name: "Alice"}

	// 1. Test JSONCodec
	jsonCodec := cacheEngine_codec.NewJSONCodec()
	data, err := jsonCodec.Marshal(user)
	if err != nil {
		t.Fatalf("JSON Marshal failed: %v", err)
	}
	var decodedJSON TestUser
	if err := jsonCodec.Unmarshal(data, &decodedJSON); err != nil {
		t.Fatalf("JSON Unmarshal failed: %v", err)
	}
	if decodedJSON != user {
		t.Errorf("expected %v, got %v", user, decodedJSON)
	}

	// 2. Test GobCodec
	gobCodec := cacheEngine_codec.NewGobCodec()
	dataGob, err := gobCodec.Marshal(user)
	if err != nil {
		t.Fatalf("Gob Marshal failed: %v", err)
	}
	var decodedGob TestUser
	if err := gobCodec.Unmarshal(dataGob, &decodedGob); err != nil {
		t.Fatalf("Gob Unmarshal failed: %v", err)
	}
	if decodedGob != user {
		t.Errorf("expected %v, got %v", user, decodedGob)
	}
}

func TestRedisSyncManager_GetOrLoadObject(t *testing.T) {
	// Khởi tạo RedisSyncManager với l2 = nil (chỉ test qua mock/no L2, fallback xuống DB)
	redisMgr := cacheEngine_redis.NewRedisSyncManager(nil)
	redisMgr.RegisterKeyConfig("user:*", 10*time.Minute)

	codec := cacheEngine_codec.NewGobCodec()
	redisMgr.SetCodec(codec)

	ctx := context.Background()
	var dbCalls int64

	// Lần đầu: Miss -> Gọi DB
	var user1 TestUser
	err := redisMgr.GetOrLoadObject(ctx, "user:123", &user1, func() (any, error) {
		atomic.AddInt64(&dbCalls, 1)
		return TestUser{ID: 123, Name: "Bob"}, nil
	})
	if err != nil {
		t.Fatalf("GetOrLoadObject failed: %v", err)
	}
	if user1.Name != "Bob" {
		t.Errorf("expected Bob, got %v", user1.Name)
	}
	if dbCalls != 1 {
		t.Errorf("expected dbCalls = 1, got %d", dbCalls)
	}
}
