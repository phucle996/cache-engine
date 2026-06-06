package cacheEngine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cacheEngine_bus "github.com/phucle996/cache-engine/bus"
	cacheEngine_local "github.com/phucle996/cache-engine/local"
	cacheEngine_taxonomy "github.com/phucle996/cache-engine/taxonomy"
)

// TestLocalSyncManager_Coverage covers remaining paths in local COW cache and LocalSyncManager.
func TestLocalSyncManager_Coverage(t *testing.T) {
	// 1. COWCache direct methods
	cow := cacheEngine_local.NewCOWCache()
	cow.Set("k1", "v1", 10*time.Minute, 10)

	// Set stale version in COWCache directly
	cow.Set("k1", "v1-stale", 10*time.Minute, 5)
	if val, _, ok := cow.Get("k1"); !ok || val != "v1" {
		t.Error("expected k1 to remain v1")
	}

	// Delete with stale version
	cow.Delete("k1", 5)
	if val, _, ok := cow.Get("k1"); !ok || val != "v1" {
		t.Error("expected k1 to remain since delete version was stale")
	}

	// Delete with newer version when other keys exist (covers k != key loop in COWCache)
	cow.Set("k2", "v2", 10*time.Minute, 10)
	cow.Delete("k1", 12)
	if _, _, ok := cow.Get("k1"); ok {
		t.Error("expected k1 to be deleted")
	}
	if val, _, ok := cow.Get("k2"); !ok || val != "v2" {
		t.Error("expected k2 to remain")
	}

	// Delete non-existent key
	cow.Delete("non-existent", 10)

	// Clear cache
	cow.Clear()
	if _, _, ok := cow.Get("k2"); ok {
		t.Error("expected k2 to be cleared")
	}

	// EvictExpired when expiredCount == 0 (Fast-path)
	cow.Set("k3", "v3", 10*time.Minute, 10)
	evicted := cow.EvictExpired()
	if evicted != 0 {
		t.Errorf("expected 0 evicted keys, got %d", evicted)
	}

	// 2. LocalSyncManager methods
	l1Mock := NewMockL1()
	syncMgr := cacheEngine_local.NewLocalSyncManager(l1Mock)
	syncMgr.Close() // close default janitor

	// KeyRegistry method
	reg := syncMgr.KeyRegistry()
	if reg == nil {
		t.Error("expected KeyRegistry to return registry instance")
	}

	// Register key config
	syncMgr.RegisterKeyConfig("key:1", 5*time.Minute)

	// GetL1 - Hit
	_, _, _ = syncMgr.SetL1(context.Background(), "key:1", "val:1", 1)
	val, err, terr, outcome := syncMgr.GetL1(context.Background(), "key:1")
	if outcome != cacheEngine_taxonomy.OutcomeL1Hit || val != "val:1" || err != nil || terr != nil {
		t.Errorf("GetL1 Hit failed: outcome=%v, val=%v, err=%v, terr=%v", outcome, val, err, terr)
	}

	// GetOrLoad - Cache Hit Fast Path
	valOrLoaded, errOrLoaded := syncMgr.GetOrLoad(context.Background(), "key:1", func() (any, int64, error) {
		return "should not be called", 1, nil
	})
	if errOrLoaded != nil || valOrLoaded != "val:1" {
		t.Errorf("expected Cache Hit, got val=%v, err=%v", valOrLoaded, errOrLoaded)
	}

	// GetL1 - Bypass (unregistered)
	val, err, terr, outcome = syncMgr.GetL1(context.Background(), "unregistered")
	if outcome != cacheEngine_taxonomy.OutcomeBypass {
		t.Errorf("expected Bypass for unregistered key, got %v", outcome)
	}

	// GetL1 - Miss
	syncMgr.RegisterKeyConfig("key:2", 5*time.Minute)
	val, err, terr, outcome = syncMgr.GetL1(context.Background(), "key:2")
	if outcome != cacheEngine_taxonomy.OutcomeL1Miss || val != nil || err != nil || terr != nil {
		t.Errorf("GetL1 Miss failed: outcome=%v, val=%v, err=%v, terr=%v", outcome, val, err, terr)
	}

	// SetL1 - Unregistered key
	err, terr, outcome = syncMgr.SetL1(context.Background(), "unregistered", "val", 1)
	if outcome != cacheEngine_taxonomy.OutcomeBypass || terr.Code != cacheEngine_taxonomy.ErrCodeUnregisteredKey {
		t.Errorf("expected SetL1 on unregistered to bypass, got %v", outcome)
	}

	// SetL1 - Stale version
	_, _, _ = syncMgr.SetL1(context.Background(), "key:1", "val:new", 10)
	err, terr, outcome = syncMgr.SetL1(context.Background(), "key:1", "val:stale", 5)
	if outcome != cacheEngine_taxonomy.OutcomeStale || terr.Code != cacheEngine_taxonomy.ErrCodeStaleVersion {
		t.Errorf("expected SetL1 with stale version to reject, got %v", outcome)
	}

	// InvalidateL1 - Unregistered key
	err, terr, outcome = syncMgr.InvalidateL1(context.Background(), "unregistered", 1)
	if outcome != cacheEngine_taxonomy.OutcomeBypass || terr.Code != cacheEngine_taxonomy.ErrCodeUnregisteredKey {
		t.Errorf("expected InvalidateL1 on unregistered to bypass, got %v", outcome)
	}

	// InvalidateL1 - Stale version
	err, terr, outcome = syncMgr.InvalidateL1(context.Background(), "key:1", 5)
	if outcome != cacheEngine_taxonomy.OutcomeStale || terr.Code != cacheEngine_taxonomy.ErrCodeStaleVersion {
		t.Errorf("expected InvalidateL1 with stale version to reject, got %v", outcome)
	}

	// GetOrLoad - Unregistered key (Bypass)
	val, err = syncMgr.GetOrLoad(context.Background(), "unregistered", func() (any, int64, error) {
		return "loaded", 1, nil
	})
	if val != "loaded" || err != nil {
		t.Errorf("GetOrLoad Bypass failed: val=%v, err=%v", val, err)
	}

	// GetOrLoad - loadFn returns error
	val, err = syncMgr.GetOrLoad(context.Background(), "key:2", func() (any, int64, error) {
		return nil, 0, fmt.Errorf("load error")
	})
	if val != nil || err == nil || err.Error() != "load error" {
		t.Errorf("expected load error, got val=%v, err=%v", val, err)
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

func TestLocalSyncManager_Janitor_Eviction(t *testing.T) {
	l1Cache := cacheEngine_local.NewCOWCache()
	syncMgr := cacheEngine_local.NewLocalSyncManager(l1Cache)
	// Đóng janitor mặc định để chạy janitor test với tần suất nhanh (10ms)
	syncMgr.Close()

	syncMgr.RegisterKeyConfig("temp:*", -1*time.Second)
	syncMgr.RegisterKeyConfig("persist:*", 1*time.Hour)

	ctx := context.Background()

	// Set 1 key chuẩn bị hết hạn nhanh, và 1 key sống lâu
	_, _, _ = syncMgr.SetL1(ctx, "temp:1", "value1", 100)
	_, _, _ = syncMgr.SetL1(ctx, "persist:1", "value2", 100)

	// Bắt đầu janitor quét mỗi 50ms
	syncMgr.StartJanitor(50 * time.Millisecond)
	defer syncMgr.Close()

	// Chờ 200ms để janitor chạy quét qua ít nhất 1 lần
	time.Sleep(200 * time.Millisecond)

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
