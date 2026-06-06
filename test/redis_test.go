package cacheEngine_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cacheEngine_codec "github.com/phucle996/cache-engine/codec"
	cacheEngine_redis "github.com/phucle996/cache-engine/redis"
	cacheEngine_taxonomy "github.com/phucle996/cache-engine/taxonomy"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

// TestRedisCache_NilClient covers methods of RedisCache when client is nil.
func TestRedisCache_NilClient(t *testing.T) {
	rc := cacheEngine_redis.NewRedisCache(nil)

	val, found, err := rc.Get(context.Background(), "key")
	if val != nil || found || err != nil {
		t.Error("expected nil, false, nil for Get with nil client")
	}

	err = rc.Set(context.Background(), "key", []byte("val"), 10*time.Minute)
	if err != nil {
		t.Error("expected nil error for Set with nil client")
	}

	err = rc.Delete(context.Background(), "key")
	if err != nil {
		t.Error("expected nil error for Delete with nil client")
	}
}

// TestRedisSyncManager_NilL2 covers GetOrLoad with a nil L2 cache block.
func TestRedisSyncManager_NilL2(t *testing.T) {
	syncMgr := cacheEngine_redis.NewRedisSyncManager(nil)
	syncMgr.RegisterKeyConfig("user:*", 5*time.Minute)

	ctx := context.Background()
	val, err := syncMgr.GetOrLoad(ctx, "user:1", func() ([]byte, error) {
		return []byte("nil-l2-loaded"), nil
	})
	if err != nil || string(val) != "nil-l2-loaded" {
		t.Errorf("GetOrLoad with nil L2 failed: val=%s, err=%v", val, err)
	}
}

// TestRedisSyncManager_Coverage covers all branches in RedisSyncManager and RedisCache.
func TestRedisSyncManager_Coverage(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	rdb := goredis.NewClient(&goredis.Options{
		Addr: s.Addr(),
	})
	defer rdb.Close()

	l2 := cacheEngine_redis.NewRedisCache(rdb)
	syncMgr := cacheEngine_redis.NewRedisSyncManager(l2)

	// KeyRegistry method
	if syncMgr.KeyRegistry() == nil {
		t.Error("expected KeyRegistry to return registry")
	}

	// Register key config
	syncMgr.RegisterKeyConfig("user:*", 5*time.Minute)

	// GetL2 - Hit
	ctx := context.Background()
	_ = l2.Set(ctx, "user:1", []byte("val:1"), 5*time.Minute)
	val, err, terr, outcome := syncMgr.GetL2(ctx, "user:1")
	if outcome != cacheEngine_taxonomy.OutcomeL2Hit || string(val) != "val:1" || err != nil || terr != nil {
		t.Errorf("expected L2 hit, got outcome=%v, err=%v, terr=%v", outcome, err, terr)
	}

	// GetOrLoad - Cache Hit Fast Path
	valOrLoaded, errOrLoaded := syncMgr.GetOrLoad(ctx, "user:1", func() ([]byte, error) {
		return nil, fmt.Errorf("should not be called")
	})
	if errOrLoaded != nil || string(valOrLoaded) != "val:1" {
		t.Errorf("expected Cache Hit, got val=%s, err=%v", valOrLoaded, errOrLoaded)
	}

	// GetOrLoad - successful load and save to L2 cache
	valOrLoaded, errOrLoaded = syncMgr.GetOrLoad(ctx, "user:load-and-save", func() ([]byte, error) {
		return []byte("saved-bytes"), nil
	})
	if errOrLoaded != nil || string(valOrLoaded) != "saved-bytes" {
		t.Errorf("GetOrLoad failed: %v", errOrLoaded)
	}

	// GetL2 - Miss
	val, err, terr, outcome = syncMgr.GetL2(ctx, "user:2")
	if outcome != cacheEngine_taxonomy.OutcomeL2Miss || val != nil || err != nil || terr != nil {
		t.Errorf("expected L2 miss, got outcome=%v", outcome)
	}

	// GetL2 - Unregistered key (Bypass)
	val, err, terr, outcome = syncMgr.GetL2(ctx, "unregistered")
	if outcome != cacheEngine_taxonomy.OutcomeBypass || terr.Code != cacheEngine_taxonomy.ErrCodeUnregisteredKey {
		t.Errorf("expected Bypass, got outcome=%v", outcome)
	}

	// SetL2 - Unregistered key (Bypass)
	err, terr, outcome = syncMgr.SetL2(ctx, "unregistered", []byte("val"))
	if outcome != cacheEngine_taxonomy.OutcomeBypass || terr.Code != cacheEngine_taxonomy.ErrCodeUnregisteredKey {
		t.Errorf("expected Bypass, got outcome=%v", outcome)
	}

	// InvalidateL2 - Unregistered key (Bypass)
	err, terr, outcome = syncMgr.InvalidateL2(ctx, "unregistered")
	if outcome != cacheEngine_taxonomy.OutcomeBypass || terr.Code != cacheEngine_taxonomy.ErrCodeUnregisteredKey {
		t.Errorf("expected Bypass, got outcome=%v", outcome)
	}

	// SetL2Object
	dummy := DummyStruct{Name: "hello"}
	err, terr, outcome = syncMgr.SetL2Object(ctx, "user:1", dummy)
	if outcome != cacheEngine_taxonomy.OutcomeUpdate || err != nil || terr != nil {
		t.Errorf("SetL2Object failed: outcome=%v, err=%v", outcome, err)
	}

	// GetL2Object - Hit & Success path
	var successDest DummyStruct
	err, terr, outcome = syncMgr.GetL2Object(ctx, "user:1", &successDest)
	if outcome != cacheEngine_taxonomy.OutcomeL2Hit || successDest.Name != "hello" || err != nil || terr != nil {
		t.Errorf("GetL2Object success failed: outcome=%v, err=%v", outcome, err)
	}

	// GetOrLoadObject - Hit & Success path
	var successDest2 DummyStruct
	err = syncMgr.GetOrLoadObject(ctx, "user:1", &successDest2, func() (any, error) {
		return nil, fmt.Errorf("should not be called")
	})
	if err != nil || successDest2.Name != "hello" {
		t.Errorf("GetOrLoadObject L2 hit failed: %v", err)
	}

	// GetL2Object - Unmarshal error
	_ = l2.Set(ctx, "user:invalid", []byte("invalid-json"), 5*time.Minute)
	var dest DummyStruct
	err, terr, outcome = syncMgr.GetL2Object(ctx, "user:invalid", &dest)
	if outcome != cacheEngine_taxonomy.OutcomeFailed || terr.Code != cacheEngine_taxonomy.ErrCodeL2Failed || err == nil {
		t.Errorf("expected unmarshal failure, got outcome=%v, err=%v", outcome, err)
	}

	// SetL2Object - Marshal error
	err, terr, outcome = syncMgr.SetL2Object(ctx, "user:1", make(chan int))
	if outcome != cacheEngine_taxonomy.OutcomeFailed || terr.Code != cacheEngine_taxonomy.ErrCodeL2Failed || err == nil {
		t.Errorf("expected marshal failure, got outcome=%v, err=%v", outcome, err)
	}

	// GetOrLoad - Unregistered key (Bypass)
	val, err = syncMgr.GetOrLoad(ctx, "unregistered", func() ([]byte, error) {
		return []byte("loaded"), nil
	})
	if string(val) != "loaded" || err != nil {
		t.Errorf("GetOrLoad bypass failed: val=%s, err=%v", val, err)
	}

	// GetOrLoad - loadFn error
	val, err = syncMgr.GetOrLoad(ctx, "user:load-err", func() ([]byte, error) {
		return nil, fmt.Errorf("load error")
	})
	if val != nil || err == nil || err.Error() != "load error" {
		t.Errorf("expected load error, got val=%v, err=%v", val, err)
	}

	// GetOrLoadObject - Unregistered key (Bypass)
	var objDest DummyStruct
	err = syncMgr.GetOrLoadObject(ctx, "unregistered", &objDest, func() (any, error) {
		return DummyStruct{Name: "bypass"}, nil
	})
	if err != nil || objDest.Name != "bypass" {
		t.Errorf("GetOrLoadObject bypass failed: err=%v, dest=%v", err, objDest)
	}

	// GetOrLoadObject - loadFn error
	err = syncMgr.GetOrLoadObject(ctx, "user:load-err", &objDest, func() (any, error) {
		return nil, fmt.Errorf("load error")
	})
	if err == nil || err.Error() != "load error" {
		t.Errorf("expected load error, got err=%v", err)
	}

	// GetOrLoadObject - Marshal error during load callback save
	var chanDest chan int
	err = syncMgr.GetOrLoadObject(ctx, "user:chan", &chanDest, func() (any, error) {
		return make(chan int), nil
	})
	if err != nil {
		t.Errorf("expected GetOrLoadObject to succeed even if caching fails, got err=%v", err)
	}

	// copyValue error cases & pointer logic
	// dest must be a non-nil pointer
	err = syncMgr.GetOrLoadObject(ctx, "user:invalid-ptr-key", dummy, func() (any, error) {
		return dummy, nil
	})
	if err == nil || err.Error() != "dest must be a non-nil pointer" {
		t.Errorf("expected pointer error, got %v", err)
	}

	// cannot assign src to dest
	err = syncMgr.GetOrLoadObject(ctx, "user:invalid-type-key", &dest, func() (any, error) {
		return 12345, nil
	})
	if err == nil {
		t.Error("expected assignment error, got nil")
	}

	// non-nil pointer to cover vSrc.Elem() loop
	var successObj DummyStruct
	err = syncMgr.GetOrLoadObject(ctx, "user:ptr-test-key", &successObj, func() (any, error) {
		return &DummyStruct{Name: "pointer-val"}, nil
	})
	if err != nil || successObj.Name != "pointer-val" {
		t.Errorf("GetOrLoadObject pointer failed: err=%v, obj=%v", err, successObj)
	}

	// src is nil case
	var ptrDest *DummyStruct
	err = syncMgr.GetOrLoadObject(ctx, "user:nil-src", &ptrDest, func() (any, error) {
		return nil, nil
	})
	if err != nil || ptrDest != nil {
		t.Errorf("expected no error and nil ptrDest, got err=%v, dest=%v", err, ptrDest)
	}

	// src is nested nil pointer
	err = syncMgr.GetOrLoadObject(ctx, "user:nil-src2", &ptrDest, func() (any, error) {
		var p *DummyStruct = nil
		return p, nil
	})
	if err != nil || ptrDest != nil {
		t.Errorf("expected no error and nil ptrDest, got err=%v, dest=%v", err, ptrDest)
	}

	// L2 connection failures
	rdb.Close()

	// GetL2 error
	val, err, terr, outcome = syncMgr.GetL2(ctx, "user:1")
	if outcome != cacheEngine_taxonomy.OutcomeFailed || terr.Code != cacheEngine_taxonomy.ErrCodeL2Failed || err == nil {
		t.Errorf("expected L2 failure, got outcome=%v, err=%v", outcome, err)
	}

	// SetL2 error
	err, terr, outcome = syncMgr.SetL2(ctx, "user:1", []byte("val"))
	if outcome != cacheEngine_taxonomy.OutcomeFailed || terr.Code != cacheEngine_taxonomy.ErrCodeL2Failed || err == nil {
		t.Errorf("expected L2 failure, got outcome=%v, err=%v", outcome, err)
	}

	// InvalidateL2 error
	err, terr, outcome = syncMgr.InvalidateL2(ctx, "user:1")
	if outcome != cacheEngine_taxonomy.OutcomeFailed || terr.Code != cacheEngine_taxonomy.ErrCodeL2Failed || err == nil {
		t.Errorf("expected L2 failure, got outcome=%v, err=%v", outcome, err)
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
