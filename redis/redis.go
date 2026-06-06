package cacheEngine_redis

/*
================================================================================
HỢP ĐỒNG (CONTRACT), NGUỒN SỰ THẬT (SoT) & RANH GIỚI (BOUNDARIES) - HYBRID CACHE
================================================================================
- Hợp đồng (Contract):
  * Đóng vai trò lớp lưu trữ phân tán L2 sử dụng cơ sở dữ liệu Redis.
  * Chỉ lưu trữ mảng byte thô ([]byte) để đảm bảo tính độc lập về cơ chế tuần tự hóa.
  * RedisSyncManager quản lý và đọc ghi cache L2 (Redis phân tán).
  * Không tích hợp trực tiếp Pub/Sub; việc phát và nhận tin nhắn do callsite điều phối.

- Nguồn sự thật (Source of Truth - SoT):
  * SoT nằm ở Database. Mọi cập nhật trạng thái yêu cầu version monotonic.

- Ranh giới & Ràng buộc (Boundaries):
  * Ngoại lệ và Lỗi: Trả về lỗi của thư viện Redis trực tiếp để caller xử lý.
  * Telemetry: Trả về đầy đủ taxonomy Error và Outcome cho service layer.
================================================================================
*/

import (
	"context"
	"time"

	cacheEngine_jitter "cache-engine/jitter"
	cacheEngine_registry "cache-engine/registry"
	cacheEngine_taxonomy "cache-engine/taxonomy"

	goredis "github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

// RedisCache là bộ nhớ đệm L2 phân tán sử dụng Redis làm cơ sở dữ liệu lưu trữ.
type RedisCache struct {
	client *goredis.Client
}

// NewRedisCache tạo mới thực thể RedisCache.
func NewRedisCache(client *goredis.Client) *RedisCache {
	return &RedisCache{client: client}
}

// Get truy xuất mảng byte thô từ Redis.
func (c *RedisCache) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if c.client == nil {
		return nil, false, nil
	}
	data, err := c.client.Get(ctx, key).Bytes()
	if err == goredis.Nil {
		return nil, false, nil // Cache miss
	}
	if err != nil {
		return nil, false, err
	}
	return data, true, nil
}

// Set ghi mảng byte thô vào Redis kèm theo TTL đã áp dụng jitter.
func (c *RedisCache) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	if c.client == nil {
		return nil
	}
	jitteredTTL := cacheEngine_jitter.ApplyTTLJitter(ttl)
	return c.client.Set(ctx, key, val, jitteredTTL).Err()
}

// Delete xóa khóa khỏi Redis.
func (c *RedisCache) Delete(ctx context.Context, key string) error {
	if c.client == nil {
		return nil
	}
	return c.client.Del(ctx, key).Err()
}

// RedisSyncManager điều phối việc đọc ghi dữ liệu cache L2 (Redis phân tán).
type RedisSyncManager struct {
	l2          *RedisCache
	keyRegistry *cacheEngine_registry.KeyRegistry
	sfGroup     singleflight.Group
}

// NewRedisSyncManager tạo mới và khởi tạo thực thể cho RedisSyncManager.
func NewRedisSyncManager(l2 *RedisCache) *RedisSyncManager {
	return &RedisSyncManager{
		l2:          l2,
		keyRegistry: cacheEngine_registry.NewKeyRegistry(10000),
	}
}

// KeyRegistry trả về thực thể KeyRegistry được quản lý bởi manager.
func (m *RedisSyncManager) KeyRegistry() *cacheEngine_registry.KeyRegistry {
	return m.keyRegistry
}

// RegisterKeyConfig đăng ký một cache key và TTL tương ứng vào cấu hình.
func (m *RedisSyncManager) RegisterKeyConfig(key string, ttl time.Duration) {
	m.keyRegistry.Register(key, ttl)
}

// GetTTL lấy thông tin cấu hình TTL của một key đã được đăng ký.
func (m *RedisSyncManager) GetTTL(key string) (time.Duration, bool) {
	return m.keyRegistry.Resolve(key)
}

// GetL2 truy xuất mảng byte thô từ L2 (Redis).
func (m *RedisSyncManager) GetL2(ctx context.Context, key string) ([]byte, error, *cacheEngine_taxonomy.Error, cacheEngine_taxonomy.Outcome) {
	_, exists := m.keyRegistry.Resolve(key)
	if !exists {
		return nil, nil, cacheEngine_taxonomy.NewError(cacheEngine_taxonomy.ErrCodeUnregisteredKey, "key is not registered in configuration", nil), cacheEngine_taxonomy.OutcomeBypass
	}

	if m.l2 != nil {
		data, found, err := m.l2.Get(ctx, key)
		if err != nil {
			return nil, err, cacheEngine_taxonomy.NewError(cacheEngine_taxonomy.ErrCodeL2Failed, "failed to read from L2 cache", err), cacheEngine_taxonomy.OutcomeFailed
		}

		if found {
			return data, nil, nil, cacheEngine_taxonomy.OutcomeL2Hit
		}
	}

	return nil, nil, nil, cacheEngine_taxonomy.OutcomeL2Miss
}

// SetL2 cập nhật dữ liệu mảng byte thô trực tiếp vào L2.
func (m *RedisSyncManager) SetL2(ctx context.Context, key string, value []byte) (error, *cacheEngine_taxonomy.Error, cacheEngine_taxonomy.Outcome) {
	ttl, exists := m.keyRegistry.Resolve(key)
	if !exists {
		return nil, cacheEngine_taxonomy.NewError(cacheEngine_taxonomy.ErrCodeUnregisteredKey, "key is not registered in configuration", nil), cacheEngine_taxonomy.OutcomeBypass
	}

	if m.l2 != nil {
		if setErr := m.l2.Set(ctx, key, value, ttl); setErr != nil {
			return setErr, cacheEngine_taxonomy.NewError(cacheEngine_taxonomy.ErrCodeL2Failed, "failed to write value to L2 cache", setErr), cacheEngine_taxonomy.OutcomeFailed
		}
	}

	return nil, nil, cacheEngine_taxonomy.OutcomeUpdate
}

// InvalidateL2 xóa khóa khỏi L2.
func (m *RedisSyncManager) InvalidateL2(ctx context.Context, key string) (error, *cacheEngine_taxonomy.Error, cacheEngine_taxonomy.Outcome) {
	_, exists := m.keyRegistry.Resolve(key)
	if !exists {
		return nil, cacheEngine_taxonomy.NewError(cacheEngine_taxonomy.ErrCodeUnregisteredKey, "key is not registered in configuration", nil), cacheEngine_taxonomy.OutcomeBypass
	}

	if m.l2 != nil {
		if delErr := m.l2.Delete(ctx, key); delErr != nil {
			return delErr, cacheEngine_taxonomy.NewError(cacheEngine_taxonomy.ErrCodeL2Failed, "failed to delete from L2 cache", delErr), cacheEngine_taxonomy.OutcomeFailed
		}
	}

	return nil, nil, cacheEngine_taxonomy.OutcomeDelete
}

// GetOrLoad hỗ trợ lấy dữ liệu thô từ L2, nếu miss sẽ dùng Singleflight để gom nhóm các request đồng thời bằng loadFn.
func (m *RedisSyncManager) GetOrLoad(
	ctx context.Context,
	key string,
	loadFn func() (value []byte, err error),
) ([]byte, error) {
	// 1. Kiểm tra L2 Cache trước
	valBytes, _, _, outcome := m.GetL2(ctx, key)
	if outcome == cacheEngine_taxonomy.OutcomeL2Hit {
		return valBytes, nil
	}

	// 2. Nếu L2 Miss -> Gom nhóm các request đồng thời bằng Singleflight
	res, err, _ := m.sfGroup.Do(key, func() (any, error) {
		ttl, exists := m.keyRegistry.Resolve(key)
		if !exists {
			// Nếu key chưa đăng ký, vẫn chạy loadFn bình thường nhưng không ghi cache (Bypass)
			return loadFn()
		}

		dbValBytes, dbErr := loadFn()
		if dbErr != nil {
			return nil, dbErr
		}

		// Tự động ghi lại cache L2
		if m.l2 != nil {
			_ = m.l2.Set(ctx, key, dbValBytes, ttl)
		}
		return dbValBytes, nil
	})

	if err != nil {
		return nil, err
	}
	return res.([]byte), nil
}

