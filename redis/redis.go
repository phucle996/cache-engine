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
	"fmt"
	"reflect"
	"time"

	cacheEngine_codec "github.com/phucle996/cache-engine/codec"
	cacheEngine_jitter "github.com/phucle996/cache-engine/jitter"
	cacheEngine_registry "github.com/phucle996/cache-engine/registry"
	cacheEngine_taxonomy "github.com/phucle996/cache-engine/taxonomy"

	goredis "github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

// RedisCache là bộ nhớ đệm L2 phân tán sử dụng Redis làm cơ sở dữ liệu lưu trữ.
type RedisCache struct {
	client  *goredis.Client
	sfGroup singleflight.Group
	codec   cacheEngine_codec.Codec
}

// NewRedisCache tạo mới thực thể RedisCache.
func NewRedisCache(client *goredis.Client) *RedisCache {
	return &RedisCache{
		client: client,
		codec:  cacheEngine_codec.NewJSONCodec(),
	}
}

// SetCodec thiết lập bộ mã hóa/giải mã cho RedisCache.
func (c *RedisCache) SetCodec(codec cacheEngine_codec.Codec) {
	if codec != nil {
		c.codec = codec
	}
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

// GetOrLoad hỗ trợ lấy dữ liệu raw bytes từ L2, tự động nạp khi miss.
func (c *RedisCache) GetOrLoad(ctx context.Context, key string, loadFn func() ([]byte, error)) ([]byte, error) {
	val, found, err := c.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if found {
		return val, nil
	}

	res, err, _ := c.sfGroup.Do(key, func() (any, error) {
		dbVal, dbErr := loadFn()
		if dbErr != nil {
			return nil, dbErr
		}

		_ = c.Set(ctx, key, dbVal, 5*time.Minute)
		return dbVal, nil
	})
	if err != nil {
		return nil, err
	}
	return res.([]byte), nil
}

// GetOrLoadObject hỗ trợ lấy đối tượng kiểu struct phức tạp từ L2, tự động nạp khi miss.
func (c *RedisCache) GetOrLoadObject(ctx context.Context, key string, dest any, loadFn func() (any, error)) error {
	valBytes, found, err := c.Get(ctx, key)
	if err != nil {
		return err
	}
	if found {
		return c.codec.Unmarshal(valBytes, dest)
	}

	_, err, _ = c.sfGroup.Do(key, func() (any, error) {
		dbObj, dbErr := loadFn()
		if dbErr != nil {
			return nil, dbErr
		}

		if err := copyValue(dest, dbObj); err != nil {
			return nil, err
		}

		serialized, err := c.codec.Marshal(dbObj)
		if err != nil {
			return nil, err
		}
		_ = c.Set(ctx, key, serialized, 5*time.Minute)
		return nil, nil
	})
	return err
}

// RedisSyncManager điều phối việc đọc ghi dữ liệu cache L2 (Redis phân tán).
type RedisSyncManager struct {
	l2          *RedisCache
	keyRegistry *cacheEngine_registry.KeyRegistry
	sfGroup     singleflight.Group
	codec       cacheEngine_codec.Codec
}

// NewRedisSyncManager tạo mới và khởi tạo thực thể cho RedisSyncManager.
func NewRedisSyncManager(l2 *RedisCache) *RedisSyncManager {
	return &RedisSyncManager{
		l2:          l2,
		keyRegistry: cacheEngine_registry.NewKeyRegistry(10000),
		codec:       cacheEngine_codec.NewJSONCodec(), // Mặc định dùng JSON
	}
}

// SetCodec thiết lập bộ mã hóa/giải mã cho manager.
func (m *RedisSyncManager) SetCodec(codec cacheEngine_codec.Codec) {
	if codec != nil {
		m.codec = codec
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

// GetL2Object tự động truy xuất và giải tuần tự hóa từ L2 vào dest.
func (m *RedisSyncManager) GetL2Object(ctx context.Context, key string, dest any) (error, *cacheEngine_taxonomy.Error, cacheEngine_taxonomy.Outcome) {
	valBytes, err, terr, outcome := m.GetL2(ctx, key)
	if outcome != cacheEngine_taxonomy.OutcomeL2Hit {
		return err, terr, outcome
	}
	if err := m.codec.Unmarshal(valBytes, dest); err != nil {
		return err, cacheEngine_taxonomy.NewError(cacheEngine_taxonomy.ErrCodeL2Failed, "failed to unmarshal object", err), cacheEngine_taxonomy.OutcomeFailed
	}
	return nil, nil, cacheEngine_taxonomy.OutcomeL2Hit
}

// SetL2Object tự động tuần tự hóa và cập nhật dữ liệu vào L2.
func (m *RedisSyncManager) SetL2Object(ctx context.Context, key string, value any) (error, *cacheEngine_taxonomy.Error, cacheEngine_taxonomy.Outcome) {
	valBytes, err := m.codec.Marshal(value)
	if err != nil {
		return err, cacheEngine_taxonomy.NewError(cacheEngine_taxonomy.ErrCodeL2Failed, "failed to marshal object", err), cacheEngine_taxonomy.OutcomeFailed
	}
	return m.SetL2(ctx, key, valBytes)
}

// GetOrLoadObject tự động truy xuất từ L2, nếu miss sẽ dùng Singleflight bao bọc loadFn và tự động ghi cache L2.
func (m *RedisSyncManager) GetOrLoadObject(
	ctx context.Context,
	key string,
	dest any,
	loadFn func() (value any, err error),
) error {
	// 1. Thử lấy từ L2 trước
	err, _, outcome := m.GetL2Object(ctx, key, dest)
	if outcome == cacheEngine_taxonomy.OutcomeL2Hit {
		return nil
	}

	// 2. Gom nhóm các request bằng Singleflight
	res, err, _ := m.sfGroup.Do(key, func() (any, error) {
		ttl, exists := m.keyRegistry.Resolve(key)
		if !exists {
			// Key chưa đăng ký -> Chạy loadFn bình thường nhưng không ghi cache (Bypass)
			return loadFn()
		}

		dbVal, dbErr := loadFn()
		if dbErr != nil {
			return nil, dbErr
		}

		// Tự động ghi lại cache L2
		if m.l2 != nil && dbVal != nil {
			valBytes, encErr := m.codec.Marshal(dbVal)
			if encErr == nil {
				_ = m.l2.Set(ctx, key, valBytes, ttl)
			}
		}
		return dbVal, nil
	})

	if err != nil {
		return err
	}

	// Sao chép kết quả vào dest
	return copyValue(dest, res)
}

func copyValue(dest any, src any) error {
	if src == nil {
		return nil
	}
	vDest := reflect.ValueOf(dest)
	if vDest.Kind() != reflect.Ptr || vDest.IsNil() {
		return fmt.Errorf("dest must be a non-nil pointer")
	}

	vSrc := reflect.ValueOf(src)
	for vSrc.Kind() == reflect.Ptr || vSrc.Kind() == reflect.Interface {
		if vSrc.IsNil() {
			return nil
		}
		vSrc = vSrc.Elem()
	}

	destElem := vDest.Elem()
	if !vSrc.Type().AssignableTo(destElem.Type()) {
		return fmt.Errorf("cannot assign %s to %s", vSrc.Type().String(), destElem.Type().String())
	}

	destElem.Set(vSrc)
	return nil
}


