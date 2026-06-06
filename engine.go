package cacheengine

/*
================================================================================
HỢP ĐỒNG (CONTRACT), NGUỒN SỰ THẬT (SoT) & RANH GIỚI (BOUNDARIES) - ENGINE (INITIALIZATION)
================================================================================
- Hợp đồng (Contract):
  * Cung cấp các hàm khởi tạo tập trung InitLocalEngine và InitRedisEngine để phân tích cấu
    hình TTL của các key từ file JSON và liên kết các thành phần L1 hoặc L2 tương ứng.
  * Việc sử dụng hay đồng bộ qua Fanout Bus sẽ do callsite tự quyết định và quản lý.

- Nguồn sự thật (Source of Truth - SoT):
  * Cấu hình tĩnh về các cache key và TTL (Time-To-Live) mặc định được tải trực tiếp từ
    file JSON cấu hình.

- Ranh giới & Ràng buộc (Boundaries):
  * Trả về thực thể quản lý tương ứng cho từng chế độ vận hành (LocalSyncManager hoặc RedisSyncManager).
================================================================================
*/

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	cacheEngine_local "cache-engine/local"
	cacheEngine_redis "cache-engine/redis"

	goredis "github.com/redis/go-redis/v9"
)

// EngineConfigJSON đại diện cho cấu trúc của một phần tử cấu hình cache key lưu trong file JSON.
type EngineConfigJSON struct {
	Key string `json:"key"`
	TTL string `json:"ttl"`
}

// InitLocalEngine khởi tạo Cache engine ở chế độ chỉ dùng L1 RAM (không dùng L2 Redis và không cần kết nối Redis).
func InitLocalEngine(keyFilePath string) (*cacheEngine_local.LocalSyncManager, error) {
	// 1. Đọc nội dung file cấu hình cache key
	data, err := os.ReadFile(keyFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read cache key configuration file: %w", err)
	}

	var items []EngineConfigJSON
	if err := json.Unmarshal(data, &items); err != nil {
		return nil, fmt.Errorf("failed to parse cache key configuration JSON: %w", err)
	}

	// 2. Khởi tạo L1
	l1Cache := cacheEngine_local.NewCOWCache()

	// 3. Khởi tạo LocalSyncManager
	syncMgr := cacheEngine_local.NewLocalSyncManager(l1Cache)

	// 4. Đăng ký các key cấu hình
	for _, item := range items {
		ttl, err := time.ParseDuration(item.TTL)
		if err != nil {
			return nil, fmt.Errorf("invalid TTL configuration '%s' for key '%s': %w", item.TTL, item.Key, err)
		}
		syncMgr.RegisterKeyConfig(item.Key, ttl)
	}

	return syncMgr, nil
}

// InitRedisEngine khởi tạo Cache engine ở chế độ dùng L2 Redis.
func InitRedisEngine(rdb *goredis.Client, keyFilePath string) (*cacheEngine_redis.RedisSyncManager, error) {
	// 1. Đọc nội dung file cấu hình cache key
	data, err := os.ReadFile(keyFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read cache key configuration file: %w", err)
	}

	var items []EngineConfigJSON
	if err := json.Unmarshal(data, &items); err != nil {
		return nil, fmt.Errorf("failed to parse cache key configuration JSON: %w", err)
	}

	// 2. Khởi tạo L2
	l2Cache := cacheEngine_redis.NewRedisCache(rdb)

	// 3. Khởi tạo RedisSyncManager
	redisSyncMgr := cacheEngine_redis.NewRedisSyncManager(l2Cache)

	// 4. Đăng ký các key cấu hình cho RedisSyncManager
	for _, item := range items {
		ttl, err := time.ParseDuration(item.TTL)
		if err != nil {
			return nil, fmt.Errorf("invalid TTL configuration '%s' for key '%s': %w", item.TTL, item.Key, err)
		}
		redisSyncMgr.RegisterKeyConfig(item.Key, ttl)
	}

	return redisSyncMgr, nil
}
