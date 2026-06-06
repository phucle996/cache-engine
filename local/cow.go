package cacheEngine_local

/*
================================================================================
HỢP ĐỒNG (CONTRACT), NGUỒN SỰ THẬT (SoT) & RANH GIỚI (BOUNDARIES) - LOCAL CACHE
================================================================================
- Hợp đồng (Contract):
  * Cung cấp một bộ nhớ đệm RAM nội bộ thread-safe, hiệu suất cực cao thông qua cơ chế
    Copy-On-Write (COW).
  * Hỗ trợ lưu trữ dữ liệu dạng any (interface{}) kèm theo metadata version kiểm tra.
  * LocalSyncManager quản lý cấu hình TTL của cache key, cung cấp các phương thức Get, Set,
    và Invalidate trả về taxonomy Error/Outcome cho logging/tracing tại callsite.
  * Không tích hợp trực tiếp Pub/Sub; việc phát và nhận tin nhắn do callsite điều phối.

- Nguồn sự thật (Source of Truth - SoT):
  * Bản chụp (snapshot) nội bộ của cache phân tán hoặc DB.
  * Việc ghi/xóa giá trị bắt buộc kiểm tra version monotonic.

- Ranh giới & Ràng buộc (Boundaries):
  * Độc lập luồng (Thread-Safety): Luồng đọc là Lock-free sử dụng atomic.Pointer load bản chụp.
  * Taxonomy: Các hàm của LocalSyncManager trả về kết quả chuẩn hóa để service layer dễ dàng xử lý.
================================================================================
*/

import (
	cacheEngine_jitter "github.com/phucle996/cache-engine/jitter"
	cacheEngine_registry "github.com/phucle996/cache-engine/registry"
	cacheEngine_taxonomy "github.com/phucle996/cache-engine/taxonomy"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

type cacheEntry struct {
	value     any
	expiresAt time.Time
	version   int64
}

type snapshot struct {
	entries map[string]cacheEntry
}

// COWCache triển khai bộ nhớ đệm L1 an toàn đa luồng cục bộ bằng cơ chế hoán đổi snapshot Copy-On-Write.
type COWCache struct {
	ptr atomic.Pointer[snapshot]
	mu  sync.Mutex // Đồng bộ hóa các luồng ghi
}

// NewCOWCache tạo và khởi tạo thực thể COWCache mới.
func NewCOWCache() *COWCache {
	c := &COWCache{}
	c.ptr.Store(&snapshot{entries: make(map[string]cacheEntry)})
	return c
}

// Get truy xuất một giá trị từ L1. Đây là luồng đọc lock-free.
func (c *COWCache) Get(key string) (any, int64, bool) {
	snap := c.ptr.Load()
	entry, exists := snap.entries[key]
	if !exists || time.Now().After(entry.expiresAt) {
		return nil, 0, false
	}
	return entry.value, entry.version, true
}

// Set ghi dữ liệu vào L1 Cache kèm TTL đã được làm lệch ngẫu nhiên.
func (c *COWCache) Set(key string, value any, ttl time.Duration, version int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	current := c.ptr.Load()
	if entry, exists := current.entries[key]; exists && version <= entry.version {
		return // Cập nhật lỗi thời (stale update) -> Loại bỏ
	}

	next := &snapshot{
		entries: make(map[string]cacheEntry, len(current.entries)+1),
	}
	for k, v := range current.entries {
		next.entries[k] = v
	}

	jitteredTTL := cacheEngine_jitter.ApplyTTLJitter(ttl)

	next.entries[key] = cacheEntry{
		value:     value,
		expiresAt: time.Now().Add(jitteredTTL),
		version:   version,
	}
	c.ptr.Store(next)
}

// Delete xóa một bản ghi khỏi L1 Cache.
func (c *COWCache) Delete(key string, version int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	current := c.ptr.Load()
	if entry, exists := current.entries[key]; exists && version <= entry.version {
		return // Lệnh xóa cũ -> Loại bỏ
	}

	next := &snapshot{
		entries: make(map[string]cacheEntry, len(current.entries)),
	}
	for k, v := range current.entries {
		if k != key {
			next.entries[k] = v
		}
	}
	c.ptr.Store(next)
}

// Clear dọn sạch toàn bộ cache RAM cục bộ.
func (c *COWCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ptr.Store(&snapshot{entries: make(map[string]cacheEntry)})
}

// EvictExpired tìm và loại bỏ tất cả các key đã hết hạn.
// Trả về số lượng key đã bị dọn dẹp thực tế.
func (c *COWCache) EvictExpired() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	current := c.ptr.Load()
	now := time.Now()

	// 1. Kiểm tra nhanh (Fast-path): Đếm xem có key nào hết hạn thực sự không
	expiredCount := 0
	for _, v := range current.entries {
		if now.After(v.expiresAt) {
			expiredCount++
		}
	}

	// Nếu không có key nào hết hạn -> Thoát sớm, bảo vệ RAM khỏi việc allocations vô ích
	if expiredCount == 0 {
		return 0
	}

	// 2. Chỉ thực hiện Copy-On-Write khi thực sự cần giải phóng RAM
	next := &snapshot{
		entries: make(map[string]cacheEntry, len(current.entries)-expiredCount),
	}

	for k, v := range current.entries {
		if !now.After(v.expiresAt) {
			next.entries[k] = v
		}
	}

	c.ptr.Store(next)
	return expiredCount
}

// L1Cache đại diện cho interface của L1 Cache bộ nhớ trong cục bộ.
type L1Cache interface {
	Get(key string) (value any, version int64, found bool)
	Set(key string, value any, ttl time.Duration, version int64)
	Delete(key string, version int64)
	Clear()
}

// LocalSyncManager điều phối việc đọc ghi cache L1 cục bộ (RAM) kèm kiểm soát cấu hình key và phiên bản.
type LocalSyncManager struct {
	l1          L1Cache
	keyRegistry *cacheEngine_registry.KeyRegistry
	sfGroup     singleflight.Group
	janitorStop chan struct{}
}

// NewLocalSyncManager tạo mới và khởi tạo thực thể cho LocalSyncManager.
func NewLocalSyncManager(l1 L1Cache) *LocalSyncManager {
	m := &LocalSyncManager{
		l1:          l1,
		keyRegistry: cacheEngine_registry.NewKeyRegistry(10000),
	}
	m.StartJanitor(5 * time.Minute)
	return m
}

// StartJanitor khởi chạy (hoặc chạy lại) luồng quét dọn dẹp định kỳ.
func (m *LocalSyncManager) StartJanitor(interval time.Duration) {
	m.Close() // Dừng bộ dọn dẹp cũ nếu có trước khi chạy mới

	m.janitorStop = make(chan struct{})
	ticker := time.NewTicker(interval)

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if evictable, ok := m.l1.(interface{ EvictExpired() int }); ok {
					evictable.EvictExpired()
				}
			case <-m.janitorStop:
				return
			}
		}
	}()
}

// Close dừng luồng dọn dẹp chạy ngầm, tránh rò rỉ goroutine khi shutdown hệ thống.
func (m *LocalSyncManager) Close() {
	if m.janitorStop != nil {
		close(m.janitorStop)
		m.janitorStop = nil
	}
}

// KeyRegistry trả về thực thể KeyRegistry được quản lý bởi manager.
func (m *LocalSyncManager) KeyRegistry() *cacheEngine_registry.KeyRegistry {
	return m.keyRegistry
}

// RegisterKeyConfig đăng ký một cache key và TTL tương ứng vào cấu hình.
func (m *LocalSyncManager) RegisterKeyConfig(key string, ttl time.Duration) {
	m.keyRegistry.Register(key, ttl)
}

// GetTTL lấy thông tin cấu hình TTL của một key đã được đăng ký.
func (m *LocalSyncManager) GetTTL(key string) (time.Duration, bool) {
	return m.keyRegistry.Resolve(key)
}

// GetL1 truy xuất dữ liệu từ L1.
func (m *LocalSyncManager) GetL1(ctx context.Context, key string) (any, error, *cacheEngine_taxonomy.Error, cacheEngine_taxonomy.Outcome) {
	_, exists := m.keyRegistry.Resolve(key)
	if !exists {
		return nil, nil, cacheEngine_taxonomy.NewError(cacheEngine_taxonomy.ErrCodeUnregisteredKey, "key is not registered in configuration", nil), cacheEngine_taxonomy.OutcomeBypass
	}

	if val, _, ok := m.l1.Get(key); ok {
		return val, nil, nil, cacheEngine_taxonomy.OutcomeL1Hit
	}

	return nil, nil, nil, cacheEngine_taxonomy.OutcomeL1Miss
}

// SetL1 cập nhật dữ liệu trực tiếp vào L1.
func (m *LocalSyncManager) SetL1(ctx context.Context, key string, value any, version int64) (error, *cacheEngine_taxonomy.Error, cacheEngine_taxonomy.Outcome) {
	ttl, exists := m.keyRegistry.Resolve(key)
	if !exists {
		return nil, cacheEngine_taxonomy.NewError(cacheEngine_taxonomy.ErrCodeUnregisteredKey, "key is not registered in configuration", nil), cacheEngine_taxonomy.OutcomeBypass
	}

	if _, localVer, ok := m.l1.Get(key); ok {
		if version <= localVer {
			return nil, cacheEngine_taxonomy.NewError(cacheEngine_taxonomy.ErrCodeStaleVersion, "update rejected due to stale version", nil), cacheEngine_taxonomy.OutcomeStale
		}
	}

	m.l1.Set(key, value, ttl, version)
	return nil, nil, cacheEngine_taxonomy.OutcomeUpdate
}

// InvalidateL1 xóa khóa khỏi L1.
func (m *LocalSyncManager) InvalidateL1(ctx context.Context, key string, version int64) (error, *cacheEngine_taxonomy.Error, cacheEngine_taxonomy.Outcome) {
	_, exists := m.keyRegistry.Resolve(key)
	if !exists {
		return nil, cacheEngine_taxonomy.NewError(cacheEngine_taxonomy.ErrCodeUnregisteredKey, "key is not registered in configuration", nil), cacheEngine_taxonomy.OutcomeBypass
	}

	if _, localVer, ok := m.l1.Get(key); ok {
		if version <= localVer {
			return nil, cacheEngine_taxonomy.NewError(cacheEngine_taxonomy.ErrCodeStaleVersion, "delete request rejected due to stale version", nil), cacheEngine_taxonomy.OutcomeStale
		}
	}

	m.l1.Delete(key, version)
	return nil, nil, cacheEngine_taxonomy.OutcomeDelete
}

// GetOrLoad hỗ trợ lấy dữ liệu từ L1, nếu miss sẽ dùng Singleflight để gom nhóm các request đồng thời bằng loadFn.
func (m *LocalSyncManager) GetOrLoad(
	ctx context.Context,
	key string,
	loadFn func() (value any, version int64, err error),
) (any, error) {
	// 1. Kiểm tra L1 Cache trước (Fast-path)
	val, _, ok := m.l1.Get(key)
	if ok {
		return val, nil
	}

	// 2. Nếu L1 Miss -> Gom nhóm các request đồng thời bằng Singleflight
	res, err, _ := m.sfGroup.Do(key, func() (any, error) {
		// Nhận thông tin cấu hình TTL để kiểm tra trước khi ghi vào Cache
		ttl, exists := m.keyRegistry.Resolve(key)
		if !exists {
			// Nếu key chưa đăng ký, vẫn chạy loadFn bình thường nhưng không ghi cache (Bypass)
			dbVal, _, dbErr := loadFn()
			return dbVal, dbErr
		}

		dbVal, version, dbErr := loadFn()
		if dbErr != nil {
			return nil, dbErr
		}

		// Tự động ghi lại cache L1
		m.l1.Set(key, dbVal, ttl, version)
		return dbVal, nil
	})

	return res, err
}

