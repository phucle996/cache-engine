package cacheengine

/*
================================================================================
HỢP ĐỒNG (CONTRACT), NGUỒN SỰ THẬT (SoT) & RANH GIỚI (BOUNDARIES) - TYPES
================================================================================
- Hợp đồng (Contract):
  * Định nghĩa các interface lõi cho Cache Hệ Thống: L1 (Local Memory), L2 (Distributed
    Redis), và Fanout (Pub/Sub Event Bus).
  * L1Cache đảm nhận lưu trữ RAM local, bắt buộc hỗ trợ monotonic version check.
  * L2Cache đảm nhận lưu trữ phân tán dạng raw bytes để tách biệt logic serialization.
  * FanoutBus đảm nhận truyền tải thông tin invalidation bất đồng bộ giữa các replicas.

- Nguồn sự thật (Source of Truth - SoT):
  * SoT cuối cùng nằm ở Database. Cache Engine lưu giữ bản copy tạm thời của SoT.
  * Mọi cập nhật trạng thái trong Cache bắt buộc phải đi kèm với Version (Unix Nanosecond
    timestamp lấy từ bản ghi Database tại thời điểm cập nhật).

- Ranh giới & Ràng buộc (Boundaries):
  * Tách biệt hoàn toàn phần dữ liệu RAM local (L1) dạng interface{} và dữ liệu truyền
    tải mạng (L2 & Bus) dạng raw byte array ([]byte).
  * Thread-safety: Các implementations của interface phải tự đảm bảo thread-safe.
================================================================================
*/

import (
	"context"
	"time"

	cacheEngine_bus "github.com/phucle996/cache-engine/bus"
)

// L1Cache định nghĩa interface cho cache bộ nhớ trong cục bộ (RAM cục bộ).
type L1Cache interface {
	// Get lấy giá trị từ L1 Cache kèm version hiện tại của key đó.
	Get(key string) (value any, version int64, found bool)

	// Set lưu giá trị vào L1 Cache kèm TTL và version kiểm tra tính tuần tự monotonic.
	Set(key string, value any, ttl time.Duration, version int64)

	// Delete xóa key khỏi L1 Cache kèm version kiểm tra tính tuần tự monotonic.
	Delete(key string, version int64)

	// Clear xóa sạch toàn bộ cache trong RAM cục bộ.
	Clear()

	// GetOrLoad hỗ trợ lấy dữ liệu từ L1, tự động nạp từ loadFn khi miss.
	GetOrLoad(ctx context.Context, key string, loadFn func() (value any, version int64, err error)) (any, error)
}

// L2Cache định nghĩa interface cho cache phân tán (ví dụ: Redis).
// Nó lưu trữ dưới dạng raw bytes để bắt buộc phía gọi (caller) thực hiện ép kiểu/kiểm soát serialization rõ ràng.
type L2Cache interface {
	// Get lấy dữ liệu dạng byte thô từ L2.
	Get(ctx context.Context, key string) (value []byte, found bool, err error)

	// Set lưu dữ liệu dạng byte thô vào L2 kèm TTL.
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error

	// Delete xóa dữ liệu khỏi L2.
	Delete(ctx context.Context, key string) error

	// GetOrLoad hỗ trợ lấy dữ liệu raw bytes từ L2, tự động nạp khi miss.
	GetOrLoad(ctx context.Context, key string, loadFn func() ([]byte, error)) ([]byte, error)

	// GetOrLoadObject hỗ trợ lấy đối tượng kiểu struct phức tạp từ L2, tự động nạp khi miss.
	GetOrLoadObject(ctx context.Context, key string, dest any, loadFn func() (any, error)) error
}

// FanoutBus định nghĩa interface cho cơ chế phát/đăng ký nhận tin nhắn hủy cache (invalidation).
type FanoutBus interface {
	// Publish gửi tin nhắn hủy cache trên kênh chung.
	Publish(ctx context.Context, key string, op string, payload []byte, version int64) error

	// Subscribe đăng ký lắng nghe và xử lý tin nhắn hủy cache từ kênh chung.
	Subscribe(ctx context.Context, handler func(msg cacheEngine_bus.FanoutMessage)) error
}

// Cache là container hợp nhất chứa cả ba thành phần L1, L2 và Fanout Bus.
type Cache struct {
	L1     L1Cache
	L2     L2Cache
	Fanout FanoutBus
}
