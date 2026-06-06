package cacheEngine_bus

/*
================================================================================
HỢP ĐỒNG (CONTRACT), NGUỒN SỰ THẬT (SoT) & RANH GIỚI (BOUNDARIES) - BUS
================================================================================
- Hợp đồng (Contract):
  * Cung cấp phương tiện phát thanh (Broadcast) và đăng ký lắng nghe (Subscribe) các tin
    nhắn invalidation (hủy cache) qua hệ thống Redis Pub/Sub.
  * Đóng gói payload thay đổi dưới dạng envelope FanoutMessage để truyền đi qua kênh chung.

- Nguồn sự thật (Source of Truth - SoT):
  * Bus chỉ đóng vai trò kênh truyền tin (Transport pipe), không lưu giữ bất kỳ trạng thái dữ
    liệu nào. Nó giúp đồng bộ trạng thái SoT giữa các replica cục bộ của hệ thống.

- Ranh giới & Ràng buộc (Boundaries):
  * Lỗi kết nối mạng: Subscribe lắng nghe qua một vòng lặp vô hạn. Nếu kết nối Redis bị gián đoạn,
    kênh channel của Redis Pub/Sub sẽ đóng lại và hàm Subscribe sẽ trả về lỗi tương ứng.
  * Serialization: Phân tách rõ ràng bằng việc tự động hóa quá trình json.Marshal và
    json.Unmarshal đối với FanoutMessage ngay tại biên lớp Bus này.
  * Security — Op Allowlist: Chỉ chấp nhận các giá trị Op được khai báo trong allowedOps.
    Mọi tin nhắn có Op không hợp lệ sẽ bị loại bỏ im lặng (silent drop) trước khi gọi handler,
    ngăn chặn việc khai thác logic ghi L1 từ dữ liệu độc hại trên kênh Redis.
  * Security — Payload Size Limit: Mọi tin nhắn (Publish) hoặc message thô (Subscribe) vượt quá
    maxMessageBytes sẽ bị từ chối. Giá trị này được callsite truyền vào lúc khởi tạo NewBus,
    cho phép mỗi kênh có giới hạn phù hợp với đặc tính payload của nó.
================================================================================
*/

import (
	cacheEngine_registry "github.com/phucle996/cache-engine/registry"
	"context"
	"encoding/json"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// allowedOps là tập hợp các giá trị Op hợp lệ được chấp nhận.
// Mọi Op nằm ngoài danh sách này sẽ bị từ chối tại Publish và bị silent drop tại Subscribe.
var allowedOps = map[string]struct{}{
	"upsert": {},
	"delete": {},
}

// FanoutMessage là phong bì dữ liệu (envelope) được gửi qua kênh Pub/Sub để hủy bỏ cache.
type FanoutMessage struct {
	Op      string          `json:"op"`
	Key     string          `json:"key"`
	Version int64           `json:"version"`
	TTL     time.Duration   `json:"ttl"`
	Payload json.RawMessage `json:"payload"`
}

// Bus là lớp bao quanh cơ chế Redis Pub/Sub dùng để phát tán các sự kiện invalidation.
type Bus struct {
	client          *goredis.Client
	channel         string
	reg             *cacheEngine_registry.KeyRegistry
	maxMessageBytes int // Giới hạn kích thước payload tối đa (bytes) do callsite quyết định
}

// NewBus tạo mới một thực thể Bus.
// maxMessageBytes: Giới hạn kích thước payload tối đa (bytes). Nếu <= 0 sẽ dùng DefaultMaxMessageBytes.
func NewBus(client *goredis.Client, channel string, reg *cacheEngine_registry.KeyRegistry, maxMessageBytes int) *Bus {
	if maxMessageBytes <= 0 {
		maxMessageBytes = 1 * 1024 * 1024 // 1 MB
	}
	return &Bus{
		client:          client,
		channel:         channel,
		reg:             reg,
		maxMessageBytes: maxMessageBytes,
	}
}

// Publish phát tán một tin nhắn hủy cache trên kênh Redis Pub/Sub.
func (b *Bus) Publish(ctx context.Context, key string, op string, payload []byte, version int64) error {
	// Guard 1: Validate Op tại thời điểm Publish để phát hiện lỗi lập trình sớm
	if _, ok := allowedOps[op]; !ok {
		return fmt.Errorf("invalid op %q: must be one of [upsert, delete]", op)
	}

	// Guard 2: Validate kích thước payload trước khi gửi đi
	if len(payload) > b.maxMessageBytes {
		return fmt.Errorf("payload size %d bytes exceeds limit of %d bytes", len(payload), b.maxMessageBytes)
	}

	ttl, exists := b.reg.Resolve(key)
	if !exists {
		return fmt.Errorf("key %s is not registered in configuration", key)
	}

	msg := FanoutMessage{
		Op:      op,
		Key:     key,
		Version: version,
		TTL:     ttl,
		Payload: payload,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return b.client.Publish(ctx, b.channel, data).Err()
}

// Subscribe lắng nghe liên tục trên kênh chung và kích hoạt hàm xử lý handler khi có tin nhắn mới.
// Trong trường hợp mất kết nối mạng hoặc Redis bị khởi động lại, hàm sẽ tự động thử lại (reconnect)
// với cơ chế exponential backoff (từ 1 giây đến tối đa 30 giây) cho đến khi Context bị hủy.
func (b *Bus) Subscribe(ctx context.Context, handler func(msg FanoutMessage)) error {
	backoff := 1 * time.Second
	for {
		_ = b.subscribeOnce(ctx, handler)
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Nếu kết nối Redis bị gián đoạn, đợi một khoảng thời gian trước khi kết nối lại
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
}

func (b *Bus) subscribeOnce(ctx context.Context, handler func(msg FanoutMessage)) error {
	pubsub := b.client.Subscribe(ctx, b.channel)
	defer pubsub.Close()

	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return fmt.Errorf("pubsub channel closed or connection lost")
			}

			// Guard 1: Giới hạn kích thước message thô trước khi unmarshal
			// Ngăn chặn tấn công OOM qua việc ghi payload khổng lồ vào kênh Redis
			if len(msg.Payload) > b.maxMessageBytes {
				continue
			}

			var event FanoutMessage
			if err := json.Unmarshal([]byte(msg.Payload), &event); err != nil {
				continue
			}

			// Guard 2: Kiểm tra Op nằm trong allowlist trước khi gọi handler
			// Ngăn chặn việc khai thác logic ghi L1 từ dữ liệu độc hại trên kênh
			if _, ok := allowedOps[event.Op]; !ok {
				continue
			}

			handler(event)
		}
	}
}
