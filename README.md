# Cache Engine

Bộ thư viện Go Cache hai lớp chuyên biệt (**L1 Copy-On-Write RAM** và **L2 Distributed Redis**). Thiết kế tối ưu cho môi trường **Cloud-Native**, **khả dụng cao (HA)**, bảo vệ hệ thống khỏi race condition, cạn kiệt tài nguyên (OOM), sập cơ sở dữ liệu (Cache Stampede) và cung cấp dữ liệu phân tích vận hành (**telemetry taxonomy**) chi tiết.

---

## 💡 Triết Lý Thiết Kế & Nguyên Tắc Tách Biệt Lớp

Thư viện tuân thủ nghiêm ngặt nguyên tắc **Decoupled Architecture** (Kiến trúc phân rã):

- **Tách biệt hoàn toàn L1 & L2**: L1 (`LocalSyncManager`) và L2 (`RedisSyncManager`) hoạt động độc lập. Không tự động gọi xuyên qua nhau, không trộn lẫn các tầng cấu hình hay truyền dẫn.
- **Callsite-Driven Coordination**: Ứng dụng gọi (callsite) chịu trách nhiệm tự phối hợp đọc/ghi giữa các tầng theo mô hình Cache-aside thủ công để phù hợp tối đa với logic nghiệp vụ.
- **Callsite-Driven Fanout (Opt-in)**: Thư viện tuyệt đối không tự động phát tán (Publish) sự kiện lên Redis Pub/Sub khi gọi Set/Invalidate. Quyết định phát tán tin nhắn hay không hoàn toàn do callsite chủ động điều phối thông qua các hook hoặc lời gọi trực tiếp.

---

## 📦 Hệ thống Namespace Tránh Trùng Lặp (Go Modules)

Để đảm bảo an toàn tuyệt đối khi tích hợp vào các dự án lớn, tất cả các package con của thư viện đã được đặt namespace riêng biệt nhằm tránh xung đột với các package gốc (ví dụ: `redis`, `local`, `bus`):

- `cacheEngine_local`: Quản lý L1 RAM Cache (Copy-On-Write).
- `cacheEngine_redis`: Quản lý L2 Distributed Cache (Redis).
- `cacheEngine_bus`: Quản lý Invalidation Bus (Redis Pub/Sub).
- `cacheEngine_registry`: Quản lý cấu hình tĩnh $O(1)$ prefix-slicing map.
- `cacheEngine_taxonomy`: Quản lý mã lỗi và mã kết quả đo lường (telemetry).
- `cacheEngine_jitter`: Quản lý tính toán phân phối TTL ngẫu nhiên.

---

## 📌 Cấu Hình Cache Key (`keys.json`)

Để sử dụng bất kỳ Manager nào, **bắt buộc phải khai báo cấu hình key và TTL tương ứng dưới dạng file JSON**. Thư viện sẽ từ chối thao tác (trả về lỗi `BYPASS` kèm mã lỗi `UNREGISTERED_KEY`) đối với các key chưa được đăng ký.

### Hỗ Trợ Key Pattern (So khớp Wildcard)

Thư viện hỗ trợ cả **key tĩnh** và **key động chứa ký tự wildcard `*`** (dùng cho ID người dùng, ID thiết bị...).
Ví dụ file `keys.json`:

```json
[
  {"key": "system:settings", "ttl": "30s"},
  {"key": "user:profile:*", "ttl": "10m"},
  {"key": "device:*:status", "ttl": "5m"}
]
```

*Lưu ý*: Đối với key động, hệ thống sử dụng thuật toán **Prefix-slicing $O(1)$ fast-path** cho các suffix wildcard (dạng `prefix:*`). Các pattern phức tạp sẽ tự động chuyển sang LRU Cache (giới hạn tối đa 10,000 bản ghi) để phân giải nhanh TTL trên hot-path mà không tốn dung lượng RAM.

---

## 🔥 Tính Năng Nâng Cao (Enterprise-Grade Features)

### 1. Singleflight (Chống Nát Database & Quá Tải Mạng)

Phương thức `GetOrLoad` trên cả L1 và L2 giúp bao bọc lời gọi DB/Redis bên trong một `singleflight.Group`.

- Khi xảy ra Cache Miss dưới tải cao, **chỉ có duy nhất 1 goroutine thực sự truy vấn Database/Redis**, các request đồng thời khác sẽ xếp hàng chờ và dùng chung kết quả trả về, chặn đứng thảm họa Cache Breakdown.

### 2. Active L1 Memory Janitor (Bộ Dọn Dẹp RAM Chủ Động)

- **Vấn đề**: Các key hết hạn trong L1 RAM nếu không được gọi `Get` lại sẽ nằm lì trong map gây phình RAM (Memory Bloat).

- **Giải pháp**: Một luồng quét định kỳ chạy ngầm thực hiện Copy-On-Write để xóa hẳn các key hết hạn, giúp GC của Go có thể thu hồi 100% RAM vật lý.
- **Tối ưu Zero-Allocation**: Janitor sẽ đếm số key hết hạn trước. Nếu bằng `0`, nó sẽ thoát sớm và không thực hiện Copy map, tiết kiệm tối đa CPU/RAM.

### 3. Self-Healing Invalidation Bus (Tự Động Kết Nối Lại)

- Kênh truyền `Subscribe` của `cacheEngine_bus` được trang bị cơ chế **Exponential Backoff Reconnect** tự phục hồi. Nếu kết nối Redis bị gián đoạn, Bus sẽ tự động kết nối lại (thử lại từ 1s tăng dần đến tối đa 30s) mà không gây crash ứng dụng.

---

## 🛠️ Các Mô Hình Sử Dụng (Deployment & Usage Patterns)

### 1. Chỉ Sử Dụng L1 (Local RAM Cache Only)

```go
// Khởi tạo L1 (Mặc định chạy Janitor dọn dẹp RAM mỗi 5 phút)
localSyncMgr, err := cacheengine.InitLocalEngine("keys.json")

// Bạn có thể tùy chỉnh thời gian chạy Janitor hoặc tắt khi shutdown:
localSyncMgr.StartJanitor(10 * time.Minute)
defer localSyncMgr.Close()
```

#### Sử dụng Singleflight bảo vệ DB

```go
val, err := localSyncMgr.GetOrLoad(ctx, "user:profile:123", func() (any, int64, error) {
    // Chỉ chạy đúng 1 lần cho nhiều request đồng thời khi cache bị Miss
    user, version, err := db.LoadUser("123")
    return user, version, err
})
```

---

### 2. Chỉ Sử Dụng L2 (Distributed Redis Cache Only)

```go
redisSyncMgr, err := cacheengine.InitRedisEngine(rdb, "keys.json")
```

#### Sử dụng Singleflight bảo vệ mạng Redis

```go
valBytes, err := redisSyncMgr.GetOrLoad(ctx, "user:profile:123", func() ([]byte, error) {
    userBytes, err := db.LoadRawBytes("123")
    return userBytes, err
})
```

---

### 3. L1 + Fanout Bus (Đồng Bộ RAM Cục Bộ HA - Không dùng L2)

```go
localSyncMgr, err := cacheengine.InitLocalEngine("keys.json")
defer localSyncMgr.Close()

// Khởi tạo Invalidation Bus (Giới hạn payload 1MB tránh OOM)
fanoutBus := cacheEngine_bus.NewBus(rdb, "invalidation-channel", localSyncMgr.KeyRegistry(), 0)
```

#### Vòng lặp lắng nghe ở Background (Tự phục hồi kết nối)

```go
go func() {
    // Tự động kết nối lại nếu rớt mạng Redis
    _ = fanoutBus.Subscribe(ctx, func(msg cacheEngine_bus.FanoutMessage) {
        if msg.Op == "upsert" {
            var data UserProfile
            if err := json.Unmarshal(msg.Payload, &data); err == nil {
                _ = localSyncMgr.SetL1(ctx, msg.Key, data, msg.Version)
            }
        } else if msg.Op == "delete" {
            _, _, _ = localSyncMgr.InvalidateL1(ctx, msg.Key, msg.Version)
        }
    })
}()
```

---

### 4. Chế Độ Lai Ghép Đầy Đủ (Hybrid Mode: L1 + L2 + Fanout Bus)

#### Luồng Đọc phối hợp Singleflight (L1 -> L2 -> DB)

```go
func GetUserProfileHybrid(ctx context.Context, localSyncMgr *cacheEngine_local.LocalSyncManager, redisSyncMgr *cacheEngine_redis.RedisSyncManager, userID string) (*UserProfile, error) {
    key := fmt.Sprintf("user:profile:%s", userID)

    // 1. Đọc nhanh từ L1, nếu Miss sẽ dùng Singleflight gom nhóm request
    val, err := localSyncMgr.GetOrLoad(ctx, key, func() (any, int64, error) {
        // --- ĐOẠN CODE NÀY CHỈ CHẠY 1 LẦN CHO CÁC REQUEST ĐỒNG THỜI BỊ L1 MISS ---

        // 2. Thử lấy từ L2 Cache (Redis)
        valBytes, err, _, outcomeL2 := redisSyncMgr.GetL2(ctx, key)
        if outcomeL2 == cacheEngine_taxonomy.OutcomeL2Hit {
            var profile UserProfile
            if err := json.Unmarshal(valBytes, &profile); err == nil {
                return profile, time.Now().UnixNano(), nil
            }
        }

        // 3. L2 Miss -> Load DB thực tế
        profile, version, dbErr := db.LoadUserProfile(userID)
        if dbErr != nil {
            return nil, 0, dbErr
        }

        // 4. Ghi ngược lại L2 Redis
        rawBytes, _ := json.Marshal(profile)
        _ = redisSyncMgr.SetL2(ctx, key, rawBytes)

        return profile, version, nil
    })

    if err != nil {
        return nil, err
    }

    profile := val.(UserProfile)
    return &profile, nil
}
```

---

## 🧪 Chạy Unit Test

Chạy bộ unit test để xác thực tính toàn vẹn của logic so khớp wildcard, LRU cache, kiểm soát stale version monotonic, Singleflight và Active Janitor:

```bash
go test -v ./...
```
