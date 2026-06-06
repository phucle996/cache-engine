# Cache Engine

Bộ thư viện Go Cache hai lớp chuyên biệt (**L1 Copy-On-Write RAM** và **L2 Distributed Redis**). Thiết kế tối ưu cho môi trường **Cloud-Native**, **khả dụng cao (HA)**, bảo vệ hệ thống khỏi race condition và cung cấp dữ liệu phân tích vận hành (**telemetry taxonomy**) chi tiết.

---

## 💡 Triết Lý Thiết Kế & Nguyên Tắc Tách Biệt Lớp

Thư viện tuân thủ nghiêm ngặt nguyên tắc **Decoupled Architecture** (Kiến trúc phân rã):

- **Tách biệt hoàn toàn L1 & L2**: L1 (`LocalSyncManager`) và L2 (`RedisSyncManager`) hoạt động độc lập. Không tự động gọi xuyên qua nhau, không trộn lẫn các tầng cấu hình hay truyền dẫn.
- **Callsite-Driven Coordination**: Ứng dụng gọi (callsite) chịu trách nhiệm tự phối hợp đọc/ghi giữa các tầng theo mô hình Cache-aside thủ công để phù hợp tối đa với logic nghiệp vụ.
- **Callsite-Driven Fanout (Opt-in)**: Thư viện tuyệt đối không tự động phát tán (Publish) sự kiện lên Redis Pub/Sub khi gọi Set/Invalidate. Quyết định phát tán tin nhắn hay không hoàn toàn do callsite chủ động điều phối thông qua các hook hoặc lời gọi trực tiếp.

---

## 📌 Yêu Cầu Bắt Buộc: Cấu Hình Cache Key (`keys.json`)

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

*Lưu ý*: Đối với key động, hệ thống sử dụng thuật toán so khớp wildcard kết hợp **LRU Cache** (giới hạn tối đa 10,000 bản ghi) để phân giải nhanh TTL trên hot-path ($O(1)$) nhằm tránh rò rỉ bộ nhớ (OOM) trong môi trường HA.

---

## 🛠️ Các Mô Hình Sử Dụng (Deployment & Usage Patterns)

### 1. Chỉ Sử Dụng L1 (Local RAM Cache Only)

Phù hợp cho các service chạy độc lập (single instance) hoặc dữ liệu tĩnh rất ít thay đổi, không yêu cầu đồng bộ tức thời giữa các replica. Không cần kết nối Redis.

- **Khởi tạo**:

  ```go
  localSyncMgr, err := cacheengine.InitLocalEngine("keys.json")
  ```

- **Đọc/Ghi**: Sử dụng trực tiếp

  ```go
  localSyncMgr.GetL1(ctx, key)
  localSyncMgr.SetL1(ctx, key, val, version)
  localSyncMgr.InvalidateL1(ctx, key, version)
  ```

---

### 2. Chỉ Sử Dụng L2 (Distributed Redis Cache Only)

Phù hợp khi RAM của service bị giới hạn, hoặc cần dùng chung bộ đệm tập trung giữa nhiều instance để tránh lãng phí dung lượng RAM cục bộ.

- **Khởi tạo**:

  ```go
  redisSyncMgr, err := cacheengine.InitRedisEngine(rdb, "keys.json")
  ```

- **Đọc/Ghi**: Sử dụng trực tiếp

  ```go
  redisSyncMgr.GetL2(ctx, key)
  redisSyncMgr.SetL2(ctx, key, valBytes)
  redisSyncMgr.InvalidateL2(ctx, key)
  ```

---

### 3. L1 + Fanout Bus (Đồng Bộ RAM Cục Bộ HA - Không dùng L2)

Phù hợp cho hệ thống cần tốc độ đọc tối đa từ RAM cục bộ trên môi trường HA (nhiều replicas), và đồng bộ hóa trực tiếp các bản sao L1 bằng cách phát tin nhắn qua Redis Pub/Sub mà không cần lưu trữ dữ liệu tại L2 Redis.

- **Khởi tạo**:

  ```go
  localSyncMgr, err := cacheengine.InitLocalEngine("keys.json")
  fanoutBus := cacheEngine_bus.NewBus(rdb, "invalidation-channel", localSyncMgr.KeyRegistry(), 0) // 0 → dùng DefaultMaxMessageBytes (1 MB)
  ```

- **Vòng lặp lắng nghe ở Background (Subscribe)**:

  ```go
  go func() {
      _ = fanoutBus.Subscribe(ctx, func(msg cacheEngine_bus.FanoutMessage) {
          if msg.Op == "upsert" {
              var data ConcreteType
              if err := json.Unmarshal(msg.Payload, &data); err == nil {
                  _ = localSyncMgr.SetL1(ctx, msg.Key, data, msg.Version)
              }
          } else if msg.Op == "delete" {
              _, _, _ = localSyncMgr.InvalidateL1(ctx, msg.Key, msg.Version)
          }
      })
  }()
  ```

- **Đọc dữ liệu (L1 -> DB)**:

  ```go
  val, _, _, outcome := localSyncMgr.GetL1(ctx, key)
  if outcome == cacheEngine_taxonomy.OutcomeL1Hit {
      return val.(ConcreteType)
  }
  // Miss -> Load từ DB, ghi lại vào L1
  dbVal, version := loadFromDB(key)
  _ = localSyncMgr.SetL1(ctx, key, dbVal, version)
  ```

- **Ghi dữ liệu (Write-Through / Write-Aside)**:

  ```go
  // 1. Lưu DB
  // 2. Lưu L1 local
  _ = localSyncMgr.SetL1(ctx, key, newVal, newVersion)
  // 3. Chủ động phát tán tin nhắn để các replica khác cập nhật
  rawBytes, _ := json.Marshal(newVal)
  _ = fanoutBus.Publish(ctx, key, "upsert", rawBytes, newVersion)
  ```

---

### 4. Chế Độ Lai Ghép Đầy Đủ (Hybrid Mode: L1 + L2 + Fanout Bus)

Mô hình tối ưu nhất cho hệ thống HA quy mô lớn: Đọc nhanh từ L1 RAM cục bộ, fallback về L2 Redis nếu L1 Miss, và chỉ đọc DB khi cả hai lớp đều Miss. Các cập nhật/xóa được đồng bộ toàn bộ replica thông qua Fanout Bus.

#### Ví dụ luồng Đọc (Cache-aside kết hợp L1/L2)

```go
func GetUserProfile(ctx context.Context, localSyncMgr *cacheEngine_local.LocalSyncManager, redisSyncMgr *cacheEngine_redis.RedisSyncManager, userID string) (*UserProfile, error) {
 key := fmt.Sprintf("user:profile:%s", userID)

 // 1. Đọc dữ liệu từ L1 Cache (RAM local)
 val, err, errx, outcome := localSyncMgr.GetL1(ctx, key)
 if outcome == cacheEngine_taxonomy.OutcomeL1Hit {
  profile := val.(UserProfile)
  return &profile, nil
 }

 // 2. Nếu L1 Miss -> Đọc dữ liệu thô từ L2 Cache (Redis)
 valBytes, err, errx, outcomeL2 := redisSyncMgr.GetL2(ctx, key)
 if outcomeL2 == cacheEngine_taxonomy.OutcomeL2Hit {
  var profile UserProfile
  if err := json.Unmarshal(valBytes, &profile); err == nil {
   // Ghi ngược lại L1 dạng struct cụ thể (tránh JSON overhead cho lần sau)
   _, _, _ = localSyncMgr.SetL1(ctx, key, profile, time.Now().UnixNano())
   return &profile, nil
  }
 }

 // 3. Cache Miss cả hai lớp -> Load dữ liệu từ DB (SoT)
 dbUser := UserProfile{ID: userID, Name: "Phuc Le", Email: "phucle@example.com"}
 dbVersion := time.Now().UnixNano()

 // 4. Ghi ngược lại cache L2 dạng bytes và L1 dạng struct
 rawBytes, _ := json.Marshal(dbUser)
 _, _, _ = redisSyncMgr.SetL2(ctx, key, rawBytes)
 _, _, _ = localSyncMgr.SetL1(ctx, key, dbUser, dbVersion)

 return &dbUser, nil
}
```

#### Ví dụ luồng Ghi & Quyết định Fanout (Hook tại Callsite)

```go
func UpdateUserProfile(ctx context.Context, localSyncMgr *cacheEngine_local.LocalSyncManager, redisSyncMgr *cacheEngine_redis.RedisSyncManager, fanoutBus *cacheEngine_bus.Bus, profile UserProfile) error {
 key := fmt.Sprintf("user:profile:%s", profile.ID)
 dbVersion := time.Now().UnixNano()

 // 1. Cập nhật dữ liệu vào DB
 // db.Save(&profile)

 // 2. Cập nhật cache L2 thô và L1 của chính instance hiện tại
 rawBytes, _ := json.Marshal(profile)
 _ = redisSyncMgr.SetL2(ctx, key, rawBytes)
 _, _, _ = localSyncMgr.SetL1(ctx, key, profile, dbVersion)

 // 3. Tùy chọn: Gọi hook phát tán (Fanout) sang các replica L1 khác trong cụm HA
 err := fanoutBus.Publish(ctx, key, "upsert", rawBytes, dbVersion)
 return err
}
```

---

## 🧪 Chạy Unit Test

Chạy bộ unit test để xác thực tính toàn vẹn của logic so khớp wildcard, LRU cache, kiểm soát stale version monotonic, và các outcome đo lường:

```bash
go test -v ./...
```
