package cacheEngine_jitter

/*
================================================================================
HỢP ĐỒNG (CONTRACT), NGUỒN SỰ THẬT (SoT) & RANH GIỚI (BOUNDARIES) - JITTER
================================================================================
- Hợp đồng (Contract):
  * Cung cấp tiện ích tính toán độ lệch thời gian ngẫu nhiên (Jitter) cho thời gian sống
    (TTL) của cache.
  * Ngăn chặn hiện tượng Cache Stampede (nhiều bản ghi hết hạn đồng thời dẫn tới quá tải DB).

- Nguồn sự thật (Source of Truth - SoT):
  * Nhận TTL đầu vào làm tham số nguồn, tính toán phân phối ngẫu nhiên giả (pseudo-random)
    dựa trên thuật toán Clamped Skew (1/10 TTL, giới hạn trong khoảng [5s, 30s]).

- Ranh giới & Ràng buộc (Boundaries):
  * Nếu TTL <= 0, trả về trực tiếp giá trị TTL gốc không áp dụng jitter (bypass).
  * Giới hạn sàn an toàn (Safe Floor): Jittered TTL trả về luôn được bảo vệ tối thiểu là
    1 giây, đảm bảo cache không bị hết hạn ngay lập tức do lệch âm quá mức.
  * Stateless: Hàm không phụ thuộc vào trạng thái toàn cục bên ngoài hay lưu trữ cục bộ.
================================================================================
*/

import (
	"math/rand"
	"time"
)

// ApplyTTLJitter tính toán độ lệch ngẫu nhiên TTL để chống thắt cổ chai DB khi hết hạn cache đồng thời.
// Khoảng lệch (Skew) bằng 1/10 thời gian sống (TTL) chỉ định, giới hạn sàn 5s và trần 30s.
func ApplyTTLJitter(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return ttl
	}

	skew := ttl / 10
	if skew < 5*time.Second {
		skew = 5 * time.Second
	}
	if skew > 30*time.Second {
		skew = 30 * time.Second
	}

	ns := skew.Nanoseconds()
	// Sinh độ lệch ngẫu nhiên trong khoảng [-skew, skew]
	offset := rand.Int63n(2*ns) - ns
	jittered := ttl + time.Duration(offset)

	if jittered <= 1*time.Second {
		return 1 * time.Second // Giới hạn sàn an toàn tối thiểu 1s
	}
	return jittered
}
