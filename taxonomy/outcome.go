package cacheEngine_taxonomy

/*
================================================================================
HỢP ĐỒNG (CONTRACT), NGUỒN SỰ THẬT (SoT) & RANH GIỚI (BOUNDARIES) - OUTCOME
================================================================================
- Hợp đồng (Contract):
  * Định nghĩa tập hợp các kết quả đầu ra (Outcomes) của các hành vi đọc, ghi và xử lý cache.
  * Giúp caller nhận diện chính xác cache hit/miss hoặc các tác vụ background cập nhật/xóa/bỏ qua.
  * Cung cấp chuỗi chuẩn hóa duy nhất hỗ trợ xuất biểu đồ Grafana/Prometheus metric.

- Nguồn sự thật (Source of Truth - SoT):
  * Danh sách các hằng số Outcome khai báo trong package này là SoT duy nhất về kết quả vận hành cache.

- Ranh giới & Ràng buộc (Boundaries):
  * Outcome là kiểu chuỗi (string) đơn giản để đảm bảo tính gọn nhẹ khi truyền nhận hoặc ghi log.
  * Đảm bảo tính nhất quán: Mỗi thao tác chỉ trả về duy nhất một Outcome tương ứng.
================================================================================
*/

// Outcome đại diện cho kết quả của một hành động truy cập hoặc cập nhật cache.
type Outcome string

const (
	// OutcomeL1Hit tìm thấy dữ liệu hợp lệ trong RAM L1 cục bộ.
	OutcomeL1Hit Outcome = "L1_HIT"
	
	// OutcomeL1Miss không tìm thấy hoặc hết hạn dữ liệu trong RAM L1 cục bộ.
	OutcomeL1Miss Outcome = "L1_MISS"
	
	// OutcomeL2Hit tìm thấy dữ liệu thô trong Redis L2.
	OutcomeL2Hit Outcome = "L2_HIT"
	
	// OutcomeL2Miss không tìm thấy hoặc hết hạn dữ liệu trong Redis L2.
	OutcomeL2Miss Outcome = "L2_MISS"
	
	// OutcomeUpdate cập nhật thành công dữ liệu mới (upsert).
	OutcomeUpdate Outcome = "UPDATE"
	
	// OutcomeDelete xóa thành công dữ liệu (delete).
	OutcomeDelete Outcome = "DELETE"
	
	// OutcomeStale thao tác bị từ chối do version truyền vào cũ hơn phiên bản hiện tại (stale version).
	OutcomeStale Outcome = "STALE"
	
	// OutcomeBypass bỏ qua thao tác vì key chưa được đăng ký trong cấu hình.
	OutcomeBypass Outcome = "BYPASS"
	
	// OutcomeFailed thao tác thất bại do lỗi phần cứng/mạng hoặc giải mã dữ liệu hỏng.
	OutcomeFailed Outcome = "FAILED"
)
