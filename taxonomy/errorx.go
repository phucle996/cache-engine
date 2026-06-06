package cacheEngine_taxonomy

/*
================================================================================
HỢP ĐỒNG (CONTRACT), NGUỒN SỰ THẬT (SoT) & RANH GIỚI (BOUNDARIES) - ERRORX
================================================================================
- Hợp đồng (Contract):
  * Định nghĩa cấu trúc lỗi chuẩn của thư viện Cache Engine: Error.
  * Mỗi lỗi trả về bao gồm mã lỗi phân loại (ErrorCode) và lỗi nguyên thủy hệ thống (primitive error).
  * Giúp các dự án gọi thư viện dễ dàng bắt lỗi và phân loại cho logging, metrics, và tracing.

- Nguồn sự thật (Source of Truth - SoT):
  * SoT của định danh lỗi là danh sách hằng số ErrorCode chuẩn hóa duy nhất trong package này.

- Ranh giới & Ràng buộc (Boundaries):
  * Lớp lỗi bao bọc (Error) tuân thủ interface error chuẩn của Go bằng cách triển khai hàm Error().
  * Luôn đóng gói lỗi nguyên thủy ẩn sâu bên dưới (ví dụ: lỗi Redis, lỗi JSON Marshalling)
    để không bị mất ngữ cảnh gốc của lỗi hệ thống.
================================================================================
*/

import (
	"fmt"
)

// ErrorCode đại diện cho phân loại mã lỗi của Cache Engine.
type ErrorCode string

const (
	// ErrCodeUnregisteredKey xảy ra khi cache key chưa được đăng ký trong cấu hình.
	ErrCodeUnregisteredKey ErrorCode = "UNREGISTERED_KEY"
	
	// ErrCodeUnmarshalFailed xảy ra khi không thể giải mã payload dữ liệu byte thô.
	ErrCodeUnmarshalFailed ErrorCode = "UNMARSHAL_FAILED"
	
	// ErrCodeMarshalFailed xảy ra khi không thể mã hóa thực thể thành dữ liệu byte thô.
	ErrCodeMarshalFailed ErrorCode = "MARSHAL_FAILED"
	
	// ErrCodeStaleVersion xảy ra khi version cập nhật/xóa cũ hơn hoặc bằng version hiện tại trong L1.
	ErrCodeStaleVersion ErrorCode = "STALE_VERSION"
	
	// ErrCodeL2Failed xảy ra khi thao tác đọc/ghi vào bộ nhớ đệm L2 (Redis) bị lỗi mạng/kết nối.
	ErrCodeL2Failed ErrorCode = "L2_FAILED"
	
	// ErrCodeFanoutFailed xảy ra khi bus phát tán Pub/Sub gặp sự cố truyền tải.
	ErrCodeFanoutFailed ErrorCode = "FANOUT_FAILED"
)

// Error cấu trúc lỗi đóng gói đầy đủ thông tin phân loại để hỗ trợ đo đạc telemetry.
type Error struct {
	Code    ErrorCode // Mã phân loại lỗi để gom nhóm metrics
	Message string    // Thông điệp mô tả lỗi
	Err     error     // Lỗi nguyên thủy hệ thống (primitive error) nếu có
}

// Error thực thi interface error chuẩn của Go.
func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("[%s] %s: %v", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

// NewError tạo mới một thực thể lỗi đóng gói.
func NewError(code ErrorCode, message string, err error) *Error {
	return &Error{
		Code:    code,
		Message: message,
		Err:     err,
	}
}
