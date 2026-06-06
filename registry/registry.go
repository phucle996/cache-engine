package cacheEngine_registry

/*
================================================================================
HỢP ĐỒNG (CONTRACT), NGUỒN SỰ THẬT (SoT) & RANH GIỚI (BOUNDARIES) - REGISTRY
================================================================================
- Hợp đồng (Contract):
  * Đăng ký cấu hình key tĩnh hoặc pattern động dạng wildcard (*).
  * Phân giải TTL của bất kỳ key nào dựa trên so khớp chính xác hoặc wildcard.
  * Tối ưu hóa so khớp tiền tố (suffix wildcard dạng prefix:*) sang map tĩnh O(1).
  * Tích hợp LRU Cache để tối ưu hóa hiệu năng phân giải các key động phức tạp khác.

- Nguồn sự thật (Source of Truth - SoT):
  * Danh sách static keys và pattern list được nạp từ file JSON cấu hình là nguồn sự thật duy nhất.

- Ranh giới & Ràng buộc (Boundaries):
  * LRU Cache được giới hạn dung lượng để ngăn chặn nguy cơ cạn kiệt bộ nhớ (OOM).
  * Đảm bảo Thread-safe bằng Lock mutex.
================================================================================
*/

import (
	"container/list"
	"path"
	"sync"
	"time"
)

// KeyRegistry quản lý cấu hình các static key và dynamic wildcard key.
type KeyRegistry struct {
	mu            sync.RWMutex
	staticKeys    map[string]time.Duration
	prefixKeys    map[string]time.Duration // Ánh xạ từ tiền tố (prefix) sang TTL phục vụ O(1)
	prefixLengths []int                    // Lưu trữ danh sách độ dài các tiền tố duy nhất
	patterns      []patternEntry           // Fallback cho các wildcard phức tạp khác
	lru           *lruCache
}

type patternEntry struct {
	pattern string
	ttl     time.Duration
}

type lruCache struct {
	capacity  int
	items     map[string]*list.Element
	evictList *list.List
}

type lruItem struct {
	key string
	ttl time.Duration
}

// NewKeyRegistry tạo mới một thực thể KeyRegistry với giới hạn dung lượng LRU.
func NewKeyRegistry(lruCapacity int) *KeyRegistry {
	if lruCapacity <= 0 {
		lruCapacity = 10000 // Giá trị mặc định an toàn cho HA
	}
	return &KeyRegistry{
		staticKeys: make(map[string]time.Duration),
		prefixKeys: make(map[string]time.Duration),
		lru: &lruCache{
			capacity:  lruCapacity,
			items:     make(map[string]*list.Element),
			evictList: list.New(),
		},
	}
}

// Register đăng ký một mẫu key kèm TTL. Nếu có wildcard thì xem là dynamic pattern.
func (r *KeyRegistry) Register(pattern string, ttl time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 1. Phân tích xem có phải suffix wildcard đơn giản hay không (ví dụ: prefix:*)
	if prefix, ok := parseSuffixWildcard(pattern); ok {
		r.prefixKeys[prefix] = ttl
		// Thêm độ dài prefix vào danh sách nếu chưa có
		exists := false
		for _, l := range r.prefixLengths {
			if l == len(prefix) {
				exists = true
				break
			}
		}
		if !exists {
			r.prefixLengths = append(r.prefixLengths, len(prefix))
		}
	} else if containsWildcard(pattern) {
		// Wildcard phức tạp khác
		r.patterns = append(r.patterns, patternEntry{
			pattern: pattern,
			ttl:     ttl,
		})
	} else {
		// Key tĩnh hoàn toàn
		r.staticKeys[pattern] = ttl
	}
}

// Resolve phân giải TTL cho một key động. Trả về TTL và trạng thái tồn tại.
func (r *KeyRegistry) Resolve(key string) (time.Duration, bool) {
	r.mu.RLock()
	// 1. Kiểm tra static key trước (Fast-path O(1))
	if ttl, ok := r.staticKeys[key]; ok {
		r.mu.RUnlock()
		return ttl, true
	}

	// 2. Kiểm tra các prefix key đã đăng ký (Fast-path O(P) ~ O(1))
	for _, l := range r.prefixLengths {
		if len(key) >= l {
			prefix := key[:l]
			if ttl, ok := r.prefixKeys[prefix]; ok {
				r.mu.RUnlock()
				return ttl, true
			}
		}
	}
	r.mu.RUnlock()

	// 3. Fallback: Dùng cho các wildcard phức tạp chứa dấu * ở giữa (Slow-path kết hợp LRU)
	r.mu.Lock()
	defer r.mu.Unlock()

	// Kiểm tra trong LRU Cache
	if elem, ok := r.lru.items[key]; ok {
		r.lru.evictList.MoveToFront(elem)
		return elem.Value.(*lruItem).ttl, true
	}

	// Quét qua danh sách các dynamic patterns phức tạp
	for _, entry := range r.patterns {
		matched, err := path.Match(entry.pattern, key)
		if err == nil && matched {
			// Thêm kết quả vào LRU Cache
			if r.lru.evictList.Len() >= r.lru.capacity {
				r.evictOldest()
			}
			item := &lruItem{key: key, ttl: entry.ttl}
			elem := r.lru.evictList.PushFront(item)
			r.lru.items[key] = elem
			return entry.ttl, true
		}
	}

	return 0, false
}

// Clear xóa toàn bộ cache LRU.
func (r *KeyRegistry) ClearLRU() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lru.items = make(map[string]*list.Element)
	r.lru.evictList.Init()
}

func (r *KeyRegistry) evictOldest() {
	elem := r.lru.evictList.Back()
	if elem != nil {
		r.lru.evictList.Remove(elem)
		item := elem.Value.(*lruItem)
		delete(r.lru.items, item.key)
	}
}

// HasPrefix trả về true và TTL nếu prefix đã được đăng ký trong prefix map.
func (r *KeyRegistry) HasPrefix(prefix string) (time.Duration, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ttl, exists := r.prefixKeys[prefix]
	return ttl, exists
}

// PrefixLengths trả về bản sao danh sách độ dài các prefix.
func (r *KeyRegistry) PrefixLengths() []int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	lengths := make([]int, len(r.prefixLengths))
	copy(lengths, r.prefixLengths)
	return lengths
}

// PatternsCount trả về số lượng dynamic patterns phức tạp được đăng ký.
func (r *KeyRegistry) PatternsCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.patterns)
}

// LRULen trả về số lượng phần tử hiện tại trong LRU Cache.
func (r *KeyRegistry) LRULen() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lru.evictList.Len()
}

// LRUHas trả về true nếu key đang tồn tại trong LRU Cache.
func (r *KeyRegistry) LRUHas(key string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, exists := r.lru.items[key]
	return exists
}

func parseSuffixWildcard(pattern string) (string, bool) {
	// Phải chứa đúng 1 dấu '*' ở cuối cùng và không chứa các ký tự wildcard khác '?', '['
	for i := 0; i < len(pattern); i++ {
		if pattern[i] == '?' || pattern[i] == '[' {
			return "", false
		}
		if pattern[i] == '*' {
			if i != len(pattern)-1 {
				return "", false // Dấu '*' không nằm ở cuối cùng
			}
		}
	}
	if len(pattern) > 0 && pattern[len(pattern)-1] == '*' {
		return pattern[:len(pattern)-1], true
	}
	return "", false
}

func containsWildcard(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == '*' || s[i] == '?' || s[i] == '[' {
			return true
		}
	}
	return false
}
