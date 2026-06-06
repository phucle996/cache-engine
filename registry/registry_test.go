package cacheEngine_registry

import (
	"testing"
	"time"
)

func TestKeyRegistry_PrefixFastPath(t *testing.T) {
	r := NewKeyRegistry(10)

	// Register a suffix wildcard
	r.Register("user:profile:*", 10*time.Minute)

	// Verify it went to prefixKeys map
	if ttl, ok := r.prefixKeys["user:profile:"]; !ok || ttl != 10*time.Minute {
		t.Errorf("expected user:profile: in prefixKeys map, got %v, ok=%v", ttl, ok)
	}

	// Verify length was recorded
	foundLen := false
	for _, l := range r.prefixLengths {
		if l == len("user:profile:") {
			foundLen = true
			break
		}
	}
	if !foundLen {
		t.Errorf("expected prefix length %d to be recorded", len("user:profile:"))
	}

	// Resolve the key
	ttl, exists := r.Resolve("user:profile:12345")
	if !exists || ttl != 10*time.Minute {
		t.Errorf("expected 10m TTL, got %v, exists=%v", ttl, exists)
	}

	// Verify it did NOT enter LRU cache (since it is a prefix fast-path key)
	if r.lru.evictList.Len() != 0 {
		t.Errorf("expected LRU cache length to be 0, got %d", r.lru.evictList.Len())
	}
}

func TestKeyRegistry_ComplexWildcardLRU(t *testing.T) {
	// LRU Capacity = 2
	r := NewKeyRegistry(2)

	// Register a complex wildcard (wildcard in the middle)
	r.Register("device:*:status", 5*time.Minute)

	// Verify it went to patterns slice, not prefixKeys
	if len(r.patterns) != 1 || r.patterns[0].pattern != "device:*:status" {
		t.Errorf("expected patterns slice to contain device:*:status")
	}
	if len(r.prefixKeys) != 0 {
		t.Errorf("expected prefixKeys map to be empty, got %d", len(r.prefixKeys))
	}

	// Resolve 1st key -> should enter LRU
	ttl, exists := r.Resolve("device:1:status")
	if !exists || ttl != 5*time.Minute {
		t.Errorf("expected 5m TTL, got %v", ttl)
	}
	if r.lru.evictList.Len() != 1 {
		t.Errorf("expected LRU len 1, got %d", r.lru.evictList.Len())
	}

	// Resolve 2nd key -> should enter LRU
	ttl, exists = r.Resolve("device:2:status")
	if !exists || ttl != 5*time.Minute {
		t.Errorf("expected 5m TTL, got %v", ttl)
	}
	if r.lru.evictList.Len() != 2 {
		t.Errorf("expected LRU len 2, got %d", r.lru.evictList.Len())
	}

	// Resolve 3rd key -> should cause LRU eviction of device:1:status
	ttl, exists = r.Resolve("device:3:status")
	if !exists || ttl != 5*time.Minute {
		t.Errorf("expected 5m TTL, got %v", ttl)
	}
	if r.lru.evictList.Len() != 2 {
		t.Errorf("expected LRU len to stay capped at 2, got %d", r.lru.evictList.Len())
	}

	// Check that device:1:status was indeed evicted
	if _, exists := r.lru.items["device:1:status"]; exists {
		t.Errorf("expected device:1:status to be evicted from LRU map")
	}
	// Check that device:2:status and device:3:status are still in LRU
	if _, exists := r.lru.items["device:2:status"]; !exists {
		t.Errorf("expected device:2:status to be in LRU map")
	}
	if _, exists := r.lru.items["device:3:status"]; !exists {
		t.Errorf("expected device:3:status to be in LRU map")
	}
}
