package cacheEngine_test

import (
	"testing"
	"time"

	cacheEngine_registry "github.com/phucle996/cache-engine/registry"
)

func TestKeyRegistry_PrefixFastPath(t *testing.T) {
	r := cacheEngine_registry.NewKeyRegistry(10)

	// Register a suffix wildcard
	r.Register("user:profile:*", 10*time.Minute)

	// Verify it went to prefixKeys map
	if ttl, ok := r.HasPrefix("user:profile:"); !ok || ttl != 10*time.Minute {
		t.Errorf("expected user:profile: in prefixKeys map, got %v, ok=%v", ttl, ok)
	}

	// Verify length was recorded
	foundLen := false
	for _, l := range r.PrefixLengths() {
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
	if r.LRULen() != 0 {
		t.Errorf("expected LRU cache length to be 0, got %d", r.LRULen())
	}
}

func TestKeyRegistry_ComplexWildcardLRU(t *testing.T) {
	// LRU Capacity = 2
	r := cacheEngine_registry.NewKeyRegistry(2)

	// Register a complex wildcard (wildcard in the middle)
	r.Register("device:*:status", 5*time.Minute)

	// Verify it went to patterns slice, not prefixKeys
	if r.PatternsCount() != 1 {
		t.Errorf("expected patterns slice to contain 1 entry, got %d", r.PatternsCount())
	}
	if _, ok := r.HasPrefix("device:"); ok {
		t.Errorf("expected prefixKeys map to be empty, but it resolved device:")
	}

	// Resolve 1st key -> should enter LRU
	ttl, exists := r.Resolve("device:1:status")
	if !exists || ttl != 5*time.Minute {
		t.Errorf("expected 5m TTL, got %v", ttl)
	}
	if r.LRULen() != 1 {
		t.Errorf("expected LRU len 1, got %d", r.LRULen())
	}

	// Resolve 2nd key -> should enter LRU
	ttl, exists = r.Resolve("device:2:status")
	if !exists || ttl != 5*time.Minute {
		t.Errorf("expected 5m TTL, got %v", ttl)
	}
	if r.LRULen() != 2 {
		t.Errorf("expected LRU len 2, got %d", r.LRULen())
	}

	// Resolve 3rd key -> should cause LRU eviction of device:1:status
	ttl, exists = r.Resolve("device:3:status")
	if !exists || ttl != 5*time.Minute {
		t.Errorf("expected 5m TTL, got %v", ttl)
	}
	if r.LRULen() != 2 {
		t.Errorf("expected LRU len to stay capped at 2, got %d", r.LRULen())
	}

	// Check that device:1:status was indeed evicted
	if r.LRUHas("device:1:status") {
		t.Errorf("expected device:1:status to be evicted from LRU map")
	}
	// Check that device:2:status and device:3:status are still in LRU
	if !r.LRUHas("device:2:status") {
		t.Errorf("expected device:2:status to be in LRU map")
	}
	if !r.LRUHas("device:3:status") {
		t.Errorf("expected device:3:status to be in LRU map")
	}
}

// TestRegistry_Coverage covers edge cases in key registry.
func TestRegistry_Coverage(t *testing.T) {
	// 1. NewKeyRegistry with 0 capacity
	r0 := cacheEngine_registry.NewKeyRegistry(0)
	if r0 == nil {
		t.Fatal("NewKeyRegistry(0) returned nil")
	}

	// 2. ClearLRU & Resolve LRU hit path & Register existing prefix length path
	r := cacheEngine_registry.NewKeyRegistry(2)
	// Register two prefix keys with the same length (13) to cover suffix duplicate length branch
	r.Register("user:profile:*", 5*time.Minute)
	r.Register("user:account:*", 10*time.Minute)

	r.Register("device:*:status", 5*time.Minute)
	// First resolve: LRU miss
	_, _ = r.Resolve("device:1:status")
	if r.LRULen() != 1 {
		t.Errorf("expected LRU len 1, got %d", r.LRULen())
	}
	// Second resolve: LRU hit
	_, _ = r.Resolve("device:1:status")

	r.ClearLRU()
	if r.LRULen() != 0 {
		t.Errorf("expected LRU len 0 after ClearLRU, got %d", r.LRULen())
	}

	// 3. parseSuffixWildcard error conditions
	r2 := cacheEngine_registry.NewKeyRegistry(10)
	// Contains ?
	r2.Register("user:?:*", 1*time.Minute)
	// Contains [
	r2.Register("user:[a-z]:*", 1*time.Minute)
	// Contains * not at the end
	r2.Register("user:*:profile", 1*time.Minute)
	// Resolve keys to verify they fall back to slow path or don't match prefix
	_, _ = r2.Resolve("user:1:profile")

	// 4. path.Match error in Resolve
	r3 := cacheEngine_registry.NewKeyRegistry(10)
	r3.Register("user:[a-", 1*time.Minute)        // malformed pattern
	r3.Register("user:*:profile", 5*time.Minute) // valid pattern after it
	// Resolve -> should skip the malformed pattern and match the second one
	ttl, exists := r3.Resolve("user:1:profile")
	if !exists || ttl != 5*time.Minute {
		t.Errorf("expected 5m TTL for user:1:profile, got %v, exists=%v", ttl, exists)
	}
}

func TestKeyRegistry_WildcardAndLRU(t *testing.T) {
	reg := cacheEngine_registry.NewKeyRegistry(2) // Capacity = 2
	reg.Register("user:profile:*", 10*time.Minute)
	reg.Register("device:*:status", 5*time.Minute)
	reg.Register("system:settings", 30*time.Second)

	// 1. Test static key
	ttl, exists := reg.Resolve("system:settings")
	if !exists || ttl != 30*time.Second {
		t.Errorf("expected 30s TTL, got %v, exists=%v", ttl, exists)
	}

	// 2. Test wildcard key
	ttl, exists = reg.Resolve("user:profile:12345")
	if !exists || ttl != 10*time.Minute {
		t.Errorf("expected 10m TTL, got %v, exists=%v", ttl, exists)
	}

	// 3. Test another wildcard key
	ttl, exists = reg.Resolve("device:99:status")
	if !exists || ttl != 5*time.Minute {
		t.Errorf("expected 5m TTL, got %v, exists=%v", ttl, exists)
	}

	// 4. Test resolving another key matching the prefix pattern.
	// Since "user:profile:*" is a suffix wildcard, it uses prefix-based fast-path and does not enter LRU.
	ttl, exists = reg.Resolve("user:profile:67890")
	if !exists || ttl != 10*time.Minute {
		t.Errorf("expected 10m TTL, got %v, exists=%v", ttl, exists)
	}
}
