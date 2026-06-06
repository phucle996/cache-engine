package cacheEngine_test

import (
	"testing"
	"time"

	cacheEngine_jitter "github.com/phucle996/cache-engine/jitter"
)

// TestJitter covers edge cases for TTL jitter.
func TestJitter(t *testing.T) {
	// 1. ttl <= 0
	if cacheEngine_jitter.ApplyTTLJitter(0) != 0 {
		t.Error("expected 0 for 0 ttl")
	}
	if cacheEngine_jitter.ApplyTTLJitter(-5) != -5 {
		t.Error("expected -5 for -5 ttl")
	}

	// 2. skew < 5s
	_ = cacheEngine_jitter.ApplyTTLJitter(10 * time.Second)

	// 3. skew > 30s
	_ = cacheEngine_jitter.ApplyTTLJitter(500 * time.Second)

	// 4. jittered <= 1s
	foundFloor := false
	for i := 0; i < 1000; i++ {
		res := cacheEngine_jitter.ApplyTTLJitter(2 * time.Second)
		if res == 1*time.Second {
			foundFloor = true
			break
		}
	}
	if !foundFloor {
		t.Error("failed to hit jittered <= 1s floor branch in 1000 runs")
	}
}
