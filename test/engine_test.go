package cacheEngine_test

import (
	"os"
	"testing"
	"time"

	cacheengine "github.com/phucle996/cache-engine"
)

func TestInitEngine_Success(t *testing.T) {
	configContent := `[
		{"key": "user:profile", "ttl": "10m"},
		{"key": "system:settings", "ttl": "30s"}
	]`
	tmpFile, err := os.CreateTemp("", "cache_key_config_*.json")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(configContent); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	// 1. Test InitLocalEngine
	localMgr, err := cacheengine.InitLocalEngine(tmpFile.Name())
	if err != nil {
		t.Fatalf("InitLocalEngine failed: %v", err)
	}
	ttl, exists := localMgr.GetTTL("user:profile")
	if !exists || ttl != 10*time.Minute {
		t.Errorf("expected 10m TTL for user:profile, got %v", ttl)
	}

	// 2. Test InitRedisEngine
	redisMgr, err := cacheengine.InitRedisEngine(nil, tmpFile.Name())
	if err != nil {
		t.Fatalf("InitRedisEngine failed: %v", err)
	}
	ttl, exists = redisMgr.GetTTL("system:settings")
	if !exists || ttl != 30*time.Second {
		t.Errorf("expected 30s TTL for system:settings in redisMgr, got %v", ttl)
	}
}

// TestEngine_Errors covers error configurations when loading JSON engines.
func TestEngine_Errors(t *testing.T) {
	// 1. File not found
	_, err := cacheengine.InitLocalEngine("non-existent-file.json")
	if err == nil {
		t.Error("expected error for non-existent file, got nil")
	}

	_, err = cacheengine.InitRedisEngine(nil, "non-existent-file.json")
	if err == nil {
		t.Error("expected error for non-existent file, got nil")
	}

	// 2. Invalid JSON format
	tmpFile, err := os.CreateTemp("", "invalid_config_*.json")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	_, _ = tmpFile.WriteString("invalid json content")
	tmpFile.Close()

	_, err = cacheengine.InitLocalEngine(tmpFile.Name())
	if err == nil {
		t.Error("expected error for invalid json, got nil")
	}

	_, err = cacheengine.InitRedisEngine(nil, tmpFile.Name())
	if err == nil {
		t.Error("expected error for invalid json, got nil")
	}

	// 3. Invalid TTL format
	tmpFile2, err := os.CreateTemp("", "invalid_ttl_config_*.json")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile2.Name())
	_, _ = tmpFile2.WriteString(`[{"key": "user:profile", "ttl": "invalid-duration"}]`)
	tmpFile2.Close()

	_, err = cacheengine.InitLocalEngine(tmpFile2.Name())
	if err == nil {
		t.Error("expected error for invalid TTL duration, got nil")
	}

	_, err = cacheengine.InitRedisEngine(nil, tmpFile2.Name())
	if err == nil {
		t.Error("expected error for invalid TTL duration, got nil")
	}
}
