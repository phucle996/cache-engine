package cacheEngine_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	cacheEngine_bus "github.com/phucle996/cache-engine/bus"
	cacheEngine_registry "github.com/phucle996/cache-engine/registry"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

// TestBus_SubscribeAndCoverage covers Subscription logic and message boundaries/validation.
func TestBus_SubscribeAndCoverage(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	rdb := goredis.NewClient(&goredis.Options{
		Addr: s.Addr(),
	})
	defer rdb.Close()

	reg := cacheEngine_registry.NewKeyRegistry(10)
	reg.Register("user:profile:*", 10*time.Minute)

	// Set maxMessageBytes to a reasonable value (200 bytes) for testing size limits
	busInstance := cacheEngine_bus.NewBus(rdb, "test-channel", reg, 200)

	// Test Publish jsonMarshal error path
	cacheEngine_bus.SetJSONMarshalForTest(func(v any) ([]byte, error) {
		return nil, fmt.Errorf("mock marshal error")
	})
	errMarshal := busInstance.Publish(context.Background(), "user:profile:123", "upsert", []byte(`{"id":1}`), 1)
	if errMarshal == nil || errMarshal.Error() != "mock marshal error" {
		t.Errorf("expected mock marshal error, got %v", errMarshal)
	}
	cacheEngine_bus.ResetJSONMarshalForTest()

	// Create a second bus with low maxMessageBytes to test Publish size limits
	busSmall := cacheEngine_bus.NewBus(rdb, "test-channel", reg, 5)
	errPublishSmall := busSmall.Publish(context.Background(), "user:profile:123", "upsert", []byte("123456"), 1)
	if errPublishSmall == nil {
		t.Error("expected Publish to fail on small bus")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var receivedMsgs []cacheEngine_bus.FanoutMessage
	var mu sync.Mutex

	errChan := make(chan error, 1)
	go func() {
		errChan <- busInstance.Subscribe(ctx, func(msg cacheEngine_bus.FanoutMessage) {
			mu.Lock()
			receivedMsgs = append(receivedMsgs, msg)
			mu.Unlock()
		})
	}()

	// Wait for subscription to be active
	time.Sleep(100 * time.Millisecond)

	// 1. Publish valid message via Bus
	err := busInstance.Publish(context.Background(), "user:profile:123", "upsert", []byte(`{"id":1}`), 1)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// 2. Publish message with payload size > maxMessageBytes via Bus (fails validation in Publish)
	bigPayload := make([]byte, 300)
	for i := range bigPayload {
		bigPayload[i] = 'a'
	}
	err = busInstance.Publish(context.Background(), "user:profile:123", "upsert", bigPayload, 2)
	if err == nil {
		t.Error("expected Publish to fail for payload size > maxMessageBytes, got nil")
	}

	// 3. Publish direct raw payload on Redis channel to trigger Subscribe Guard 1 (> maxMessageBytes)
	rdb.Publish(context.Background(), "test-channel", string(make([]byte, 300)))

	// 4. Publish invalid JSON message
	rdb.Publish(context.Background(), "test-channel", "invalid-json")

	// 5. Publish JSON but with invalid Op
	badOpMsg := `{"op":"invalid_op","key":"user:profile:123","version":1}`
	rdb.Publish(context.Background(), "test-channel", badOpMsg)

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if len(receivedMsgs) != 1 {
		t.Errorf("expected exactly 1 received message, got %d", len(receivedMsgs))
	} else if receivedMsgs[0].Key != "user:profile:123" {
		t.Errorf("expected key user:profile:123, got %s", receivedMsgs[0].Key)
	}
	mu.Unlock()

	cancel()

	select {
	case err := <-errChan:
		if err != context.Canceled {
			t.Errorf("expected context.Canceled error, got %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Error("Subscribe did not exit after context cancel")
	}
}

// TestBus_SubscribeRetry covers the reconnect and backoff logic in Bus.Subscribe.
func TestBus_SubscribeRetry(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	rdb := goredis.NewClient(&goredis.Options{
		Addr: s.Addr(),
	})

	reg := cacheEngine_registry.NewKeyRegistry(10)
	busInstance := cacheEngine_bus.NewBus(rdb, "test-channel", reg, 0)

	// Setup extremely fast backoff for testing
	cacheEngine_bus.SetBackoffForTest(1 * time.Millisecond, 3 * time.Millisecond)
	defer cacheEngine_bus.ResetBackoffForTest()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errChan := make(chan error, 1)
	go func() {
		errChan <- busInstance.Subscribe(ctx, func(msg cacheEngine_bus.FanoutMessage) {})
	}()

	time.Sleep(100 * time.Millisecond)

	// Close client to trigger connection lost (ok = false in Subscribe)
	rdb.Close()

	// Wait 15ms to trigger multiple retries and trigger backoff cap (maxBackoff is 3ms)
	time.Sleep(15 * time.Millisecond)

	cancel()

	select {
	case err := <-errChan:
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Error("Subscribe did not exit")
	}
}

func TestBus_PublishTTLFromRegistry(t *testing.T) {
	reg := cacheEngine_registry.NewKeyRegistry(10)
	reg.Register("user:profile:*", 10*time.Minute)
	busInstance := cacheEngine_bus.NewBus(nil, "test-channel", reg, 0) // 0 → dùng DefaultMaxMessageBytes

	// 1. Unregistered key phải bị từ chối
	err := busInstance.Publish(context.Background(), "unregistered:key", "upsert", []byte("{}"), 123)
	if err == nil {
		t.Error("expected error for unregistered key, got nil")
	}

	// 2. Op không hợp lệ phải bị từ chối ngay tại Publish (trước khi chạm Redis)
	err = busInstance.Publish(context.Background(), "user:profile:123", "INVALID_OP", []byte("{}"), 123)
	if err == nil {
		t.Error("expected error for invalid op, got nil")
	}

	// 3. Op hợp lệ nhưng client là nil → panic hoặc error từ Redis client, không phải từ validation
	defer func() {
		if r := recover(); r != nil {
			// Panic expected due to nil redis client — validation đã pass, lỗi đến từ transport
		}
	}()
	err = busInstance.Publish(context.Background(), "user:profile:123", "delete", []byte("{}"), 123)
	if err == nil {
		t.Error("expected error/panic due to nil redis client, got nil")
	}
}
