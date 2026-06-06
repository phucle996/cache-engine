package cacheEngine_bus

import (
	"encoding/json"
	"time"
)

// SetJSONMarshalForTest overrides the jsonMarshal function for testing JSON failures.
func SetJSONMarshalForTest(f func(v any) ([]byte, error)) {
	jsonMarshal = f
}

// ResetJSONMarshalForTest restores the original jsonMarshal function.
func ResetJSONMarshalForTest() {
	jsonMarshal = json.Marshal
}

// SetBackoffForTest overrides the backoff variables for testing reconnect logic.
func SetBackoffForTest(initial, max time.Duration) {
	subscribeBackoff = initial
	maxBackoff = max
}

// ResetBackoffForTest restores the original backoff variables.
func ResetBackoffForTest() {
	subscribeBackoff = 1 * time.Second
	maxBackoff = 30 * time.Second
}
