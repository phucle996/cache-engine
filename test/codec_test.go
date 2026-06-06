package cacheEngine_test

import (
	"testing"

	cacheEngine_codec "github.com/phucle996/cache-engine/codec"
)

func TestCodecs(t *testing.T) {
	user := TestUser{ID: 42, Name: "Alice"}

	// 1. Test JSONCodec
	jsonCodec := cacheEngine_codec.NewJSONCodec()
	data, err := jsonCodec.Marshal(user)
	if err != nil {
		t.Fatalf("JSON Marshal failed: %v", err)
	}
	var decodedJSON TestUser
	if err := jsonCodec.Unmarshal(data, &decodedJSON); err != nil {
		t.Fatalf("JSON Unmarshal failed: %v", err)
	}
	if decodedJSON != user {
		t.Errorf("expected %v, got %v", user, decodedJSON)
	}

	// 2. Test GobCodec
	gobCodec := cacheEngine_codec.NewGobCodec()
	dataGob, err := gobCodec.Marshal(user)
	if err != nil {
		t.Fatalf("Gob Marshal failed: %v", err)
	}
	var decodedGob TestUser
	if err := gobCodec.Unmarshal(dataGob, &decodedGob); err != nil {
		t.Fatalf("Gob Unmarshal failed: %v", err)
	}
	if decodedGob != user {
		t.Errorf("expected %v, got %v", user, decodedGob)
	}
}

// TestGobCodec_MarshalError covers the error path of Gob encoding.
func TestGobCodec_MarshalError(t *testing.T) {
	codec := cacheEngine_codec.NewGobCodec()
	_, err := codec.Marshal(func() {})
	if err == nil {
		t.Error("expected error when marshalling a function with Gob, got nil")
	}
}
