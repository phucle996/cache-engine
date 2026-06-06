package cacheEngine_codec

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
)

// Codec định nghĩa hợp đồng mã hóa và giải mã dữ liệu nhị phân.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// JSONCodec là bộ mã hóa giải mã JSON chuẩn.
type JSONCodec struct{}

func NewJSONCodec() Codec {
	return &JSONCodec{}
}

func (c *JSONCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (c *JSONCodec) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

// GobCodec là bộ mã hóa giải mã Gob nhị phân chuẩn của Go.
type GobCodec struct{}

func NewGobCodec() Codec {
	return &GobCodec{}
}

func (c *GobCodec) Marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *GobCodec) Unmarshal(data []byte, v any) error {
	buf := bytes.NewReader(data)
	return gob.NewDecoder(buf).Decode(v)
}
