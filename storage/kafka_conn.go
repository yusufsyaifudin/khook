package storage

import (
	"context"
	"encoding/json"
	"fmt"
)

type KafkaConnStore interface {
	PersistKafkaConfig(ctx context.Context, in InputPersistKafkaConfig) (out OutPersistKafkaConfig, err error)
	GetKafkaConfigs(ctx context.Context) (rows KafkaConfigRows, err error)
	DeleteKafkaConfig(ctx context.Context, in InputDeleteKafkaConfig) (out OutDeleteKafkaConfig, err error)
}

type KafkaConfig struct {
	Label   string   `json:"label" validate:"required"`
	Brokers []string `json:"brokers,omitempty" validate:"required"`
}

// Scan will read the data bytes from database and parse it as KafkaConfig
func (m *KafkaConfig) Scan(src interface{}) error {
	if m == nil {
		return fmt.Errorf("error scan service account on nil struct")
	}

	switch v := src.(type) {
	case []byte:
		return json.Unmarshal(v, m)
	case string:
		return json.Unmarshal([]byte(fmt.Sprintf("%s", v)), m)
	}

	return fmt.Errorf("unknown type %T to format as kafka config", src)
}

type KafkaConfigRows interface {
	Next() bool
	KafkaConfig() (cfg KafkaConfig, checkSum string, err error)
}

type NoRows struct{}

var _ KafkaConfigRows = (*NoRows)(nil)

func (n *NoRows) Next() bool                                { return false }
func (n *NoRows) KafkaConfig() (KafkaConfig, string, error) { return KafkaConfig{}, "", nil }

type InputPersistKafkaConfig struct {
	KafkaConfig KafkaConfig `validate:"required"`
}

type OutPersistKafkaConfig struct {
	CheckSum    string
	KafkaConfig KafkaConfig
}

type InputDeleteKafkaConfig struct {
	Label string `validate:"required"`
}

type OutDeleteKafkaConfig struct {
	Success bool
}
