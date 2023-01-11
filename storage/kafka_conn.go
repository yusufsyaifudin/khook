package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
)

type ChangeType string

const (
	Put    ChangeType = "Put"
	Delete ChangeType = "Put"
)

type KafkaConnStore interface {
	PersistKafkaConnConfig(ctx context.Context, in InPersistKafkaConnConfig) (out OutPersistKafkaConnConfig, err error)
	GetKafkaConnConfig(ctx context.Context, in InGetKafkaConnConfig) (out OutGetKafkaConnConfig, err error)
	GetKafkaConnConfigs(ctx context.Context) (rows KafkaConnConfigRows, err error)
	WatchKafkaConnConfig(ctx context.Context, out chan OutWatchKafkaConnConfig)
}

type InPersistKafkaConnConfig struct {
	KafkaConfig KafkaConnectionConfig `validate:"required"`
}

type OutPersistKafkaConnConfig struct {
	KafkaConfig KafkaConnectionConfig
}

type InGetKafkaConnConfig struct {
	Namespace string `validate:"required"`
	Name      string `validate:"required"`
}

type OutGetKafkaConnConfig struct {
	KafkaConfig KafkaConnectionConfig
}

type OutWatchKafkaConnConfig struct {
	ChangeType  ChangeType
	KafkaConfig KafkaConnectionConfig
}

type KafkaConnConfigRows interface {
	Next() bool
	KafkaConnection() (cfg KafkaConnectionConfig, err error)
}

type KafkaConfigNoRows struct{}

var _ KafkaConnConfigRows = (*KafkaConfigNoRows)(nil)

func (n *KafkaConfigNoRows) Next() bool { return false }
func (n *KafkaConfigNoRows) KafkaConnection() (KafkaConnectionConfig, error) {
	return KafkaConnectionConfig{}, os.ErrNotExist
}

type KafkaConfigSpec struct {
	Brokers []string `json:"brokers,omitempty" validate:"required"`
}

type KafkaConnectionConfig struct {
	Type     `json:",inline" validate:"required"`
	Metadata `json:",inline" validate:"required"`

	Spec KafkaConfigSpec `json:"spec" validate:"required"`
}

// Scan will read the data bytes from database and parse it as KafkaConnectionConfig
func (m *KafkaConnectionConfig) Scan(src interface{}) error {
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

func NewKafkaConnectionConfig() KafkaConnectionConfig {
	conn := KafkaConnectionConfig{
		Type: Type{
			ApiVersion: "khook/v1",
			Kind:       KindKafkaConnection,
		},
		Metadata: Metadata{
			Namespace: "default",
		},
		Spec: KafkaConfigSpec{},
	}
	return conn
}
