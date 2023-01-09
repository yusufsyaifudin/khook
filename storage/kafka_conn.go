package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
)

type KafkaConnStore interface {
	PersistKafkaConfig(ctx context.Context, in InputPersistKafkaConfig) (out OutPersistKafkaConfig, err error)
	GetKafkaConfigs(ctx context.Context) (rows KafkaConfigRows, err error)
}

type KafkaConnection struct {
	Type     `json:",inline" validate:"required"`
	Metadata `json:",inline" validate:"required"`

	Spec KafkaConfigSpec `json:"spec" validate:"required"`
}

// Scan will read the data bytes from database and parse it as KafkaConnection
func (m *KafkaConnection) Scan(src interface{}) error {
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

func NewKafkaConnection() KafkaConnection {
	conn := KafkaConnection{
		Type: Type{
			ApiVersion: "khook/v1",
			Kind:       "KafkaBrokerConnection",
		},
		Metadata: Metadata{
			Namespace: "default",
		},
		Spec: KafkaConfigSpec{},
	}
	return conn
}

type KafkaConfigSpec struct {
	Brokers []string `json:"brokers,omitempty" validate:"required"`
}

type KafkaConfigRows interface {
	Next() bool
	KafkaConnection() (cfg KafkaConnection, state ResourceState, err error)
}

type KafkaConfigNoRows struct{}

var _ KafkaConfigRows = (*KafkaConfigNoRows)(nil)

func (n *KafkaConfigNoRows) Next() bool { return false }
func (n *KafkaConfigNoRows) KafkaConnection() (KafkaConnection, ResourceState, error) {
	return KafkaConnection{}, ResourceState{}, os.ErrNotExist
}

type InputPersistKafkaConfig struct {
	KafkaConfig   KafkaConnection `validate:"required"`
	ResourceState ResourceState   `validate:"required"`
}

type OutPersistKafkaConfig struct {
	KafkaConfig   KafkaConnection
	ResourceState ResourceState
}
