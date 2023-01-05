package storage

import "context"

type KafkaConnStore interface {
	PersistKafkaConfig(ctx context.Context, in InputPersistKafkaConfig) (out OutPersistKafkaConfig, err error)
	GetAllKafkaConfig(ctx context.Context) (rows KafkaConfigRows, err error)
	DeleteKafkaConfig(ctx context.Context, in InputDeleteKafkaConfig) (out OutDeleteKafkaConfig, err error)
}

type KafkaConfig struct {
	Label   string   `json:"label"`
	Address []string `json:"address,omitempty"`
}

type KafkaConfigRows interface {
	Next() bool
	KafkaConfig() (KafkaConfig, error)
}

type InputPersistKafkaConfig struct {
	KafkaConfig KafkaConfig `validate:"required"`
}

type OutPersistKafkaConfig struct {
	KafkaConfig KafkaConfig
}

type InputDeleteKafkaConfig struct {
	Label string `validate:"required"`
}

type OutDeleteKafkaConfig struct {
	Success bool
}
