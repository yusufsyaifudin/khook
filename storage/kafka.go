package storage

import "context"

type KafkaConnStore interface {
	PersistKafkaConfig(ctx context.Context, in InputPersistKafkaConfig) (out OutPersistKafkaConfig, err error)
	GetAllKafkaConfig(ctx context.Context) (out OutGetAllKafkaConfig, err error)
	DeleteKafkaConfig(ctx context.Context, in InputDeleteKafkaConfig) (out OutDeleteKafkaConfig, err error)
}

type KafkaConfig struct {
	Label   string   `json:"label"`
	Address []string `json:"address,omitempty"`
}

type InputPersistKafkaConfig struct {
	KafkaConfig KafkaConfig `validate:"required"`
}

type OutPersistKafkaConfig struct {
	KafkaConfig KafkaConfig
}

type OutGetAllKafkaConfig struct {
	KafkaConfigs []KafkaConfig
}

type InputDeleteKafkaConfig struct {
	Label string `validate:"required"`
}

type OutDeleteKafkaConfig struct {
	Success bool
}
