package storage

import "context"

type KafkaConsumerStore interface {
	PersistKafkaConsumer(ctx context.Context, in InputPersistKafkaConsumer) (out OutPersistKafkaConsumer, err error)
	GetKafkaConsumers(ctx context.Context) (out KafkaConsumerRows, err error)
	GetKafkaConsumer(ctx context.Context, in InGetKafkaConsumer) (out OutGetKafkaConsumer, err error)
}

type InputPersistKafkaConsumer struct {
	KafkaConsumerConfig KafkaConsumerConfig `validate:"required"`
}

type OutPersistKafkaConsumer struct {
	KafkaConsumerConfig KafkaConsumerConfig
}

type InGetKafkaConsumer struct {
	Namespace string `validate:"required"`
	Name      string `validate:"required"`
}

type OutGetKafkaConsumer struct {
	KafkaConsumerConfig KafkaConsumerConfig
}

// KafkaConsumerRows contains list of KafkaConsumerConfig
type KafkaConsumerRows interface {
	Next() bool
	KafkaConsumerConfig() (w KafkaConsumerConfig, err error)
}
