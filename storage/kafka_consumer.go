package storage

import "context"

type KafkaConsumerStore interface {
	PersistKafkaConsumer(ctx context.Context, in InputPersistKafkaConsumer) (out OutPersistKafkaConsumer, err error)
	GetKafkaConsumers(ctx context.Context) (out KafkaConsumerRows, err error)
	GetSinkTargetByLabel(ctx context.Context, label string) (out SinkTarget, err error)
}

type InputPersistKafkaConsumer struct {
	ConsumerConfigRow ConsumerConfigRow `validate:"required"`
}

type OutPersistKafkaConsumer struct {
	ConsumerConfigRow ConsumerConfigRow
}

// KafkaConsumerRows contains list of SinkTarget
type KafkaConsumerRows interface {
	Next() bool
	ConsumerConfigRow() (w ConsumerConfigRow, err error)
}
