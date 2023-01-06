package storage

import "context"

type KafkaConsumerStore interface {
	PersistKafkaConsumer(ctx context.Context, in InputPersistKafkaConsumer) (out OutPersistKafkaConsumer, err error)
	GetKafkaConsumers(ctx context.Context) (out KafkaConsumerRows, err error)
	GetSinkTargetByLabel(ctx context.Context, label string) (out SinkTarget, err error)
}

type InputPersistKafkaConsumer struct {
	SinkTarget SinkTarget `validate:"required"`
}

type OutPersistKafkaConsumer struct {
	Checksum   string
	SinkTarget SinkTarget
}

// KafkaConsumerRows contains list of SinkTarget
type KafkaConsumerRows interface {
	Next() bool
	SinkTarget() (w SinkTarget, checksum string, err error)
}
