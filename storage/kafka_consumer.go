package storage

import (
	"context"
	"github.com/yusufsyaifudin/khook/pkg/types"
	"io"
)

type KafkaConsumerStore interface {
	io.Closer
	PersistKafkaConsumer(ctx context.Context, in InputPersistKafkaConsumer) (out OutPersistKafkaConsumer, err error)
	GetKafkaConsumers(ctx context.Context) (out KafkaConsumerRows, err error)
	GetKafkaConsumer(ctx context.Context, in InGetKafkaConsumer) (out OutGetKafkaConsumer, err error)
	WatchKafkaConsumer(ctx context.Context, out chan OutWatchKafkaConsumer)
}

type InputPersistKafkaConsumer struct {
	Resource types.KafkaConsumerConfig `validate:"required"`
}

type OutPersistKafkaConsumer struct {
	Resource types.KafkaConsumerConfig
}

type InGetKafkaConsumer struct {
	Namespace string `validate:"required"`
	Name      string `validate:"required"`
}

type OutGetKafkaConsumer struct {
	Resource types.KafkaConsumerConfig
}

type OutWatchKafkaConsumer struct {
	ChangeType ChangeType
	Resource   types.KafkaConsumerConfig
}

// KafkaConsumerRows contains list of KafkaConsumerConfig
type KafkaConsumerRows interface {
	Next() bool
	KafkaConsumerConfig() (w types.KafkaConsumerConfig, err error)
}
