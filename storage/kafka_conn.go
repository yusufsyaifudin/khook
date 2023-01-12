package storage

import (
	"context"
	"github.com/yusufsyaifudin/khook/pkg/types"
	"io"
	"os"
)

type KafkaConnStore interface {
	io.Closer
	PersistKafkaConnConfig(ctx context.Context, in InPersistKafkaConnConfig) (out OutPersistKafkaConnConfig, err error)
	GetKafkaConnConfig(ctx context.Context, in InGetKafkaConnConfig) (out OutGetKafkaConnConfig, err error)
	GetKafkaConnConfigs(ctx context.Context) (rows KafkaConnConfigRows, err error)
	WatchKafkaConnConfig(ctx context.Context, out chan OutWatchKafkaConnConfig)
}

type InPersistKafkaConnConfig struct {
	Resource types.KafkaBrokerConfig `validate:"required"`
}

type OutPersistKafkaConnConfig struct {
	Resource types.KafkaBrokerConfig
}

type InGetKafkaConnConfig struct {
	Namespace string `validate:"required"`
	Name      string `validate:"required"`
}

type OutGetKafkaConnConfig struct {
	Resource types.KafkaBrokerConfig
}

type OutWatchKafkaConnConfig struct {
	ChangeType ChangeType
	Resource   types.KafkaBrokerConfig
}

type KafkaConnConfigRows interface {
	Next() bool
	KafkaBrokerConfig() (cfg types.KafkaBrokerConfig, err error)
}

type KafkaConfigNoRows struct{}

var _ KafkaConnConfigRows = (*KafkaConfigNoRows)(nil)

func (n *KafkaConfigNoRows) Next() bool { return false }

func (n *KafkaConfigNoRows) KafkaBrokerConfig() (types.KafkaBrokerConfig, error) {
	return types.KafkaBrokerConfig{}, os.ErrNotExist
}
