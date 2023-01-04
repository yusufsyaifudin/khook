package inmem

import (
	"context"
	"sync"

	"github.com/yusufsyaifudin/khook/storage"
)

type KafkaConnStore struct {
	store sync.Map
}

var _ storage.KafkaConnStore = (*KafkaConnStore)(nil)

func NewKafkaConnStore() *KafkaConnStore {
	return &KafkaConnStore{
		store: sync.Map{},
	}
}

func (k *KafkaConnStore) PersistKafkaConfig(ctx context.Context, in storage.InputPersistKafkaConfig) (out storage.OutPersistKafkaConfig, err error) {
	kafkaCfg, _ := k.store.LoadOrStore(in.KafkaConfig.Label, in.KafkaConfig)
	out = storage.OutPersistKafkaConfig{KafkaConfig: kafkaCfg.(storage.KafkaConfig)}
	return
}

func (k *KafkaConnStore) GetAllKafkaConfig(ctx context.Context) (out storage.OutGetAllKafkaConfig, err error) {
	kafkaConfigs := make([]storage.KafkaConfig, 0)

	k.store.Range(func(key, value any) bool {
		kafkaConfig, ok := value.(storage.KafkaConfig)
		if ok {
			kafkaConfigs = append(kafkaConfigs, kafkaConfig)
		}

		return true
	})

	out = storage.OutGetAllKafkaConfig{KafkaConfigs: kafkaConfigs}
	return
}

func (k *KafkaConnStore) DeleteKafkaConfig(ctx context.Context, in storage.InputDeleteKafkaConfig) (out storage.OutDeleteKafkaConfig, err error) {
	k.store.Delete(in.Label)
	out = storage.OutDeleteKafkaConfig{Success: true}
	return
}
