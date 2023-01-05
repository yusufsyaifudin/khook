package inmem

import (
	"context"
	"io"
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

func (k *KafkaConnStore) GetAllKafkaConfig(ctx context.Context) (rows storage.KafkaConfigRows, err error) {
	kafkaConfigs := make([]storage.KafkaConfig, 0)

	k.store.Range(func(key, value any) bool {
		kafkaConfig, ok := value.(storage.KafkaConfig)
		if ok {
			kafkaConfigs = append(kafkaConfigs, kafkaConfig)
		}

		return true
	})

	rows = &KafkaConfigRows{
		kafkaConfigs: kafkaConfigs,
	}
	return
}

func (k *KafkaConnStore) DeleteKafkaConfig(ctx context.Context, in storage.InputDeleteKafkaConfig) (out storage.OutDeleteKafkaConfig, err error) {
	k.store.Delete(in.Label)
	out = storage.OutDeleteKafkaConfig{Success: true}
	return
}

type KafkaConfigRows struct {
	lock            sync.Mutex
	kafkaConfigs    []storage.KafkaConfig
	currKafkaConfig *storage.KafkaConfig
}

var _ storage.KafkaConfigRows = (*KafkaConfigRows)(nil)

func (w *KafkaConfigRows) Next() bool {
	w.lock.Lock()
	defer w.lock.Unlock()

	if len(w.kafkaConfigs) > 0 {
		w.currKafkaConfig, w.kafkaConfigs = &w.kafkaConfigs[0], w.kafkaConfigs[1:]
		return true
	}

	return false
}

func (w *KafkaConfigRows) KafkaConfig() (storage.KafkaConfig, error) {
	w.lock.Lock()
	defer w.lock.Unlock()
	if w.currKafkaConfig == nil {
		return storage.KafkaConfig{}, io.ErrUnexpectedEOF
	}

	return *w.currKafkaConfig, nil
}
