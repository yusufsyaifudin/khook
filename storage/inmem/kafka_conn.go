package inmem

import (
	"context"
	"fmt"
	"github.com/yusufsyaifudin/khook/pkg/validator"
	"io"
	"sync"
	"time"

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
	err = validator.Validate(in)
	if err != nil {
		err = fmt.Errorf("inmem: kafka config validation error: %w", err)
		return
	}

	kafkaCfg, _ := k.store.LoadOrStore(in.KafkaConfig.Name, in.KafkaConfig)
	out = storage.OutPersistKafkaConfig{
		KafkaConfig: kafkaCfg.(storage.KafkaConnection),
		ResourceState: storage.ResourceState{
			Rev:       1,
			Status:    storage.Start,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
	}
	return
}

func (k *KafkaConnStore) GetKafkaConfigs(ctx context.Context) (rows storage.KafkaConfigRows, err error) {
	kafkaConfigs := make([]storage.KafkaConnection, 0)

	k.store.Range(func(key, value any) bool {
		kafkaConfig, ok := value.(storage.KafkaConnection)
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

type KafkaConfigRows struct {
	lock            sync.Mutex
	kafkaConfigs    []storage.KafkaConnection
	currKafkaConfig *storage.KafkaConnection
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

func (w *KafkaConfigRows) KafkaConnection() (storage.KafkaConnection, storage.ResourceState, error) {
	w.lock.Lock()
	defer w.lock.Unlock()
	if w.currKafkaConfig == nil {
		return storage.KafkaConnection{}, storage.ResourceState{}, io.ErrUnexpectedEOF
	}

	return *w.currKafkaConfig, storage.ResourceState{
		Rev:       1,
		Status:    storage.Start,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}, nil
}
