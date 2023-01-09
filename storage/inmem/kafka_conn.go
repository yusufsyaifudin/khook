package inmem

import (
	"context"
	"fmt"
	"github.com/yusufsyaifudin/khook/pkg/validator"
	"github.com/yusufsyaifudin/khook/storage"
	"io"
	"os"
	"sync"
)

type KafkaConnStore struct {
	store      sync.Map
	storeState sync.Map
}

var _ storage.KafkaConnStore = (*KafkaConnStore)(nil)

func NewKafkaConnStore() *KafkaConnStore {
	return &KafkaConnStore{
		store: sync.Map{},
	}
}

func (k *KafkaConnStore) PersistKafkaConnConfig(ctx context.Context, in storage.InPersistKafkaConnConfig) (out storage.OutPersistKafkaConnConfig, err error) {
	err = validator.Validate(in)
	if err != nil {
		err = fmt.Errorf("inmem: kafka config validation error: %w", err)
		return
	}

	id := fmt.Sprintf("%s:%s", in.KafkaConfig.Namespace, in.KafkaConfig.Name)
	kafkaCfg, _ := k.store.LoadOrStore(id, in.KafkaConfig)
	state, _ := k.storeState.LoadOrStore(id, in.ResourceState)

	out = storage.OutPersistKafkaConnConfig{
		KafkaConfig:   kafkaCfg.(storage.KafkaConnectionConfig),
		ResourceState: state.(storage.ResourceState),
	}
	return
}

func (k *KafkaConnStore) GetKafkaConnConfigs(ctx context.Context) (rows storage.KafkaConnConfigRows, err error) {
	kafkaConfigs := make([]storage.KafkaConnectionConfig, 0)
	states := make(map[string]storage.ResourceState)

	k.store.Range(func(key, value any) bool {
		kafkaConfigs = append(kafkaConfigs, value.(storage.KafkaConnectionConfig))
		return true
	})

	k.storeState.Range(func(key, value any) bool {
		states[key.(string)] = value.(storage.ResourceState)
		return true
	})

	rows = &KafkaConfigRows{
		kafkaConfigs: kafkaConfigs,
		states:       states,
	}
	return
}

func (k *KafkaConnStore) GetKafkaConnConfig(ctx context.Context, in storage.InGetKafkaConnConfig) (out storage.OutGetKafkaConnConfig, err error) {
	id := fmt.Sprintf("%s:%s", in.Namespace, in.Name)

	cfg, exist := k.store.Load(id)
	if !exist {
		err = os.ErrNotExist
		return
	}

	state, exist := k.storeState.Load(id)
	if !exist {
		err = os.ErrNotExist
		return
	}

	out = storage.OutGetKafkaConnConfig{
		KafkaConfig:   cfg.(storage.KafkaConnectionConfig),
		ResourceState: state.(storage.ResourceState),
	}

	return
}

type KafkaConfigRows struct {
	lock            sync.Mutex
	kafkaConfigs    []storage.KafkaConnectionConfig
	states          map[string]storage.ResourceState
	currKafkaConfig *storage.KafkaConnectionConfig
}

var _ storage.KafkaConnConfigRows = (*KafkaConfigRows)(nil)

func (w *KafkaConfigRows) Next() bool {
	w.lock.Lock()
	defer w.lock.Unlock()

	if len(w.kafkaConfigs) > 0 {
		w.currKafkaConfig, w.kafkaConfigs = &w.kafkaConfigs[0], w.kafkaConfigs[1:]
		return true
	}

	return false
}

func (w *KafkaConfigRows) KafkaConnection() (storage.KafkaConnectionConfig, storage.ResourceState, error) {
	w.lock.Lock()
	defer w.lock.Unlock()
	if w.currKafkaConfig == nil {
		return storage.KafkaConnectionConfig{}, storage.ResourceState{}, io.ErrUnexpectedEOF
	}

	id := fmt.Sprintf("%s:%s", w.currKafkaConfig.Namespace, w.currKafkaConfig.Name)
	return *w.currKafkaConfig, w.states[id], nil
}
