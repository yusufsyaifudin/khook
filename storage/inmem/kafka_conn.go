package inmem

import (
	"context"
	"fmt"
	"github.com/yusufsyaifudin/khook/pkg/types"
	"github.com/yusufsyaifudin/khook/pkg/validator"
	"github.com/yusufsyaifudin/khook/storage"
	"io"
	"os"
	"sort"
	"sync"
)

type KafkaConnStore struct {
	store   sync.Map
	changes chan storage.OutWatchKafkaConnConfig
}

var _ storage.KafkaConnStore = (*KafkaConnStore)(nil)

func NewKafkaConnStore() *KafkaConnStore {
	return &KafkaConnStore{
		store:   sync.Map{},
		changes: make(chan storage.OutWatchKafkaConnConfig, 100),
	}
}

func (k *KafkaConnStore) PersistKafkaConnConfig(ctx context.Context, in storage.InPersistKafkaConnConfig) (out storage.OutPersistKafkaConnConfig, err error) {
	err = validator.Validate(in)
	if err != nil {
		err = fmt.Errorf("inmem: kafka config validation error: %w", err)
		return
	}

	id := fmt.Sprintf("%s:%s", in.Resource.Namespace, in.Resource.Name)
	kafkaCfg, _ := k.store.LoadOrStore(id, in.Resource)

	k.changes <- storage.OutWatchKafkaConnConfig{
		ChangeType: storage.Put,
		Resource:   kafkaCfg.(types.KafkaBrokerConfig),
	}

	out = storage.OutPersistKafkaConnConfig{
		Resource: kafkaCfg.(types.KafkaBrokerConfig),
	}
	return
}

func (k *KafkaConnStore) GetKafkaConnConfigs(ctx context.Context) (rows storage.KafkaConnConfigRows, err error) {
	kafkaConfigs := make([]types.KafkaBrokerConfig, 0)

	k.store.Range(func(key, value any) bool {
		kafkaConfigs = append(kafkaConfigs, value.(types.KafkaBrokerConfig))
		return true
	})

	sort.Slice(kafkaConfigs, func(i, j int) bool {
		return kafkaConfigs[i].Name < kafkaConfigs[j].Name
	})

	rows = &KafkaConfigRows{
		kafkaConfigs: kafkaConfigs,
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

	out = storage.OutGetKafkaConnConfig{
		Resource: cfg.(types.KafkaBrokerConfig),
	}

	return
}

func (k *KafkaConnStore) WatchKafkaConnConfig(ctx context.Context, out chan storage.OutWatchKafkaConnConfig) {
	for {
		select {
		case c := <-k.changes:
			out <- c
		}
	}
}

func (k *KafkaConnStore) Close() error {
	return nil
}

type KafkaConfigRows struct {
	lock            sync.Mutex
	kafkaConfigs    []types.KafkaBrokerConfig
	currKafkaConfig *types.KafkaBrokerConfig
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

func (w *KafkaConfigRows) KafkaBrokerConfig() (types.KafkaBrokerConfig, error) {
	w.lock.Lock()
	defer w.lock.Unlock()
	if w.currKafkaConfig == nil {
		return types.KafkaBrokerConfig{}, io.ErrUnexpectedEOF
	}

	return *w.currKafkaConfig, nil
}
