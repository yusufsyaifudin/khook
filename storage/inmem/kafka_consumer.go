package inmem

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/yusufsyaifudin/khook/pkg/validator"
	"github.com/yusufsyaifudin/khook/storage"
	"io"
	"sync"
)

type KafkaConsumerStore struct {
	store      sync.Map
	storeState sync.Map
}

var _ storage.KafkaConsumerStore = (*KafkaConsumerStore)(nil)

func NewKafkaConsumerStore() *KafkaConsumerStore {
	return &KafkaConsumerStore{
		store:      sync.Map{},
		storeState: sync.Map{},
	}
}

func (w *KafkaConsumerStore) PersistKafkaConsumer(ctx context.Context, in storage.InputPersistKafkaConsumer) (out storage.OutPersistKafkaConsumer, err error) {
	err = validator.Validate(in)
	if err != nil {
		err = fmt.Errorf("inmem: input validation error: %w", err)
		return
	}

	id := fmt.Sprintf("%s:%s", in.KafkaConsumerConfig.Namespace, in.KafkaConsumerConfig.Name)
	actual, _ := w.store.LoadOrStore(id, in.KafkaConsumerConfig)
	state, _ := w.storeState.LoadOrStore(id, in.ResourceState)

	out = storage.OutPersistKafkaConsumer{
		KafkaConsumerConfig: actual.(storage.KafkaConsumerConfig),
		ResourceState:       state.(storage.ResourceState),
	}

	return
}

func (w *KafkaConsumerStore) GetKafkaConsumer(ctx context.Context, in storage.InGetKafkaConsumer) (out storage.OutGetKafkaConsumer, err error) {
	id := fmt.Sprintf("%s:%s", in.Namespace, in.Name)

	consumer, exist := w.store.Load(id)
	if !exist {
		err = sql.ErrNoRows
		return
	}

	state, exist := w.storeState.Load(id)
	if !exist {
		err = sql.ErrNoRows
		return
	}

	out = storage.OutGetKafkaConsumer{
		KafkaConsumerConfig: consumer.(storage.KafkaConsumerConfig),
		ResourceState:       state.(storage.ResourceState),
	}

	return
}

func (w *KafkaConsumerStore) GetKafkaConsumers(ctx context.Context) (out storage.KafkaConsumerRows, err error) {
	consumers := make([]storage.KafkaConsumerConfig, 0)
	states := make(map[string]storage.ResourceState)

	w.store.Range(func(key, value any) bool {
		consumers = append(consumers, value.(storage.KafkaConsumerConfig))
		return true
	})

	w.storeState.Range(func(key, value any) bool {
		states[key.(string)] = value.(storage.ResourceState)
		return true
	})

	out = &WebhookRows{
		rows:   consumers,
		states: states,
	}
	return
}

type WebhookRows struct {
	lock    sync.Mutex
	rows    []storage.KafkaConsumerConfig
	states  map[string]storage.ResourceState
	currRow *storage.KafkaConsumerConfig
}

var _ storage.KafkaConsumerRows = (*WebhookRows)(nil)

func (w *WebhookRows) Next() bool {
	w.lock.Lock()
	defer w.lock.Unlock()

	if len(w.rows) > 0 {
		w.currRow, w.rows = &w.rows[0], w.rows[1:]
		return true
	}

	return false
}

func (w *WebhookRows) KafkaConsumerConfig() (storage.KafkaConsumerConfig, storage.ResourceState, error) {
	w.lock.Lock()
	defer w.lock.Unlock()
	if w.currRow == nil {
		return storage.KafkaConsumerConfig{}, storage.ResourceState{}, io.ErrUnexpectedEOF
	}

	id := fmt.Sprintf("%s:%s", w.currRow.Namespace, w.currRow.Name)
	return *w.currRow, w.states[id], nil
}
