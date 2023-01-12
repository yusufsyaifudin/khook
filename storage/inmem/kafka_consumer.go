package inmem

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/yusufsyaifudin/khook/pkg/types"
	"github.com/yusufsyaifudin/khook/pkg/validator"
	"github.com/yusufsyaifudin/khook/storage"
	"io"
	"sort"
	"sync"
)

type KafkaConsumerStore struct {
	store   sync.Map
	changes chan storage.OutWatchKafkaConsumer
}

var _ storage.KafkaConsumerStore = (*KafkaConsumerStore)(nil)

func NewKafkaConsumerStore() *KafkaConsumerStore {
	return &KafkaConsumerStore{
		store:   sync.Map{},
		changes: make(chan storage.OutWatchKafkaConsumer, 10),
	}
}

func (k *KafkaConsumerStore) PersistKafkaConsumer(ctx context.Context, in storage.InputPersistKafkaConsumer) (out storage.OutPersistKafkaConsumer, err error) {
	err = validator.Validate(in)
	if err != nil {
		err = fmt.Errorf("inmem: input validation error: %k", err)
		return
	}

	id := fmt.Sprintf("%s:%s", in.Resource.Namespace, in.Resource.Name)
	actual, _ := k.store.LoadOrStore(id, in.Resource)

	k.changes <- storage.OutWatchKafkaConsumer{
		ChangeType: storage.Put,
		Resource:   actual.(types.KafkaConsumerConfig),
	}

	out = storage.OutPersistKafkaConsumer{
		Resource: actual.(types.KafkaConsumerConfig),
	}

	return
}

func (k *KafkaConsumerStore) GetKafkaConsumer(ctx context.Context, in storage.InGetKafkaConsumer) (out storage.OutGetKafkaConsumer, err error) {
	id := fmt.Sprintf("%s:%s", in.Namespace, in.Name)

	consumer, exist := k.store.Load(id)
	if !exist {
		err = sql.ErrNoRows
		return
	}

	out = storage.OutGetKafkaConsumer{
		Resource: consumer.(types.KafkaConsumerConfig),
	}

	return
}

func (k *KafkaConsumerStore) GetKafkaConsumers(ctx context.Context) (out storage.KafkaConsumerRows, err error) {
	consumers := make([]types.KafkaConsumerConfig, 0)

	k.store.Range(func(key, value any) bool {
		consumers = append(consumers, value.(types.KafkaConsumerConfig))
		return true
	})

	sort.Slice(consumers, func(i, j int) bool {
		return consumers[i].Name < consumers[j].Name
	})

	out = &WebhookRows{
		rows: consumers,
	}
	return
}

func (k *KafkaConsumerStore) WatchKafkaConsumer(ctx context.Context, out chan storage.OutWatchKafkaConsumer) {
	for {
		select {
		case c := <-k.changes:
			out <- c
		}
	}
}

func (k *KafkaConsumerStore) Close() error {
	return nil
}

type WebhookRows struct {
	lock    sync.Mutex
	rows    []types.KafkaConsumerConfig
	currRow *types.KafkaConsumerConfig
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

func (w *WebhookRows) KafkaConsumerConfig() (types.KafkaConsumerConfig, error) {
	w.lock.Lock()
	defer w.lock.Unlock()
	if w.currRow == nil {
		return types.KafkaConsumerConfig{}, io.ErrUnexpectedEOF
	}

	return *w.currRow, nil
}
