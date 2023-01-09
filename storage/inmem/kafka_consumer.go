package inmem

import (
	"context"
	"fmt"
	"github.com/yusufsyaifudin/khook/pkg/validator"
	"io"
	"os"
	"sync"

	"github.com/yusufsyaifudin/khook/storage"
)

type KafkaConsumerStore struct {
	store sync.Map
}

var _ storage.KafkaConsumerStore = (*KafkaConsumerStore)(nil)

func NewKafkaConsumerStore() *KafkaConsumerStore {
	return &KafkaConsumerStore{
		store: sync.Map{},
	}
}

func (w *KafkaConsumerStore) PersistKafkaConsumer(ctx context.Context, in storage.InputPersistKafkaConsumer) (out storage.OutPersistKafkaConsumer, err error) {
	err = validator.Validate(in)
	if err != nil {
		err = fmt.Errorf("inmem: input validation error: %w", err)
		return
	}

	actual, _ := w.store.LoadOrStore(in.ConsumerConfigRow.Label, in.ConsumerConfigRow)

	out = storage.OutPersistKafkaConsumer{
		ConsumerConfigRow: actual.(storage.ConsumerConfigRow),
	}

	return
}

func (w *KafkaConsumerStore) GetKafkaConsumers(ctx context.Context) (out storage.KafkaConsumerRows, err error) {
	webhooks := make([]storage.ConsumerConfigRow, 0)

	w.store.Range(func(key, value any) bool {
		webhooks = append(webhooks, value.(storage.ConsumerConfigRow))
		return true
	})

	out = &WebhookRows{
		rows: webhooks,
	}
	return
}

func (w *KafkaConsumerStore) GetSinkTargetByLabel(ctx context.Context, label string) (storage.SinkTarget, error) {
	webhook, exist := w.store.Load(label)
	if !exist {
		return storage.SinkTarget{}, os.ErrNotExist
	}

	return webhook.(storage.SinkTarget), nil
}

type WebhookRows struct {
	lock    sync.Mutex
	rows    []storage.ConsumerConfigRow
	currRow *storage.ConsumerConfigRow
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

func (w *WebhookRows) ConsumerConfigRow() (storage.ConsumerConfigRow, error) {
	w.lock.Lock()
	defer w.lock.Unlock()
	if w.currRow == nil {
		return storage.ConsumerConfigRow{}, io.ErrUnexpectedEOF
	}

	return *w.currRow, nil
}
