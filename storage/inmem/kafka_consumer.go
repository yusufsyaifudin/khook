package inmem

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

	actual, _ := w.store.LoadOrStore(in.SinkTarget.Label, in.SinkTarget)
	b, err := json.Marshal(actual)
	if err != nil {
		return
	}

	h := sha256.New()
	_, err = io.Copy(h, bytes.NewReader(b))
	if err != nil {
		err = fmt.Errorf("cannot copy to calculate sink target checksum: %w", err)
		return
	}

	checkSum := hex.EncodeToString(h.Sum(nil))

	out = storage.OutPersistKafkaConsumer{
		Checksum:   checkSum,
		SinkTarget: actual.(storage.SinkTarget),
	}

	return
}

func (w *KafkaConsumerStore) GetKafkaConsumers(ctx context.Context) (out storage.KafkaConsumerRows, err error) {
	webhooks := make([]storage.SinkTarget, 0)

	w.store.Range(func(key, value any) bool {
		webhooks = append(webhooks, value.(storage.SinkTarget))
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
	rows    []storage.SinkTarget
	currRow *storage.SinkTarget
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

func (w *WebhookRows) SinkTarget() (storage.SinkTarget, string, error) {
	w.lock.Lock()
	defer w.lock.Unlock()
	if w.currRow == nil {
		return storage.SinkTarget{}, "", io.ErrUnexpectedEOF
	}

	b, err := json.Marshal(w.currRow)
	if err != nil {
		return storage.SinkTarget{}, "", err
	}

	h := sha256.New()
	_, err = io.Copy(h, bytes.NewReader(b))
	if err != nil {
		err = fmt.Errorf("cannot copy to calculate sink target checksum: %w", err)
		return storage.SinkTarget{}, "", err
	}

	checkSum := hex.EncodeToString(h.Sum(nil))
	return *w.currRow, checkSum, nil
}
