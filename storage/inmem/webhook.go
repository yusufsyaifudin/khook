package inmem

import (
	"context"
	"os"
	"sync"

	"github.com/yusufsyaifudin/khook/storage"
)

type WebhookStore struct {
	store sync.Map
}

var _ storage.WebhookStore = (*WebhookStore)(nil)

func NewWebhookStore() *WebhookStore {
	return &WebhookStore{
		store: sync.Map{},
	}
}

func (w *WebhookStore) PersistWebhook(ctx context.Context, in storage.InputPersistWebhook) (out storage.OutInputPersistWebhook, err error) {
	actual, _ := w.store.LoadOrStore(in.Webhook.Label, in.Webhook)
	out = storage.OutInputPersistWebhook{
		Webhook: actual.(storage.Webhook),
	}

	return
}

func (w *WebhookStore) GetWebhooks(ctx context.Context, in storage.InputGetWebhooks) (out storage.WebhookRows, err error) {
	webhooks := make([]storage.Webhook, 0)

	w.store.Range(func(key, value any) bool {
		webhooks = append(webhooks, value.(storage.Webhook))
		return true
	})

	out = &WebhookRows{
		webhooks: webhooks,
	}
	return
}

func (w *WebhookStore) GetWebhookByLabel(ctx context.Context, label string) (storage.Webhook, error) {
	webhook, exist := w.store.Load(label)
	if !exist {
		return storage.Webhook{}, os.ErrNotExist
	}

	return webhook.(storage.Webhook), nil
}

type WebhookRows struct {
	lock     sync.Mutex
	webhooks []storage.Webhook
}

var _ storage.WebhookRows = (*WebhookRows)(nil)

func (w *WebhookRows) Next() bool {
	w.lock.Lock()
	defer w.lock.Unlock()

	if len(w.webhooks) > 0 {
		return true
	}

	return false
}

func (w *WebhookRows) Webhook() storage.Webhook {
	w.lock.Lock()
	defer w.lock.Unlock()

	var webhook storage.Webhook
	webhook, w.webhooks = w.webhooks[0], w.webhooks[1:]
	return webhook
}
