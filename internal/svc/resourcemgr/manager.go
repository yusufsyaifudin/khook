package resourcemgr

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/hashicorp/go-multierror"
	"github.com/yusufsyaifudin/khook/internal/pkg/kafkaclientmgr"
	"github.com/yusufsyaifudin/khook/storage"
	"io"
	"sync"
	"time"
)

type ConsumerManagerConfig struct {
	KafkaConnStore     storage.KafkaConnStore
	WebhookStore       storage.WebhookStore
	KafkaClientManager *kafkaclientmgr.KafkaClientManager
}

type ConsumerManager struct {
	Config ConsumerManagerConfig

	ticker          *time.Ticker
	mKafkaConsGroup sync.RWMutex

	// kafkaConsGroup contains KafkaClientManager ConsumerGroup connection with the Webhook label as key.
	// Webhook label must be unique.
	kafkaConsGroup map[string]sarama.ConsumerGroup

	// kafkaPausedConsGroup contains paused KafkaClientManager ConsumerGroup connection.
	kafkaPausedConsGroup map[string]sarama.ConsumerGroup
}

var _ Consumer = (*ConsumerManager)(nil)
var _ io.Closer = (*ConsumerManager)(nil)

func NewConsumerManager(cfg ConsumerManagerConfig) (*ConsumerManager, error) {
	mgr := &ConsumerManager{
		Config:               cfg,
		ticker:               time.NewTicker(3 * time.Second),
		mKafkaConsGroup:      sync.RWMutex{},
		kafkaConsGroup:       make(map[string]sarama.ConsumerGroup),
		kafkaPausedConsGroup: make(map[string]sarama.ConsumerGroup),
	}

	go mgr.manageConsumers()

	return mgr, nil
}

func (c *ConsumerManager) Close() error {
	c.mKafkaConsGroup.Lock()
	defer c.mKafkaConsGroup.Unlock()

	var errCum error
	for consGroupName, consGroup := range c.kafkaConsGroup {
		if consGroup == nil {
			continue
		}
		_err := consGroup.Close()
		if _err != nil {
			errCum = multierror.Append(errCum, fmt.Errorf("consumer group '%s': %w", consGroupName, _err))
		}
	}

	for consGroupName, consGroup := range c.kafkaPausedConsGroup {
		if consGroup == nil {
			continue
		}
		_err := consGroup.Close()
		if _err != nil {
			errCum = multierror.Append(errCum, fmt.Errorf("consumer group '%s': %w", consGroupName, _err))
		}
	}

	return errCum
}

func (c *ConsumerManager) AddKafkaConfig(ctx context.Context, in InAddKafkaConfig) (out OutAddKafkaConfig, err error) {
	outPersist, err := c.Config.KafkaConnStore.PersistKafkaConfig(ctx, storage.InputPersistKafkaConfig{
		KafkaConfig: storage.KafkaConfig{
			Label:   in.Label,
			Address: in.Address,
		},
	})

	if err != nil {
		return
	}

	err = c.Config.KafkaClientManager.AddConnection(ctx, outPersist.KafkaConfig)
	if err != nil {
		return
	}

	out = OutAddKafkaConfig{
		KafkaConfig: outPersist.KafkaConfig,
	}

	return
}

// GetActiveKafkaConfigs get all active kafka consumer.
func (c *ConsumerManager) GetActiveKafkaConfigs(ctx context.Context) (out OutGetActiveKafkaConfigs) {
	outKafkaConfig := c.Config.KafkaClientManager.GetAllConn(ctx)
	out = OutGetActiveKafkaConfigs{
		KafkaConfigs: outKafkaConfig,
	}

	return
}

// AddWebhook add webhook configuration, and later will be managed by manageConsumers to run actual consumer.
func (c *ConsumerManager) AddWebhook(ctx context.Context, in InputAddWebhook) (out OutAddWebhook, err error) {
	outAddWebhook, err := c.Config.WebhookStore.PersistWebhook(ctx, storage.InputPersistWebhook{
		Webhook: storage.Webhook{
			Label:         in.Webhook.Label,
			WebhookSource: in.Webhook.WebhookSource,
			WebhookSink:   in.Webhook.WebhookSink,
		},
	})
	if err != nil {
		return
	}

	out = OutAddWebhook{
		Webhook: outAddWebhook.Webhook,
	}

	return
}

// GetWebhooks get all registered webhook in the database.
func (c *ConsumerManager) GetWebhooks(ctx context.Context) (out OutGetWebhooks, err error) {
	outGetWebhook, err := c.Config.WebhookStore.GetWebhooks(ctx)
	if err != nil {
		return
	}

	webhooks := make([]storage.Webhook, 0)
	for outGetWebhook.Next() {
		webhook, _err := outGetWebhook.Webhook()
		if _err != nil {
			err = fmt.Errorf("iterating webhook row: %s", _err)
			return
		}
		webhooks = append(webhooks, webhook)
	}

	out = OutGetWebhooks{
		Webhooks: webhooks,
	}

	return
}

// GetActiveWebhooks get active webhook that running in this program.
func (c *ConsumerManager) GetActiveWebhooks(ctx context.Context) (out OutGetActiveWebhooks, err error) {
	c.mKafkaConsGroup.RLock()
	defer c.mKafkaConsGroup.RUnlock()

	activeWebhook := make([]storage.Webhook, 0)
	for webhookLabel := range c.kafkaConsGroup {
		webhook, errWebhook := c.Config.WebhookStore.GetWebhookByLabel(ctx, webhookLabel)
		if errWebhook != nil {
			continue
		}

		activeWebhook = append(activeWebhook, webhook)
	}

	out = OutGetActiveWebhooks{
		ActiveWebhooks: activeWebhook,
	}
	return
}

// PauseWebhook pause existing webhook
func (c *ConsumerManager) PauseWebhook(ctx context.Context, in InPauseWebhook) (out OutPauseWebhook, err error) {
	c.mKafkaConsGroup.Lock()
	defer c.mKafkaConsGroup.Unlock()

	label := in.Label
	consumerGroup, exist := c.kafkaConsGroup[label]
	if !exist {
		out = OutPauseWebhook{
			Paused: false,
		}
		return
	}

	consumerGroup.PauseAll() // synchronous operation, wait until pause done

	delete(c.kafkaConsGroup, label)
	c.kafkaPausedConsGroup[label] = consumerGroup

	out = OutPauseWebhook{
		Paused: true,
	}
	return
}

// ResumeWebhook resume paused webhook
func (c *ConsumerManager) ResumeWebhook(ctx context.Context, in InResumeWebhook) (out OutResumeWebhook, err error) {
	c.mKafkaConsGroup.Lock()
	defer c.mKafkaConsGroup.Unlock()

	label := in.Label
	consumerGroup, exist := c.kafkaPausedConsGroup[label]
	if !exist {
		out = OutResumeWebhook{
			Message: fmt.Sprintf("webhook %s is not found in paused consumer", label),
		}
		return
	}

	consumerGroup.ResumeAll() // synchronous operation, wait until pause done

	delete(c.kafkaPausedConsGroup, label)
	c.kafkaConsGroup[label] = consumerGroup

	out = OutResumeWebhook{
		Message: fmt.Sprintf("webhook %s is continue to receive stream", label),
	}
	return
}

// GetPausedWebhooks get paused webhook that running in this program.
func (c *ConsumerManager) GetPausedWebhooks(ctx context.Context) (out OutGetPausedWebhooks, err error) {
	c.mKafkaConsGroup.RLock()
	defer c.mKafkaConsGroup.RUnlock()

	pausedWebhook := make([]storage.Webhook, 0)
	for webhookLabel := range c.kafkaPausedConsGroup {
		webhook, errWebhook := c.Config.WebhookStore.GetWebhookByLabel(ctx, webhookLabel)
		if errWebhook != nil {
			continue
		}

		pausedWebhook = append(pausedWebhook, webhook)
	}

	out = OutGetPausedWebhooks{
		PausedWebhooks: pausedWebhook,
	}
	return
}
