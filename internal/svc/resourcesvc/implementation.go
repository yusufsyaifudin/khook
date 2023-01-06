package resourcesvc

import (
	"context"
	"fmt"
	"github.com/yusufsyaifudin/khook/internal/pkg/kafkaclientmgr"
	"github.com/yusufsyaifudin/khook/internal/pkg/kafkaconsumermgr"
	"github.com/yusufsyaifudin/khook/storage"
)

type ConsumerManagerConfig struct {
	KafkaConnStore       storage.KafkaConnStore
	KafkaConsumerStore   storage.KafkaConsumerStore
	KafkaClientManager   kafkaclientmgr.Manager
	KafkaConsumerManager kafkaconsumermgr.Manager
}

type ConsumerManager struct {
	Config ConsumerManagerConfig
}

var _ ResourceService = (*ConsumerManager)(nil)

func NewResourceService(cfg ConsumerManagerConfig) (*ConsumerManager, error) {
	mgr := &ConsumerManager{
		Config: cfg,
	}

	return mgr, nil
}

func (c *ConsumerManager) AddKafkaConfig(ctx context.Context, in InAddKafkaConfig) (out OutAddKafkaConfig, err error) {
	outPersist, err := c.Config.KafkaConnStore.PersistKafkaConfig(ctx, storage.InputPersistKafkaConfig{
		KafkaConfig: storage.KafkaConfig{
			Label:   in.Label,
			Brokers: in.Address,
		},
	})

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
		Total:        len(outKafkaConfig),
		KafkaConfigs: outKafkaConfig,
	}

	return
}

// AddWebhook add webhook configuration, and later will be managed by manageConsumers to run actual consumer.
func (c *ConsumerManager) AddWebhook(ctx context.Context, in InputAddWebhook) (out OutAddWebhook, err error) {
	outAddWebhook, err := c.Config.KafkaConsumerStore.PersistKafkaConsumer(ctx, storage.InputPersistKafkaConsumer{
		SinkTarget: in.Webhook,
	})
	if err != nil {
		return
	}

	out = OutAddWebhook{
		Webhook: outAddWebhook.SinkTarget,
	}

	return
}

// GetWebhooks get all registered webhook in the database.
func (c *ConsumerManager) GetWebhooks(ctx context.Context) (out OutGetWebhooks, err error) {
	outGetWebhook, err := c.Config.KafkaConsumerStore.GetKafkaConsumers(ctx)
	if err != nil {
		return
	}

	webhooks := make([]storage.SinkTarget, 0)
	for outGetWebhook.Next() {
		webhook, _, _err := outGetWebhook.SinkTarget()
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

// GetActiveConsumers get active webhook that running in this program.
func (c *ConsumerManager) GetActiveConsumers(ctx context.Context) (out OutGetActiveConsumers, err error) {
	consumers := c.Config.KafkaConsumerManager.GetActiveConsumers(ctx)

	activeConsumers := make([]storage.SinkTarget, 0)
	for _, consumer := range consumers {
		sinkTarget, err := c.Config.KafkaConsumerStore.GetSinkTargetByLabel(ctx, consumer.Label)
		if err != nil {
			continue
		}

		activeConsumers = append(activeConsumers, sinkTarget)
	}

	out = OutGetActiveConsumers{
		ActiveConsumers: activeConsumers,
	}
	return
}

// PauseWebhook pause existing webhook
func (c *ConsumerManager) PauseWebhook(ctx context.Context, in InPauseWebhook) (out OutPauseWebhook, err error) {
	// TODO: update database with state pause

	out = OutPauseWebhook{
		Paused: true,
	}
	return
}

// ResumeWebhook resume paused webhook
func (c *ConsumerManager) ResumeWebhook(ctx context.Context, in InResumeWebhook) (out OutResumeWebhook, err error) {

	return
}

// GetPausedWebhooks get paused webhook that running in this program.
func (c *ConsumerManager) GetPausedWebhooks(ctx context.Context) (out OutGetPausedWebhooks, err error) {
	return OutGetPausedWebhooks{}, err
}
