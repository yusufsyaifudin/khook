package resourcesvc

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/yusufsyaifudin/khook/internal/pkg/kafkaclientmgr"
	"github.com/yusufsyaifudin/khook/internal/pkg/kafkaconsumermgr"
	"github.com/yusufsyaifudin/khook/pkg/types"
	"github.com/yusufsyaifudin/khook/pkg/validator"
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

func (c *ConsumerManager) AddResource(ctx context.Context, in InAddResource) (out OutAddResource, err error) {
	err = validator.Validate(in)
	if err != nil {
		err = fmt.Errorf("cannot validate resource: %w", err)
		return
	}

	specBytes, err := json.Marshal(in.Resource.Spec)
	if err != nil {
		err = fmt.Errorf("cannot marshal spec to json: %w", err)
		return OutAddResource{}, err
	}

	var outResource KhookResource
	kind := in.Resource.Kind
	switch kind {
	case types.KindKafkaBroker:
		cfg := types.NewKafkaConnectionConfig()
		cfg.Metadata.Name = in.Resource.Name

		if in.Resource.Namespace != "" {
			cfg.Metadata.Namespace = in.Resource.Namespace
		}

		var spec types.KafkaBrokerSpec
		err = json.Unmarshal(specBytes, &spec)
		if err != nil {
			err = fmt.Errorf("invalid kafka broker config spec: %w", err)
			return
		}

		err = validator.Validate(spec)
		if err != nil {
			err = fmt.Errorf("cannot validate resource type %s: %w", kind, err)
			return
		}

		cfg.Spec = spec

		var outPersist storage.OutPersistKafkaConnConfig
		outPersist, err = c.Config.KafkaConnStore.PersistKafkaConnConfig(ctx, storage.InPersistKafkaConnConfig{
			Resource: cfg,
		})
		if err != nil {
			err = fmt.Errorf("cannot store kafka broker config: %w", err)
			return
		}

		outResource = KhookResource{
			Type:     outPersist.Resource.Type,
			Metadata: outPersist.Resource.Metadata,
			Spec:     outPersist.Resource.Spec,
		}

	case types.KindKafkaConsumer:
		cfg := types.NewKafkaConsumerConfig()
		cfg.Metadata.Name = in.Resource.Name

		if in.Resource.Namespace != "" {
			cfg.Metadata.Namespace = in.Resource.Namespace
		}

		var spec types.ConsumerConfigSpec
		err = json.Unmarshal(specBytes, &spec)
		if err != nil {
			err = fmt.Errorf("invalid kafka broker config spec: %w", err)
			return
		}

		err = validator.Validate(spec)
		if err != nil {
			err = fmt.Errorf("cannot validate resource type %s: %w", kind, err)
			return
		}

		cfg.Spec = spec

		var outPersist storage.OutPersistKafkaConsumer
		outPersist, err = c.Config.KafkaConsumerStore.PersistKafkaConsumer(ctx, storage.InputPersistKafkaConsumer{
			Resource: cfg,
		})
		if err != nil {
			err = fmt.Errorf("cannot store kafka consumer config: %w", err)
			return
		}

		outResource = KhookResource{
			Type:     outPersist.Resource.Type,
			Metadata: outPersist.Resource.Metadata,
			Spec:     outPersist.Resource.Spec,
		}
	default:
		err = fmt.Errorf("cannot store resource kind=%s", kind)
		return
	}

	out = OutAddResource{
		Resource: outResource,
	}

	return
}

func (c *ConsumerManager) GetBrokers(ctx context.Context) (out OutGetKafkaConfigs, err error) {
	rows, err := c.Config.KafkaConnStore.GetKafkaConnConfigs(ctx)
	if err != nil {
		err = fmt.Errorf("cannot get kafka configs: %w", err)
		return
	}

	configs := make([]types.KafkaBrokerConfig, 0)
	for rows.Next() {
		row, err := rows.KafkaBrokerConfig()
		if err != nil {
			continue
		}

		configs = append(configs, row)
	}

	out = OutGetKafkaConfigs{
		Total:   len(configs),
		Configs: configs,
	}
	return
}

func (c *ConsumerManager) GetConsumers(ctx context.Context) (out OutGetWebhooks, err error) {
	rows, err := c.Config.KafkaConsumerStore.GetKafkaConsumers(ctx)
	if err != nil {
		err = fmt.Errorf("cannot get kafka consumers: %w", err)
		return
	}

	consumers := make([]types.KafkaConsumerConfig, 0)
	for rows.Next() {
		row, err := rows.KafkaConsumerConfig()
		if err != nil {
			continue
		}

		consumers = append(consumers, row)
	}

	out = OutGetWebhooks{
		Total:     len(consumers),
		Consumers: consumers,
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

// GetActiveConsumers get active webhook that running in this program.
func (c *ConsumerManager) GetActiveConsumers(ctx context.Context) (out OutGetActiveConsumers) {
	consumers := c.Config.KafkaConsumerManager.GetActiveConsumers(ctx)

	problemConsumer := make([]ProblematicConsumer, 0)
	activeConsumers := make([]types.KafkaConsumerConfig, 0)
	for _, consumer := range consumers {
		sinkTarget, err := c.Config.KafkaConsumerStore.GetKafkaConsumer(ctx, storage.InGetKafkaConsumer{
			Namespace: consumer.Namespace,
			Name:      consumer.Name,
		})
		if err != nil {
			problemConsumer = append(problemConsumer, ProblematicConsumer{
				Problem:   err.Error(),
				Namespace: consumer.Namespace,
				Name:      consumer.Name,
			})
			continue
		}

		activeConsumers = append(activeConsumers, sinkTarget.Resource)
	}

	out = OutGetActiveConsumers{
		TotalActive:          len(activeConsumers),
		ConsumersActive:      activeConsumers,
		TotalProblematic:     len(problemConsumer),
		ConsumersProblematic: problemConsumer,
	}
	return
}
