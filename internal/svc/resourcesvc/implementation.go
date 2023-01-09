package resourcesvc

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/yusufsyaifudin/khook/internal/pkg/kafkaclientmgr"
	"github.com/yusufsyaifudin/khook/internal/pkg/kafkaconsumermgr"
	"github.com/yusufsyaifudin/khook/storage"
	"time"
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

	specBytes, err := json.Marshal(in.Resource.Spec)
	if err != nil {
		err = fmt.Errorf("cannot marshal spec to json: %w", err)
		return OutAddResource{}, err
	}

	var outResource KhookResource
	switch in.Resource.Kind {
	case storage.KindKafkaConnection:
		cfg := storage.NewKafkaConnectionConfig()
		cfg.Metadata.Name = in.Resource.Name

		if in.Resource.Namespace != "" {
			cfg.Metadata.Namespace = in.Resource.Namespace
		}

		var spec storage.KafkaConfigSpec
		err = json.Unmarshal(specBytes, &spec)
		if err != nil {
			err = fmt.Errorf("invalid kafka connnection config spec: %w", err)
			return
		}

		cfg.Spec = spec

		// get existing row to increment the revision counter
		var existingKafkaCfg storage.OutGetKafkaConnConfig
		existingKafkaCfg, err = c.Config.KafkaConnStore.GetKafkaConnConfig(ctx, storage.InGetKafkaConnConfig{
			Namespace: cfg.Namespace,
			Name:      cfg.Name,
		})
		if err != nil && errors.Is(err, sql.ErrNoRows) {
			err = fmt.Errorf("cannot get kafka connection config: %w", err)
			return
		}

		state := existingKafkaCfg.ResourceState
		state.Rev += 1
		state.UpdatedAt = time.Now()

		var outPersist storage.OutPersistKafkaConnConfig
		outPersist, err = c.Config.KafkaConnStore.PersistKafkaConnConfig(ctx, storage.InPersistKafkaConnConfig{
			KafkaConfig:   cfg,
			ResourceState: state,
		})
		if err != nil {
			err = fmt.Errorf("cannot store kafka connection config: %w", err)
			return
		}

		outResource = KhookResource{
			Type:     outPersist.KafkaConfig.Type,
			Metadata: outPersist.KafkaConfig.Metadata,
			Spec:     outPersist.KafkaConfig.Spec,
		}

	case storage.KindKafkaConsumer:
		cfg := storage.NewKafkaConsumerConfig()
		cfg.Metadata.Name = in.Resource.Name

		if in.Resource.Namespace != "" {
			cfg.Metadata.Namespace = in.Resource.Namespace
		}

		var spec storage.ConsumerConfigSpec
		err = json.Unmarshal(specBytes, &spec)
		if err != nil {
			err = fmt.Errorf("invalid kafka connnection config spec: %w", err)
			return
		}

		cfg.Spec = spec

		// get existing row to increment the revision counter
		var existingKafkaConsumer storage.OutGetKafkaConsumer
		existingKafkaConsumer, err = c.Config.KafkaConsumerStore.GetKafkaConsumer(ctx, storage.InGetKafkaConsumer{
			Namespace: cfg.Namespace,
			Name:      cfg.Name,
		})
		if errors.Is(err, sql.ErrNoRows) {
			err = nil
		}

		if err != nil {
			err = fmt.Errorf("cannot get kafka consumer config: %w", err)
			return
		}

		state := existingKafkaConsumer.ResourceState
		state.Rev += 1
		state.UpdatedAt = time.Now()

		var outPersist storage.OutPersistKafkaConsumer
		outPersist, err = c.Config.KafkaConsumerStore.PersistKafkaConsumer(ctx, storage.InputPersistKafkaConsumer{
			KafkaConsumerConfig: cfg,
			ResourceState:       state,
		})
		if err != nil {
			err = fmt.Errorf("cannot store kafka consumer config: %w", err)
			return
		}

		outResource = KhookResource{
			Type:     outPersist.KafkaConsumerConfig.Type,
			Metadata: outPersist.KafkaConsumerConfig.Metadata,
			Spec:     outPersist.KafkaConsumerConfig.Spec,
		}
	default:
		err = fmt.Errorf("cannot store resource kind=%s", in.Resource.Kind)
		return
	}

	out = OutAddResource{
		Resource: outResource,
	}

	return
}

func (c *ConsumerManager) GetConsumers(ctx context.Context) (out OutGetWebhooks, err error) {
	rows, err := c.Config.KafkaConsumerStore.GetKafkaConsumers(ctx)
	if err != nil {
		err = fmt.Errorf("cannot get kafka consumers: %w", err)
		return
	}

	consumers := make([]storage.KafkaConsumerConfig, 0)
	for rows.Next() {
		row, _, err := rows.KafkaConsumerConfig()
		if err != nil {
			continue
		}

		consumers = append(consumers, row)
	}

	out = OutGetWebhooks{
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
func (c *ConsumerManager) GetActiveConsumers(ctx context.Context) (out OutGetActiveConsumers, err error) {
	consumers := c.Config.KafkaConsumerManager.GetActiveConsumers(ctx)

	activeConsumers := make([]storage.KafkaConsumerConfig, 0)
	for _, consumer := range consumers {
		sinkTarget, err := c.Config.KafkaConsumerStore.GetKafkaConsumer(ctx, storage.InGetKafkaConsumer{
			Namespace: consumer.Namespace,
			Name:      consumer.Label,
		})
		if err != nil {
			continue
		}

		activeConsumers = append(activeConsumers, sinkTarget.KafkaConsumerConfig)
	}

	out = OutGetActiveConsumers{
		Consumers: activeConsumers,
	}
	return
}
