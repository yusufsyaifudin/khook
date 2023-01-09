package resourcesvc

import (
	"context"
	"github.com/yusufsyaifudin/khook/internal/pkg/kafkaclientmgr"
	"github.com/yusufsyaifudin/khook/storage"
)

type ResourceService interface {
	AddResource(ctx context.Context, in InAddResource) (out OutAddResource, err error)
	GetConsumers(ctx context.Context) (out OutGetWebhooks, err error)
	GetActiveKafkaConfigs(ctx context.Context) (out OutGetActiveKafkaConfigs)
	GetActiveConsumers(ctx context.Context) (out OutGetActiveConsumers, err error)
}

type KhookResource struct {
	storage.Type     `json:",inline" validate:"required"`
	storage.Metadata `json:",inline" validate:"required"`
	Spec             any `json:"spec,omitempty" validate:"required"`
}

// InAddResource is similar like storage.KafkaConnectionConfig but without ID
type InAddResource struct {
	Resource KhookResource `json:"resource"`
}

type OutAddResource struct {
	Resource KhookResource `json:"resource"`
}

type OutGetActiveKafkaConfigs struct {
	Total        int                       `json:"total"`
	KafkaConfigs []kafkaclientmgr.ConnInfo `json:"kafka_configs"`
}

type OutGetWebhooks struct {
	Consumers []storage.KafkaConsumerConfig `json:"consumers"`
}

type OutGetActiveConsumers struct {
	Consumers []storage.KafkaConsumerConfig `json:"consumers"`
}
