package resourcesvc

import (
	"context"
	"github.com/yusufsyaifudin/khook/internal/pkg/kafkaclientmgr"
	"github.com/yusufsyaifudin/khook/pkg/types"
)

type ResourceService interface {
	AddResource(ctx context.Context, in InAddResource) (out OutAddResource, err error)
	GetBrokers(ctx context.Context) (out OutGetKafkaConfigs, err error)
	GetConsumers(ctx context.Context) (out OutGetWebhooks, err error)

	GetActiveKafkaConfigs(ctx context.Context) (out OutGetActiveKafkaConfigs)
	GetActiveConsumers(ctx context.Context) (out OutGetActiveConsumers)
}

type KhookResource struct {
	types.Type     `json:",inline" validate:"required"`
	types.Metadata `json:",inline" validate:"required"`
	Spec           any `json:"spec,omitempty" validate:"required"`
}

// InAddResource is similar like storage.KafkaBrokerConfig but without ID
type InAddResource struct {
	Resource KhookResource `json:"resource" validate:"required"`
}

type OutAddResource struct {
	Resource KhookResource `json:"resource"`
}

type OutGetKafkaConfigs struct {
	Total   int                       `json:"total"`
	Configs []types.KafkaBrokerConfig `json:"consumers"`
}

type OutGetWebhooks struct {
	Total     int                         `json:"total"`
	Consumers []types.KafkaConsumerConfig `json:"consumers"`
}

type OutGetActiveKafkaConfigs struct {
	Total        int                       `json:"total"`
	KafkaConfigs []kafkaclientmgr.ConnInfo `json:"kafkaConfigs"`
}

type ProblematicConsumer struct {
	Problem   string `json:"problem,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name,omitempty"`
}

type OutGetActiveConsumers struct {
	TotalActive     int                         `json:"totalActive"`
	ConsumersActive []types.KafkaConsumerConfig `json:"consumersActive"`

	TotalProblematic     int                   `json:"totalProblematic,omitempty"`
	ConsumersProblematic []ProblematicConsumer `json:"consumersProblematic,omitempty"`
}
