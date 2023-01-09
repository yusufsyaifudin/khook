package resourcesvc

import (
	"context"
	"github.com/yusufsyaifudin/khook/internal/pkg/kafkaclientmgr"
	"github.com/yusufsyaifudin/khook/storage"
)

type ResourceService interface {
	AddKafkaConfig(ctx context.Context, in InAddKafkaConfig) (out OutAddKafkaConfig, err error)
	GetActiveKafkaConfigs(ctx context.Context) (out OutGetActiveKafkaConfigs)
	AddWebhook(ctx context.Context, in InputAddWebhook) (out OutAddWebhook, err error)
	GetWebhooks(ctx context.Context) (out OutGetWebhooks, err error)
	GetActiveConsumers(ctx context.Context) (out OutGetActiveConsumers, err error)
	PauseWebhook(ctx context.Context, in InPauseWebhook) (out OutPauseWebhook, err error)
	ResumeWebhook(ctx context.Context, in InResumeWebhook) (out OutResumeWebhook, err error)
	GetPausedWebhooks(ctx context.Context) (out OutGetPausedWebhooks, err error)
}

// InAddKafkaConfig is similar like storage.KafkaConnection but without ID
type InAddKafkaConfig struct {
	Label   string   `json:"label"`
	Address []string `json:"address,omitempty"`
}

type OutAddKafkaConfig struct {
	KafkaConfig storage.KafkaConnection `json:"kafka_config"`
}

type OutGetActiveKafkaConfigs struct {
	Total        int                       `json:"total"`
	KafkaConfigs []kafkaclientmgr.ConnInfo `json:"kafka_configs"`
}

type InputAddWebhook struct {
	Webhook storage.ConsumerConfigRow `json:"webhook"`
}

type OutAddWebhook struct {
	Webhook storage.SinkTarget `json:"webhook"`
}

type OutGetWebhooks struct {
	Webhooks []storage.SinkTarget `json:"webhooks"`
}

type OutGetActiveConsumers struct {
	ActiveConsumers []storage.SinkTarget `json:"active_webhooks"`
}

type InPauseWebhook struct {
	Label string `json:"label"`
}

type OutPauseWebhook struct {
	Paused bool `json:"paused"`
}

type InResumeWebhook struct {
	Label string `json:"label"`
}

type OutResumeWebhook struct {
	Message string `json:"message"`
}

type OutGetPausedWebhooks struct {
	PausedWebhooks []storage.SinkTarget `json:"paused_webhooks"`
}
