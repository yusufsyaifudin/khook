package resourcemgr

import (
	"context"
	"github.com/yusufsyaifudin/khook/internal/pkg/kafkamgr"
	"github.com/yusufsyaifudin/khook/storage"
)

type Consumer interface {
	AddKafkaConfig(ctx context.Context, in InAddKafkaConfig) (out OutAddKafkaConfig, err error)
	GetActiveKafkaConfigs(ctx context.Context) (out OutGetActiveKafkaConfigs)
	AddWebhook(ctx context.Context, in InputAddWebhook) (out OutAddWebhook, err error)
	GetWebhooks(ctx context.Context) (out OutGetWebhooks, err error)
	GetActiveWebhooks(ctx context.Context) (out OutGetActiveWebhooks, err error)
	PauseWebhook(ctx context.Context, in InPauseWebhook) (out OutPauseWebhook, err error)
	GetPausedWebhooks(ctx context.Context) (out OutGetPausedWebhooks, err error)
}

// InAddKafkaConfig is similar like storage.KafkaConfig but without ID
type InAddKafkaConfig struct {
	Label   string   `json:"label"`
	Address []string `json:"address,omitempty"`
}

type OutAddKafkaConfig struct {
	KafkaConfig storage.KafkaConfig `json:"kafka_config"`
}

type OutGetActiveKafkaConfigs struct {
	KafkaConfigs []kafkamgr.ConnInfo `json:"kafka_configs"`
}

type InputAddWebhook struct {
	Webhook storage.Webhook `json:"webhook"`
}

type OutAddWebhook struct {
	Webhook storage.Webhook `json:"webhook"`
}

type OutGetWebhooks struct {
	Webhooks []storage.Webhook `json:"webhooks"`
}

type OutGetActiveWebhooks struct {
	ActiveWebhooks []storage.Webhook `json:"active_webhooks"`
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
	PausedWebhooks []storage.Webhook `json:"paused_webhooks"`
}
