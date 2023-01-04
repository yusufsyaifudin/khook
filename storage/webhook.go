package storage

import "context"

type WebhookStore interface {
	PersistWebhook(ctx context.Context, in InputPersistWebhook) (out OutInputPersistWebhook, err error)
	GetWebhooks(ctx context.Context, in InputGetWebhooks) (out WebhookRows, err error)
	GetWebhookByLabel(ctx context.Context, label string) (out Webhook, err error)
}

type WebhookSource struct {
	// KafkaConfigLabel is the KafkaConfig.Label as the stream source.
	KafkaConfigLabel string `json:"kafka_config_label,omitempty"`

	// KafkaTopic is limited to one topic per webhook.
	// This add visibility to the user, where list of Webhook only get message from one Kafka topic,
	// unless they register the same URL on WebhookSink from the WebhookSource.KafkaTopic
	KafkaTopic string `json:"kafka_topic,omitempty"`

	// KafkaTopicDLQ is where the stream is DLQ-ed if it cannot be sinked to the WebhookSink.
	// If empty, then it will generate the random Kafka topic.
	// Please ensure that KafkaTopicDLQ is not the same as KafkaTopic, otherwise you will get an infinite-loop.
	// Also, make sure that KafkaTopicDLQ is only used by one stream, or the data may mix with another stream.
	KafkaTopicDLQ string `json:"kafka_topic_dlq,omitempty"`
}

type WebhookSink struct {
	URL string `json:"url,omitempty" validate:"required"`

	// CEType The type of event data
	CEType string `json:"ce_type,omitempty" validate:"required"`
}

type Webhook struct {
	Label         string         `json:"label,omitempty" validate:"required"`
	WebhookSource *WebhookSource `json:"webhook_source,omitempty" validate:"required"`
	WebhookSink   *WebhookSink   `json:"webhook_sink,omitempty" validate:"required"`
}

type InputPersistWebhook struct {
	Webhook Webhook `validate:"required"`
}

type OutInputPersistWebhook struct {
	Webhook Webhook
}

type InputGetWebhooks struct{}

type WebhookRows interface {
	Next() bool
	Webhook() Webhook
}
