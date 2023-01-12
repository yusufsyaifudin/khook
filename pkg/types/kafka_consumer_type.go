package types

import (
	"fmt"
)

// KafkaConsumerConfig .
type KafkaConsumerConfig struct {
	Type     `json:",inline" validate:"required"`
	Metadata `json:",inline" validate:"required"`
	Spec     ConsumerConfigSpec `json:"spec,omitempty" validate:"required"`
}

func NewKafkaConsumerConfig() KafkaConsumerConfig {
	cfg := KafkaConsumerConfig{
		Type: Type{
			ApiVersion: "khook/v1",
			Kind:       KindKafkaConsumer,
		},
		Metadata: Metadata{
			Namespace: "default",
		},
	}
	return cfg
}

type ConsumerConfigKafkaClientSelector struct {
	Name string `json:"name" validate:"required"`

	// KafkaTopic is limited to one topic per webhook.
	// This add visibility to the user, where list of SinkTarget only get message from one Kafka topic,
	// unless they register the same URL on WebhookSink from the WebhookSource.KafkaTopic
	KafkaTopic string `json:"kafka_topic,omitempty" validate:"required"`
}

type ConsumerConfigSpec struct {
	Selector   ConsumerConfigKafkaClientSelector `json:"selector" validate:"required"`
	SinkTarget SinkTarget                        `json:"sinkTarget" validate:"required"`
}

// SinkTarget contains contract on how sink is processed.
// Detail implementation would be in internal/pkg/kafkaconsumermgr
// The minimal and easiest to understand of SinkTarget is CloudEvents,
// where message from KafkaBrokerConfig (in specific topic) is sent via HTTP
// as specified in the CloudEvents.URL as CloudEvents message.
type SinkTarget struct {
	Type        string       `json:"type,omitempty" validate:"required"`
	CloudEvents *CloudEvents `json:"cloudevents,omitempty" validate:"required_if=Type cloudevents"`
}

// Validate implements validate.CustomValidate not using pointer
func (m SinkTarget) Validate() error {
	switch m.Type {
	case "cloudevents":
		return nil

	default:
		return fmt.Errorf("unknown type '%s' on sink target as consumer processor", m.Type)
	}
}

type CloudEvents struct {
	URL string `json:"url,omitempty" validate:"required"`

	// Type is the ce-type of event data
	Type string `json:"type,omitempty" validate:"required"`
}
