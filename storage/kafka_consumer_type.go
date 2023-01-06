package storage

import (
	"encoding/json"
	"fmt"
)

// SinkTarget contains contract on how sink is processed.
// Detail implementation would be in internal/pkg/kafkaconsumermgr
// The minimal and easiest to understand of SinkTarget is CloudEvents,
// where message from KafkaConfig (in specific topic) is sent via HTTP
// as specified in the CloudEvents.URL as CloudEvents message.
type SinkTarget struct {
	Label string `json:"label,omitempty" validate:"required"`
	Type  string `json:"type,omitempty" validate:"required"`

	// KafkaConfigLabel is the KafkaConfig.Label as the stream source.
	KafkaConfigLabel string `json:"kafka_config_label,omitempty" validate:"required"`

	// KafkaTopic is limited to one topic per webhook.
	// This add visibility to the user, where list of SinkTarget only get message from one Kafka topic,
	// unless they register the same URL on WebhookSink from the WebhookSource.KafkaTopic
	KafkaTopic string `json:"kafka_topic,omitempty" validate:"required"`

	// KafkaTopicDLQ is where the stream is DLQ-ed if it cannot be sinked to the WebhookSink.
	// If empty, then it will generate the random Kafka topic.
	// Please ensure that KafkaTopicDLQ is not the same as KafkaTopic, otherwise you will get an infinite-loop.
	// Also, make sure that KafkaTopicDLQ is only used by one stream, or the data may mix with another stream.
	KafkaTopicDLQ string `json:"kafka_topic_dlq,omitempty" validate:"required"`

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

// Scan will read the data bytes from database and parse it as SinkTarget
func (m *SinkTarget) Scan(src interface{}) error {
	if m == nil {
		return fmt.Errorf("error scan sink target on nil struct")
	}

	switch v := src.(type) {
	case []byte:
		return json.Unmarshal(v, m)
	case string:
		return json.Unmarshal([]byte(fmt.Sprintf("%s", v)), m)
	}

	return fmt.Errorf("unknown type %T to format as sink target", src)
}

type CloudEvents struct {
	URL string `json:"url,omitempty" validate:"required"`

	// Type is the ce-type of event data
	Type string `json:"type,omitempty" validate:"required"`
}
