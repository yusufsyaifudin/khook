package types

type KafkaBrokerSpec struct {
	Brokers []string `json:"brokers,omitempty" validate:"required"`
}

type KafkaBrokerConfig struct {
	Type     `json:",inline" validate:"required"`
	Metadata `json:",inline" validate:"required"`

	Spec KafkaBrokerSpec `json:"spec" validate:"required"`
}

func NewKafkaConnectionConfig() KafkaBrokerConfig {
	conn := KafkaBrokerConfig{
		Type: Type{
			ApiVersion: "khook/v1",
			Kind:       KindKafkaBroker,
		},
		Metadata: Metadata{
			Namespace: "default",
		},
		Spec: KafkaBrokerSpec{},
	}
	return conn
}
