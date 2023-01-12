package types

type Kind string

const (
	KindKafkaBroker   Kind = "KafkaBroker"
	KindKafkaConsumer Kind = "KafkaConsumer"
)

type Type struct {
	ApiVersion string `json:"apiVersion,omitempty"`
	Kind       Kind   `json:"kind,omitempty"`
}

type Metadata struct {
	Name      string            `json:"name,omitempty" validate:"required,max=65"`
	Namespace string            `json:"namespace,omitempty" validate:"required,max=65"`
	Labels    map[string]string `json:"labels,omitempty"`
}
