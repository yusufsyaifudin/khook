package storage

type Kind string

const (
	KindKafkaConnection Kind = "KafkaBrokerConnection"
	KindKafkaConsumer   Kind = "KafkaConsumer"
)

type Type struct {
	ApiVersion string `json:"apiVersion"`
	Kind       Kind   `json:"kind"`
}

type Metadata struct {
	Name      string            `json:"name" validate:"required,max=65"`
	Namespace string            `json:"namespace" validate:"required,max=65"`
	Labels    map[string]string `json:"labels"`
}
