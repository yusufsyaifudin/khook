package storage

type Type struct {
	ApiVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
}

type Metadata struct {
	Name      string            `json:"name" validate:"required,max=65"`
	Namespace string            `json:"namespace" validate:"required,max=65"`
	Labels    map[string]string `json:"labels"`
}
