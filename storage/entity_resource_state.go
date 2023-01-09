package storage

import "time"

type Status string

const (
	Start   Status = "start"
	Paused  Status = "paused"
	Stop    Status = "stop"
	Deleted Status = "deleted"
)

type ResourceState struct {
	Rev       int       `json:"rev,omitempty"`
	Status    Status    `json:"status,omitempty"`
	CreatedAt time.Time `json:"createdAt,omitempty"`
	UpdatedAt time.Time `json:"updatedAt,omitempty"`
}
