package storage

type ChangeType string

const (
	Put    ChangeType = "Put"
	Delete ChangeType = "Delete"
)
