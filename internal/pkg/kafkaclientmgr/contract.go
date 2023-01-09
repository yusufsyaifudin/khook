package kafkaclientmgr

import (
	"context"
	"github.com/Shopify/sarama"
)

// Manager is an interface for connection manager
type Manager interface {
	GetAllConn(ctx context.Context) (conn []ConnInfo)
	GetConn(ctx context.Context, ns, name string) (sarama.Client, error)
	Close() error
}

type ConnInfo struct {
	Label   string   `json:"label,omitempty"`
	Brokers []string `json:"brokers,omitempty"`
	Topics  []string `json:"topics,omitempty"`
	Error   string   `json:"error,omitempty"`
}
