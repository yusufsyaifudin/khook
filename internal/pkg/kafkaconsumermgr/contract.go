package kafkaconsumermgr

import (
	"context"
)

type Manager interface {
	Close() error
	GetActiveConsumers(ctx context.Context) []Consumer
}

type Consumer struct {
	Label string
}
