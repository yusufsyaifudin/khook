package kafkaconsumermgr

import (
	"fmt"
	"github.com/yusufsyaifudin/khook/internal/pkg/kafkaclientmgr"
	"github.com/yusufsyaifudin/khook/storage"
	"github.com/yusufsyaifudin/khook/storage/inmem"
	"time"
)

func defaultKafkaOpt() *kafkaOpt {
	return &kafkaOpt{
		kafkaConsumerStore: inmem.NewKafkaConsumerStore(),
		updateEvery:        10 * time.Second,
	}
}

type KafkaOpt func(opt *kafkaOpt) error

func WithConnStore(store storage.KafkaConsumerStore) KafkaOpt {
	return func(opt *kafkaOpt) error {
		if store == nil {
			return fmt.Errorf("nil store")
		}

		opt.kafkaConsumerStore = store
		return nil
	}
}

func WithClientConnectionManager(manager kafkaclientmgr.Manager) KafkaOpt {
	return func(opt *kafkaOpt) error {
		if manager == nil {
			return fmt.Errorf("nil kafka client manager")
		}

		opt.kafkaClientManager = manager
		return nil
	}
}

func WithUpdateInterval(interval time.Duration) KafkaOpt {
	return func(opt *kafkaOpt) error {
		if interval.Seconds() <= 0 {
			return nil
		}

		opt.updateEvery = interval
		return nil
	}
}
