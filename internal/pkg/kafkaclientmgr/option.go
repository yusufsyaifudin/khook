package kafkaclientmgr

import (
	"fmt"
	"github.com/yusufsyaifudin/khook/storage"
	"github.com/yusufsyaifudin/khook/storage/inmem"
	"time"
)

func defaultKafkaOpt() *configOption {
	return &configOption{
		kafkaConnStore:  inmem.NewKafkaConnStore(),
		updateConnEvery: 10 * time.Second,
	}
}

type KafkaOpt func(opt *configOption) error

func WithConnStore(store storage.KafkaConnStore) KafkaOpt {
	return func(opt *configOption) error {
		if store == nil {
			return fmt.Errorf("nil KafkaConnStore")
		}

		opt.kafkaConnStore = store
		return nil
	}
}
func WithUpdateConnInterval(interval time.Duration) KafkaOpt {
	return func(opt *configOption) error {
		if interval.Seconds() <= 0 {
			return nil
		}

		opt.updateConnEvery = interval
		return nil
	}
}
