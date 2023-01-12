package kafkaconsumermgr

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/yusufsyaifudin/khook/internal/pkg/kafkaclientmgr"
	"github.com/yusufsyaifudin/khook/internal/pkg/kafkaconsumermgr/manager"
	"github.com/yusufsyaifudin/khook/internal/pkg/sipper"
	"github.com/yusufsyaifudin/khook/pkg/types"
	"github.com/yusufsyaifudin/khook/storage"
	"log"
	"math/rand"
	"sync"
	"time"
)

type kafkaOpt struct {
	kafkaConsumerStore storage.KafkaConsumerStore
	kafkaClientManager kafkaclientmgr.Manager
	updateEvery        time.Duration
}

// KafkaConsumerManager is a manager to connect between Kafka topic to Sink
type KafkaConsumerManager struct {
	kafkaOpt              *kafkaOpt
	tickerRefreshConsumer *time.Ticker

	// state manager for sync from database
	once             sync.Once
	lock             sync.RWMutex
	updateInProgress bool

	// watch connection config changes
	watchConnConfig chan storage.OutWatchKafkaConsumer
	connManager     *manager.ConnManager
}

var _ Manager = (*KafkaConsumerManager)(nil)

func NewKafkaConsumerManager(opts ...KafkaOpt) (*KafkaConsumerManager, error) {
	defaultOpt := defaultKafkaOpt()
	for _, opt := range opts {
		err := opt(defaultOpt)
		if err != nil {
			return nil, err
		}
	}

	svc := &KafkaConsumerManager{
		kafkaOpt:              defaultOpt,
		tickerRefreshConsumer: time.NewTicker(defaultOpt.updateEvery + (time.Duration(rand.Int63n(150)) * time.Millisecond)),
		once:                  sync.Once{},
		lock:                  sync.RWMutex{},
		updateInProgress:      false,
		watchConnConfig:       make(chan storage.OutWatchKafkaConsumer),
		connManager:           manager.NewConnManager(),
	}

	go svc.kafkaOpt.kafkaConsumerStore.WatchKafkaConsumer(context.Background(), svc.watchConnConfig)
	go svc.init()
	go svc.connectKafkaToSink()

	return svc, nil
}

func (k *KafkaConsumerManager) init() {
	k.once.Do(func() {
		k.updateFromDb(time.Now())
	})
}

func (k *KafkaConsumerManager) connectKafkaToSink() {
	for {
		select {

		case changes := <-k.watchConnConfig:
			log.Printf("got consumer update: %+v\n", changes)
			if !k.connManager.IsUpdated(changes.Resource) {
				continue
			}
			k.addConsumer(changes.Resource)

		case t := <-k.tickerRefreshConsumer.C:
			log.Printf("get consumer list at %s\n", t.Format(time.RFC3339Nano))
			k.updateFromDb(t)
		}

	}
}

func (k *KafkaConsumerManager) updateFromDb(t time.Time) {
	k.lock.RLock()
	if k.updateInProgress {
		k.lock.RUnlock()
		return
	}
	k.lock.RUnlock()

	k.lock.Lock()
	k.updateInProgress = true
	k.lock.Unlock()

	kafkaConsumers, err := k.kafkaOpt.kafkaConsumerStore.GetKafkaConsumers(context.Background())
	if err != nil {
		log.Printf("get consumer list error %s at %s\n", err, t.Format(time.RFC3339Nano))
		return
	}

	for kafkaConsumers.Next() {
		consumerCfg, _err := kafkaConsumers.KafkaConsumerConfig()
		if _err != nil {
			log.Printf("getting sink target row error: %s\n", _err)
			continue
		}

		topicSource := consumerCfg.Spec.Selector.KafkaTopic
		if !k.connManager.IsUpdated(consumerCfg) {
			log.Printf("(not updated) already registered consumer %s from kafka %s\n", consumerCfg.Name, topicSource)
			continue
		}

		k.addConsumer(consumerCfg)
	}

	k.lock.Lock()
	k.updateInProgress = false
	k.lock.Unlock()
}

func (k *KafkaConsumerManager) addConsumer(cfg types.KafkaConsumerConfig) {
	resourceName := cfg.Metadata.Name
	namespace := cfg.Metadata.Namespace

	kafkaClientName := cfg.Spec.Selector.Name
	topicSource := cfg.Spec.Selector.KafkaTopic

	log.Printf("add %s to consume kafka topic %s\n", cfg.Name, topicSource)
	kafkaClient, err := k.kafkaOpt.kafkaClientManager.GetConn(context.Background(), namespace, kafkaClientName)
	if err != nil {
		log.Printf(
			"error kafka connection '%s' for consumer '%s' to consumer topic '%s': %s\n",
			kafkaClientName, resourceName, topicSource, err,
		)
		return
	}

	if kafkaClient.Closed() {
		// TODO: if current client is closed, delete all active webhook consumer related to this client.
		return
	}

	consumerGroup := fmt.Sprintf("consumer-%s-%s", namespace, resourceName)
	kafkaConsumerGroup, err := sarama.NewConsumerGroupFromClient(consumerGroup, kafkaClient)
	if err != nil {
		log.Printf(
			"setup consumer group webhhok '%s' kafka label '%s': %s\n",
			resourceName, kafkaClientName, err,
		)
		return
	}

	log.Println("[start] register webhook consumer", resourceName)

	// isConnUpOrErr will wait either nil error (come from Setup consumer handler)
	// or error from sarama.ConsumerGroupHandler.Consume inside function InitConsumer
	isConnUpOrErr := make(chan error, 1)
	processor, procErr := sipper.SelectProcessor(resourceName, cfg.Spec.SinkTarget, isConnUpOrErr)
	if procErr != nil {
		log.Printf("cannot select processor: %s\n", procErr)
		return
	}

	sipper.InitConsumer(context.Background(), isConnUpOrErr, &sipper.InputInitConsumer{
		ConsumerGroup: kafkaConsumerGroup,
		Topic:         topicSource,
		Processor:     processor,
	})

	// waiting for consumer to be up or return error
	connErr := <-isConnUpOrErr

	log.Println("[done] register consumer", resourceName)
	if connErr != nil {
		log.Printf("add consumer '%s' to kafka label '%s' failed: %s\n",
			resourceName,
			kafkaClientName,
			connErr,
		)

		return
	}

	log.Printf("add consumer '%s' to kafka label %s success\n",
		resourceName,
		kafkaClientName,
	)

	err = k.connManager.Add(&manager.ConsumerGroupInfo{
		Name:              resourceName,
		Namespace:         namespace,
		ConsumerGroupConn: kafkaConsumerGroup,
	}, cfg)
	if err != nil {
		log.Printf("failed adding to connection manager: %s\n", err)
	}
}

func (k *KafkaConsumerManager) GetActiveConsumers(ctx context.Context) []Consumer {
	activeConsumers := k.connManager.GetActive()
	consumers := make([]Consumer, 0)
	for _, consumerInfo := range activeConsumers {
		consumers = append(consumers, Consumer{
			Name:      consumerInfo.Name,
			Namespace: consumerInfo.Namespace,
		})
	}

	return consumers
}

func (k *KafkaConsumerManager) Close() error {
	return k.connManager.Close()
}
