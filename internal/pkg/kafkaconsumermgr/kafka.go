package kafkaconsumermgr

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/hashicorp/go-multierror"
	"github.com/yusufsyaifudin/khook/internal/pkg/kafkaclientmgr"
	"github.com/yusufsyaifudin/khook/internal/pkg/sipper"
	"github.com/yusufsyaifudin/khook/storage"
	"log"
	"math/rand"
	"sort"
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

	mKafkaConsGroup sync.RWMutex

	// kafkaConsGroup contains KafkaClientManager ConsumerGroup connection with the SinkTarget label as key.
	// SinkTarget label must be unique.
	kafkaConsGroup map[string]sarama.ConsumerGroup

	// kafkaPausedConsGroup contains paused KafkaClientManager ConsumerGroup connection.
	kafkaPausedConsGroup map[string]sarama.ConsumerGroup
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
		mKafkaConsGroup:       sync.RWMutex{},
		kafkaConsGroup:        make(map[string]sarama.ConsumerGroup),
		kafkaPausedConsGroup:  make(map[string]sarama.ConsumerGroup),
	}

	go svc.connectKafkaToSink()

	return svc, nil
}

func (k *KafkaConsumerManager) connectKafkaToSink() {
	for {
		select {

		// Since ticker may call in instant mode, the KafkaConsumerStore should be locally cached!
		case t := <-k.tickerRefreshConsumer.C:
			log.Printf("get consumer list at %s\n", t.Format(time.RFC3339Nano))
			kafkaConsumers, err := k.kafkaOpt.kafkaConsumerStore.GetKafkaConsumers(context.Background())
			if err != nil {
				log.Printf("get consumer list error %s at %s\n", err, t.Format(time.RFC3339Nano))
				continue
			}

			for kafkaConsumers.Next() {
				sinkTarget, _, _err := kafkaConsumers.SinkTarget()
				if _err != nil {
					log.Printf("getting sink target row error: %s\n", _err)
					continue
				}

				sourceKafkaConnLabel := sinkTarget.KafkaConfigLabel
				targetSinkLabel := sinkTarget.Label
				topicSource := sinkTarget.KafkaTopic

				k.mKafkaConsGroup.RLock()
				_, consumerGroupExist := k.kafkaConsGroup[targetSinkLabel]
				if consumerGroupExist {
					log.Printf("already registered sink %s from kafka %s\n", targetSinkLabel, topicSource)

					k.mKafkaConsGroup.RUnlock()
					continue
				}

				_, consumerGroupPausedExist := k.kafkaPausedConsGroup[targetSinkLabel]
				if consumerGroupPausedExist {
					log.Printf("paused consumer %s from kafka %s already registered\n", targetSinkLabel, topicSource)

					k.mKafkaConsGroup.RUnlock()
					continue
				}
				k.mKafkaConsGroup.RUnlock()

				log.Printf("add %s to consume kafka topic %s\n", targetSinkLabel, topicSource)
				kafkaClient, err := k.kafkaOpt.kafkaClientManager.GetConn(context.Background(), sourceKafkaConnLabel)
				if err != nil {
					log.Printf(
						"error kafka connection '%s' for consumer '%s' to consumer topic '%s': %s\n",
						sourceKafkaConnLabel, targetSinkLabel, topicSource, err,
					)
					continue
				}

				if kafkaClient.Closed() {
					// TODO: if current client is closed, delete all active webhook consumer related to this client.
					continue
				}

				consumerGroup := fmt.Sprintf("consumer-%s", targetSinkLabel)
				kafkaConsumerGroup, err := sarama.NewConsumerGroupFromClient(consumerGroup, kafkaClient)
				if err != nil {
					log.Printf(
						"setup consumer group webhhok '%s' kafka label '%s': %s\n",
						targetSinkLabel, sourceKafkaConnLabel, err,
					)
					continue
				}

				log.Println("[start] register webhook consumer", targetSinkLabel)

				// isConnUpOrErr will wait either nil error (come from Setup consumer handler)
				// or error from sarama.ConsumerGroupHandler.Consume inside function InitConsumer
				isConnUpOrErr := make(chan error, 1)
				processor, procErr := sipper.SelectProcessor(sinkTarget, isConnUpOrErr)
				if procErr != nil {
					log.Printf("cannot select processor: %s\n", procErr)
					continue
				}

				sipper.InitConsumer(context.Background(), isConnUpOrErr, &sipper.InputInitConsumer{
					ConsumerGroup: kafkaConsumerGroup,
					Topic:         topicSource,
					Processor:     processor,
				})

				// waiting for consumer to be up or return error
				connErr := <-isConnUpOrErr

				log.Println("[done] register consumer", targetSinkLabel)
				if connErr == nil {
					log.Printf("add consumer '%s' to kafka label %s success\n",
						targetSinkLabel,
						sourceKafkaConnLabel,
					)

					k.mKafkaConsGroup.Lock()
					k.kafkaConsGroup[targetSinkLabel] = kafkaConsumerGroup
					k.mKafkaConsGroup.Unlock()
					continue
				}

				log.Printf("add consumer '%s' to kafka label '%s' failed: %s\n",
					targetSinkLabel,
					sourceKafkaConnLabel,
					connErr,
				)

				continue
			}
		}
	}
}

func (k *KafkaConsumerManager) GetActiveConsumers(ctx context.Context) []Consumer {
	k.mKafkaConsGroup.RLock()
	defer k.mKafkaConsGroup.RUnlock()

	consumers := make([]Consumer, 0)
	for consumerLabel := range k.kafkaConsGroup {
		consumers = append(consumers, Consumer{
			Label: consumerLabel,
		})
	}

	sort.Slice(consumers, func(i, j int) bool {
		return consumers[i].Label < consumers[j].Label
	})

	return consumers
}

func (k *KafkaConsumerManager) Close() error {
	k.mKafkaConsGroup.Lock()
	defer k.mKafkaConsGroup.Unlock()

	var errCum error
	for consGroupName, consGroup := range k.kafkaConsGroup {
		if consGroup == nil {
			continue
		}
		_err := consGroup.Close()
		if _err != nil {
			errCum = multierror.Append(errCum, fmt.Errorf("consumer group '%s': %w", consGroupName, _err))
		}
	}

	for consGroupName, consGroup := range k.kafkaPausedConsGroup {
		if consGroup == nil {
			continue
		}
		_err := consGroup.Close()
		if _err != nil {
			errCum = multierror.Append(errCum, fmt.Errorf("consumer group '%s': %w", consGroupName, _err))
		}
	}

	return errCum
}
