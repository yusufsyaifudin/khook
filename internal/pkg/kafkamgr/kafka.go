package kafkamgr

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/yusufsyaifudin/khook/storage"
	"github.com/yusufsyaifudin/khook/storage/inmem"
	"log"
	"sync"
	"time"
)

type kafkaOpt struct {
	connStore        storage.KafkaConnStore
	rebuildConnEvery time.Duration
}

func kafkaOptDefault() *kafkaOpt {
	return &kafkaOpt{
		connStore:        inmem.NewKafkaConnStore(),
		rebuildConnEvery: 10 * time.Second,
	}
}

type KafkaOpt func(opt *kafkaOpt) error

func WithConnStore(store storage.KafkaConnStore) KafkaOpt {
	return func(opt *kafkaOpt) error {
		if store == nil {
			return fmt.Errorf("nil storage")
		}

		opt.connStore = store
		return nil
	}
}

func WithRebuildConnInterval(interval time.Duration) KafkaOpt {
	return func(opt *kafkaOpt) error {
		if interval.Seconds() <= 0 {
			return nil
		}

		opt.rebuildConnEvery = interval
		return nil
	}
}

type Kafka struct {
	kafkaOpt *kafkaOpt
	ticker   *time.Ticker

	kafkaConnLock   sync.RWMutex
	kafkaConnCfgMap map[string]storage.KafkaConfig
	kafkaConnMap    map[string]sarama.Client
}

func NewKafka(opts ...KafkaOpt) (*Kafka, error) {
	defaultOpt := kafkaOptDefault()
	for _, opt := range opts {
		err := opt(defaultOpt)
		if err != nil {
			return nil, err
		}
	}

	svc := &Kafka{
		kafkaOpt:        defaultOpt,
		ticker:          time.NewTicker(defaultOpt.rebuildConnEvery),
		kafkaConnLock:   sync.RWMutex{},
		kafkaConnCfgMap: make(map[string]storage.KafkaConfig),
		kafkaConnMap:    make(map[string]sarama.Client),
	}

	go svc.manageConnection()

	return svc, nil
}

func (k *Kafka) manageConnection() {
	for {
		select {
		case t := <-k.ticker.C:
			log.Printf("update connection at %s\n", t.Format(time.RFC3339Nano))

			shortTimeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			listKafkaCfg, err := k.kafkaOpt.connStore.GetAllKafkaConfig(shortTimeCtx)
			cancel()
			if err != nil {
				log.Printf("error when update connection: failed to get all kafka config: %s\n", err)
				continue
			}

			for _, kafkaConfig := range listKafkaCfg.KafkaConfigs {
				log.Printf("check conn id %s exist or not with read lock\n", kafkaConfig.Label)

				k.kafkaConnLock.RLock()
				if conn, exist := k.kafkaConnMap[kafkaConfig.Label]; exist && conn != nil {
					_, err = conn.RefreshController()
					if err == nil {
						// skip this connection to be re-connected, because it already connected
						k.kafkaConnLock.RUnlock() // before continue, don't forget to unlock
						continue
					}
				}
				k.kafkaConnLock.RUnlock()

				// Try to re-connect the current connection if it failed to refresh controller
				saramaCfg := sarama.NewConfig()
				kafkaConn, err := sarama.NewClient(kafkaConfig.Address, saramaCfg)
				if err != nil {
					log.Printf("conn id %s is error to create: %s\n", kafkaConfig.Label, err)
					continue // read lock should already un-locked before continue
				}

				k.kafkaConnLock.Lock()
				k.kafkaConnCfgMap[kafkaConfig.Label] = kafkaConfig
				k.kafkaConnMap[kafkaConfig.Label] = kafkaConn
				k.kafkaConnLock.Unlock()
			}
		}
	}
}

type ConnInfo struct {
	Label   string   `json:"label,omitempty"`
	Brokers []string `json:"brokers,omitempty"`
	Topics  []string `json:"topics,omitempty"`
	Error   string   `json:"error,omitempty"`
}

func (k *Kafka) GetAllConn(ctx context.Context) (conn []ConnInfo) {
	k.kafkaConnLock.RLock()
	defer k.kafkaConnLock.RUnlock()

	syncMap := &sync.Map{}
	wg := &sync.WaitGroup{}
	for kafkaConnID, client := range k.kafkaConnMap {
		wg.Add(1)

		// TODO: we need cancellation in Go routine
		go func(_connID string, _wg *sync.WaitGroup, _map *sync.Map, _client sarama.Client) {
			defer wg.Done()

			connInfo := ConnInfo{
				Brokers: make([]string, 0),
			}

			kafkaCfg, exist := k.kafkaConnCfgMap[_connID]
			if !exist {
				connInfo.Error = fmt.Sprintf("connection config detail for id %s not available", _connID)
				_map.Store(_connID, connInfo)
				return
			}

			connInfo.Label = kafkaCfg.Label

			for _, b := range _client.Brokers() {
				if b == nil {
					continue
				}

				connInfo.Brokers = append(connInfo.Brokers, b.Addr())
			}

			_topics, _err := _client.Topics()
			if _err != nil {
				connInfo.Error = _err.Error()

				_map.Store(_connID, connInfo)
				return
			}

			connInfo.Topics = _topics
			_map.Store(_connID, connInfo)
			return

		}(kafkaConnID, wg, syncMap, client)
	}

	wg.Wait()

	conn = make([]ConnInfo, 0)
	syncMap.Range(func(key, value any) bool {
		conn = append(conn, value.(ConnInfo))
		return true
	})

	return
}

func (k *Kafka) GetConn(ctx context.Context, label string) (sarama.Client, error) {
	k.kafkaConnLock.RLock()
	client, exist := k.kafkaConnMap[label]
	if !exist {
		k.kafkaConnLock.RUnlock()

		// also ensure key is not exist in config map
		k.kafkaConnLock.Lock()
		delete(k.kafkaConnMap, label)
		delete(k.kafkaConnCfgMap, label)
		k.kafkaConnLock.Unlock()
		return nil, fmt.Errorf("cannot get kafka connection for label '%s'", label)
	}

	k.kafkaConnLock.RUnlock()

	if client == nil {
		k.kafkaConnLock.Lock()
		delete(k.kafkaConnMap, label)
		delete(k.kafkaConnCfgMap, label)
		k.kafkaConnLock.Unlock()
		return nil, fmt.Errorf("nil kafka client for label '%s'", label)
	}

	return client, nil
}
