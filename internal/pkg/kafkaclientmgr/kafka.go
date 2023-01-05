package kafkaclientmgr

import (
	"context"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/google/go-cmp/cmp"
	"github.com/hashicorp/go-multierror"
	"github.com/yusufsyaifudin/khook/storage"
	"github.com/yusufsyaifudin/khook/storage/inmem"
	"log"
	"math/rand"
	"sync"
	"time"
)

type kafkaOpt struct {
	kafkaConnStore   storage.KafkaConnStore
	refreshConnEvery time.Duration
	updateConnEvery  time.Duration
}

func kafkaOptDefault() *kafkaOpt {
	return &kafkaOpt{
		kafkaConnStore:   inmem.NewKafkaConnStore(),
		refreshConnEvery: 10 * time.Second,
		updateConnEvery:  10 * time.Second,
	}
}

type KafkaOpt func(opt *kafkaOpt) error

func WithConnStore(store storage.KafkaConnStore) KafkaOpt {
	return func(opt *kafkaOpt) error {
		if store == nil {
			return fmt.Errorf("nil KafkaConnStore")
		}

		opt.kafkaConnStore = store
		return nil
	}
}

func WithRefreshConnInterval(interval time.Duration) KafkaOpt {
	return func(opt *kafkaOpt) error {
		if interval.Seconds() <= 0 {
			return nil
		}

		opt.refreshConnEvery = interval
		return nil
	}
}

func WithUpdateConnInterval(interval time.Duration) KafkaOpt {
	return func(opt *kafkaOpt) error {
		if interval.Seconds() <= 0 {
			return nil
		}

		opt.updateConnEvery = interval
		return nil
	}
}

type kafkaConnState struct {
	Config storage.KafkaConfig
	Client sarama.Client
}

type mutateConnState struct {
	Action string
	Label  string
	Config storage.KafkaConfig
}

// KafkaClientManager is a manager to create and handle the Kafka connection.
type KafkaClientManager struct {
	kafkaOpt          *kafkaOpt
	tickerRefreshConn *time.Ticker
	tickerUpdateConn  *time.Ticker
	mutateConnStates  chan mutateConnState
	kafkaConnLock     sync.RWMutex

	// kafkaConnMap kafka label as key
	kafkaConnMap map[string]*kafkaConnState
}

func NewKafkaClientManager(opts ...KafkaOpt) (*KafkaClientManager, error) {
	defaultOpt := kafkaOptDefault()
	for _, opt := range opts {
		err := opt(defaultOpt)
		if err != nil {
			return nil, err
		}
	}

	svc := &KafkaClientManager{
		kafkaOpt:          defaultOpt,
		tickerRefreshConn: time.NewTicker(defaultOpt.refreshConnEvery + (time.Duration(rand.Int63n(100)) * time.Millisecond)),
		tickerUpdateConn:  time.NewTicker(defaultOpt.updateConnEvery + (time.Duration(rand.Int63n(150)) * time.Millisecond)),
		mutateConnStates:  make(chan mutateConnState, 10),
		kafkaConnLock:     sync.RWMutex{},
		kafkaConnMap:      make(map[string]*kafkaConnState),
	}

	go svc.manageConnection()
	return svc, nil
}

func (k *KafkaClientManager) AddConnection(ctx context.Context, cfg storage.KafkaConfig) error {
	// TODO validate config, if error then return immediately
	k.mutateConnStates <- mutateConnState{
		Action: "add",
		Label:  cfg.Label,
		Config: cfg,
	}
	return nil
}

func (k *KafkaClientManager) addConnection(kafkaCfg storage.KafkaConfig) {
	// Add new connection, but before that, make sure the connection is not duplicate.
	k.kafkaConnLock.RLock()
	kafkaState, connExist := k.kafkaConnMap[kafkaCfg.Label]
	if connExist && cmp.Equal(kafkaState.Config, kafkaCfg) && kafkaState != nil && kafkaState.Client != nil {
		if _, _err := kafkaState.Client.RefreshController(); _err == nil {
			// connection exist, and still good: leave it alone
			k.kafkaConnLock.RUnlock() // before continue, don't forget to unlock
			return
		}

		// connection is not good, try to acquire new connection
	}
	k.kafkaConnLock.RUnlock()

	saramaCfg := sarama.NewConfig()
	kafkaConn, err := sarama.NewClient(kafkaCfg.Address, saramaCfg)
	if err != nil {
		log.Printf("conn id %s is error to create: %s\n", kafkaCfg.Label, err)
		return
	}

	k.kafkaConnLock.Lock()
	k.kafkaConnMap[kafkaCfg.Label] = &kafkaConnState{
		Config: kafkaCfg,
		Client: kafkaConn,
	}
	k.kafkaConnLock.Unlock()
}

func (k *KafkaClientManager) DeleteConnection(ctx context.Context, kafkaCfgLabel string) error {
	// TODO validate config, if error then return immediately
	k.mutateConnStates <- mutateConnState{
		Action: "delete",
		Label:  kafkaCfgLabel,
	}
	return nil
}

func (k *KafkaClientManager) deleteConnection(kafkaCfgLabel string) {
	k.kafkaConnLock.Lock()
	defer k.kafkaConnLock.Unlock()
	connState, exist := k.kafkaConnMap[kafkaCfgLabel]
	if !exist {
		return
	}

	if connState.Client == nil {
		return
	}

	delete(k.kafkaConnMap, kafkaCfgLabel)
	err := connState.Client.Close()
	if err != nil {
		log.Printf("error when delete and close connection '%s'\n", kafkaCfgLabel)
	}

	return
}

func (k *KafkaClientManager) manageConnection() {
	for {
		select {

		case mutateConn := <-k.mutateConnStates:
			switch mutateConn.Action {
			case "add":
				k.addConnection(mutateConn.Config)
			case "delete":
				k.deleteConnection(mutateConn.Label)
			}

		case t := <-k.tickerRefreshConn.C:
			// ping connection periodically..
			log.Printf("refresh connection at %s\n", t.Format(time.RFC3339Nano))
			k.refreshConnections()

		case t := <-k.tickerUpdateConn.C:
			// ping connection periodically..
			log.Printf("update connection at %s\n", t.Format(time.RFC3339Nano))
			k.updateConnections()

		}
	}
}

// updateConnections will update connection from Database.
// If found new connection info, then it will add to the list.
// Why we iterate from Database?
// Because, it easy to scale. Imagine we only one source of truth (database)
// with Redis as cache. When we deploy this program to many nodes,
// we eventually will get the same Kafka connection list.
//
// Then, in another code, we can use Redis as distributed state management to
// distribute number of consumers depending on Kafka partition.
func (k *KafkaClientManager) updateConnections() {
	outKafkaCfg, err := k.kafkaOpt.kafkaConnStore.GetAllKafkaConfig(context.Background())
	if err != nil {
		log.Printf("cannot list kafka config during rebalance, your connection may be outdated: %s\n", err)
		return
	}

	k.kafkaConnLock.RLock()
	unvisitedConn := make(map[string]struct{})
	for kafkaLabel := range k.kafkaConnMap {
		unvisitedConn[kafkaLabel] = struct{}{}
	}
	k.kafkaConnLock.RUnlock()

	for outKafkaCfg.Next() {
		kafkaCfg, _err := outKafkaCfg.KafkaConfig()
		if _err != nil {
			log.Printf("getting kafka config row error: %s\n", _err)
			continue
		}

		kafkaLabel := kafkaCfg.Label

		log.Printf("check conn id %s exist or not with read lock\n", kafkaLabel)

		// delete from unvisited.
		// if after for loop done it still not empty, it means that the connection is not exist anymore.
		// we can then close and delete the connection
		delete(unvisitedConn, kafkaLabel)

		k.kafkaConnLock.RLock()
		_, exist := k.kafkaConnMap[kafkaLabel]
		if exist {
			// If existed, then leave it alone.
			// Another go routine refreshConnections, will refresh the existing connection
			k.kafkaConnLock.RUnlock() // unlock read before continue
			continue
		}

		// Try to re-connect the current connection if it failed to refresh controller
		saramaCfg := sarama.NewConfig()
		kafkaConn, _err := sarama.NewClient(kafkaCfg.Address, saramaCfg)
		if _err != nil {
			log.Printf("conn id %s is error to create: %s\n", kafkaLabel, _err)

			k.kafkaConnLock.RUnlock() // unlock read before continue
			continue                  // read lock should already un-locked before continue
		}

		k.kafkaConnLock.RUnlock() // unlock read before lock write
		k.kafkaConnLock.Lock()
		k.kafkaConnMap[kafkaLabel] = &kafkaConnState{
			Config: kafkaCfg,
			Client: kafkaConn,
		}
		k.kafkaConnLock.Unlock()
	}

	// delete unvisited connection from the connection list.
	for kafkaLabel := range unvisitedConn {
		log.Printf("deleting kafka connection '%s' from list because not exist in storage\n", unvisitedConn)
		k.kafkaConnLock.Lock()
		connState, exist := k.kafkaConnMap[kafkaLabel]
		if !exist {
			k.kafkaConnLock.Unlock()
			continue
		}

		if connState == nil {
			k.kafkaConnLock.Unlock()
			continue
		}

		if connState.Client == nil {
			k.kafkaConnLock.Unlock()
			continue
		}

		// TODO: need inform the consumer that this already deleted

		_err := connState.Client.Close()
		if _err != nil {
			log.Printf("cannot close client '%s' on unvisited connection: %s\n", kafkaLabel, _err)
			k.kafkaConnLock.Unlock()
			continue
		}

		delete(k.kafkaConnMap, kafkaLabel)
		k.kafkaConnLock.Unlock()
	}

}

func (k *KafkaClientManager) refreshConnections() {
	k.kafkaConnLock.RLock()
	localMapLabel := make(map[string]struct{})
	for kafkaLabel := range k.kafkaConnMap {
		localMapLabel[kafkaLabel] = struct{}{}
	}
	k.kafkaConnLock.RUnlock()

	for kafkaLabel := range localMapLabel {
		log.Printf("check conn id %s exist or not with read lock\n", kafkaLabel)

		k.kafkaConnLock.RLock()
		kafkaState, exist := k.kafkaConnMap[kafkaLabel]
		if !exist {
			k.kafkaConnLock.RUnlock()
			continue
		}

		if kafkaState == nil {
			// If this happens, it weirds. Because, no way that it nil but key exist.
			log.Printf("connection '%s' is nil\n", kafkaLabel)

			k.kafkaConnLock.RUnlock()
			k.kafkaConnLock.Lock()
			delete(k.kafkaConnMap, kafkaLabel)
			k.kafkaConnLock.Unlock()
			continue
		}

		if kafkaState.Client != nil {
			isRemoveThisConn := false
			cfg := kafkaState.Client.Config()
			for _, broker := range kafkaState.Client.Brokers() {
				if isRemoveThisConn {
					continue // continue on the next broker, if one is unavailable
				}

				_err := broker.Open(cfg)
				if errors.Is(_err, sarama.ErrAlreadyConnected) || _err == nil {
					log.Printf("connection '%s' broker '%s' is still up\n", kafkaLabel, broker.Addr())
					continue // continue brokers loop
				} else {
					// if one broker is failed, then remove it from connection list, so it can be re-connected again
					log.Printf("connection '%s' broker '%s' is error: %s\n", kafkaLabel, broker.Addr(), _err)
					isRemoveThisConn = true
					continue // continue brokers loop
				}
			}

			if isRemoveThisConn {
				k.kafkaConnLock.RUnlock() // unlock read before lock write
				k.kafkaConnLock.Lock()
				delete(k.kafkaConnMap, kafkaLabel)
				k.kafkaConnLock.Unlock()

				log.Printf("one of connection '%s' brokers failed, removed\n", kafkaLabel)
				continue // continue parent operation
			}
		}

		// Try to re-connect the current connection if it failed to refresh controller
		saramaCfg := sarama.NewConfig()
		kafkaConn, err := sarama.NewClient(kafkaState.Config.Address, saramaCfg)
		if err != nil {
			log.Printf("conn id %s is error to create: %s\n", kafkaLabel, err)

			k.kafkaConnLock.RUnlock() // read lock should already un-locked before continue
			continue
		}

		k.kafkaConnLock.RUnlock() // unlock read before lock write
		k.kafkaConnLock.Lock()
		k.kafkaConnMap[kafkaLabel] = &kafkaConnState{
			Config: kafkaState.Config,
			Client: kafkaConn,
		}
		k.kafkaConnLock.Unlock()
	}
}

type ConnInfo struct {
	Label   string   `json:"label,omitempty"`
	Brokers []string `json:"brokers,omitempty"`
	Topics  []string `json:"topics,omitempty"`
	Error   string   `json:"error,omitempty"`
}

func (k *KafkaClientManager) GetAllConn(ctx context.Context) (conn []ConnInfo) {
	k.kafkaConnLock.RLock()
	defer k.kafkaConnLock.RUnlock()

	syncMap := &sync.Map{}
	wg := &sync.WaitGroup{}
	for _, kafkaState := range k.kafkaConnMap {
		wg.Add(1)

		// TODO: we need cancellation in Go routine
		go func(_wg *sync.WaitGroup, _map *sync.Map, _kafkaState *kafkaConnState) {
			defer wg.Done()

			connInfo := ConnInfo{
				Label:   _kafkaState.Config.Label,
				Brokers: make([]string, 0),
				Topics:  nil,
			}

			for _, b := range _kafkaState.Client.Brokers() {
				if b == nil {
					continue
				}

				connInfo.Brokers = append(connInfo.Brokers, b.Addr())
			}

			_topics, _err := _kafkaState.Client.Topics()
			if _err != nil {
				connInfo.Error = _err.Error()

				_map.Store(connInfo.Label, connInfo)
				return
			}

			connInfo.Topics = _topics
			_map.Store(connInfo.Label, connInfo)
			return

		}(wg, syncMap, kafkaState)
	}

	wg.Wait()

	conn = make([]ConnInfo, 0)
	syncMap.Range(func(key, value any) bool {
		conn = append(conn, value.(ConnInfo))
		return true
	})

	return
}

func (k *KafkaClientManager) GetConn(ctx context.Context, label string) (sarama.Client, error) {
	k.kafkaConnLock.RLock()
	client, exist := k.kafkaConnMap[label]
	if !exist {
		k.kafkaConnLock.RUnlock()

		// also ensure key is not exist in config map
		k.kafkaConnLock.Lock()
		delete(k.kafkaConnMap, label)
		k.kafkaConnLock.Unlock()
		return nil, fmt.Errorf("cannot get kafka connection for label '%s'", label)
	}

	k.kafkaConnLock.RUnlock()

	if client == nil {
		k.kafkaConnLock.Lock()
		delete(k.kafkaConnMap, label)
		k.kafkaConnLock.Unlock()
		return nil, fmt.Errorf("nil kafka client for label '%s'", label)
	}

	_, err := client.Client.Topics()
	if err != nil {
		err = fmt.Errorf("listing topic error: %w", err)
		return nil, err
	}

	return client.Client, nil
}

func (k *KafkaClientManager) Close() error {
	k.kafkaConnLock.Lock()
	defer k.kafkaConnLock.Unlock()

	var errCum error
	for connLabel, connState := range k.kafkaConnMap {
		if connState == nil {
			continue
		}
		_err := connState.Client.Close()
		if _err != nil {
			errCum = multierror.Append(errCum, fmt.Errorf("kafka config '%s': %w", connLabel, _err))
		}
	}

	return errCum
}
