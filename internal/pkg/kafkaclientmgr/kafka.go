package kafkaclientmgr

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/hashicorp/go-multierror"
	"github.com/yusufsyaifudin/khook/pkg/types"
	"github.com/yusufsyaifudin/khook/storage"
	"golang.org/x/sync/semaphore"
	"io"
	"log"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"
)

type kafkaConnState struct {
	// Save checksum of the client as SHA256 to minimize memory footprint
	Checksum string
	Client   sarama.Client
}

type configOption struct {
	kafkaConnStore  storage.KafkaConnStore
	updateConnEvery time.Duration
}

// KafkaClientManager is a manager to create and handle the Kafka connection.
type KafkaClientManager struct {
	cfgOpt           *configOption
	tickerUpdateConn *time.Ticker
	sem              *semaphore.Weighted
	kafkaConnLock    sync.RWMutex

	// kafkaConnMap kafka label as key
	kafkaConnMap        map[string]*kafkaConnState
	relabanceInProgress bool

	// watch connection config changes
	watchConnConfig chan storage.OutWatchKafkaConnConfig

	// Once ensure only once operation is called when init KafkaClientManager
	once sync.Once
}

var _ Manager = (*KafkaClientManager)(nil)

func NewKafkaClientManager(opts ...KafkaOpt) (*KafkaClientManager, error) {
	defaultOpt := defaultKafkaOpt()
	for _, opt := range opts {
		err := opt(defaultOpt)
		if err != nil {
			return nil, err
		}
	}

	svc := &KafkaClientManager{
		cfgOpt:           defaultOpt,
		tickerUpdateConn: time.NewTicker(defaultOpt.updateConnEvery + (time.Duration(rand.Int63n(150)) * time.Millisecond)),
		sem:              semaphore.NewWeighted(10),
		kafkaConnLock:    sync.RWMutex{},
		kafkaConnMap:     make(map[string]*kafkaConnState),
		watchConnConfig:  make(chan storage.OutWatchKafkaConnConfig),
	}

	go svc.init()
	go svc.cfgOpt.kafkaConnStore.WatchKafkaConnConfig(context.Background(), svc.watchConnConfig)
	go svc.manageConnection()

	return svc, nil
}

// init will call the
func (k *KafkaClientManager) init() {
	k.once.Do(func() {
		k.updateConnections()
	})
}

func (k *KafkaClientManager) getConnID(kafkaCfg types.KafkaBrokerConfig) string {
	return k.getConnIDScoped(kafkaCfg.Namespace, kafkaCfg.Name)
}

func (k *KafkaClientManager) getConnIDScoped(ns, name string) string {
	return fmt.Sprintf("%s:%s", ns, name)
}

// addOrRefreshConnection Add new connection, but before that, make sure the connection is not duplicate.
func (k *KafkaClientManager) addOrRefreshConnection(kafkaCfg types.KafkaBrokerConfig) {
	connID := k.getConnID(kafkaCfg)

	k.kafkaConnLock.RLock() // lock read

	currConn, connExist := k.kafkaConnMap[connID]
	log.Printf(
		"conn status label '%s' %t state not nil='%t'\n",
		kafkaCfg.Name, connExist, currConn != nil,
	)

	checksum, errChecksum := specChecksum(kafkaCfg.Spec)
	if errChecksum != nil {
		log.Printf("cannot calculate checksum for spec kafka: %s\n", errChecksum)
		return
	}

	isRemoveThisConn := false
	if connExist && k.kafkaConnMap[connID].Checksum == checksum && currConn != nil && currConn.Client != nil {

		// if connection is existed AND checksum is still same:
		// try to check each connection to brokers
		saramaClientCfg := currConn.Client.Config()
		for idxBroker, broker := range currConn.Client.Brokers() { // brokers loop
			if isRemoveThisConn {
				continue // skip all next brokers, if one broker is already unavailable
			}

			if broker == nil {
				continue // continue on the next broker, if one broker is unavailable
			}

			_errBroker := broker.Open(saramaClientCfg)
			if errors.Is(_errBroker, sarama.ErrAlreadyConnected) {
				log.Printf("connection '%s' broker %d '%s' is still up\n", kafkaCfg.Name, idxBroker, broker.Addr())

				_errBroker = nil // discard error, so it will continue to next iteration
			}

			if _errBroker != nil {
				// if one broker is failed, then remove it from connection list, so it can be re-connected again
				log.Printf("connection '%s' broker %d '%s' is error: %s\n", kafkaCfg.Name, idxBroker, broker.Addr(), _errBroker)
				isRemoveThisConn = true
				continue // continue brokers loop
			}
		} // end of brokers loop

	}

	k.kafkaConnLock.RUnlock() // unlock read

	// We will update the current connection if one of these condition return True:
	// * isRemoveThisConn = true               -> means that one of brokers is down
	// * connExist = false                     -> means that connection is not exist in the current opened connection
	// * currConn.ConfigCheckSum != checksum -> configuration is different
	// Otherwise, return it because all existing broker connection is still good.
	updateConnReasons := make([]string, 0)
	if isRemoveThisConn {
		updateConnReasons = append(updateConnReasons, "bad broker")
	}

	if !connExist || currConn == nil {
		updateConnReasons = append(updateConnReasons, "connection is not exist in the list")
	}

	if currConn != nil && currConn.Checksum != checksum {
		updateConnReasons = append(updateConnReasons, fmt.Sprintf("checksum changed '%s' to '%s'", currConn.Checksum, checksum))
	}

	if len(updateConnReasons) > 0 {
		k.kafkaConnLock.Lock() // unlock before return
		saramaCfg := sarama.NewConfig()
		kafkaConn, errClient := sarama.NewClient(kafkaCfg.Spec.Brokers, saramaCfg)
		if errClient != nil {
			log.Printf("conn id %s is error to create: %s\n", kafkaCfg.Name, errClient)
			k.kafkaConnLock.Unlock() // unlock before return

			// delete connection using function that we've created
			k.deleteConn(connID, errClient.Error())
			return
		}

		k.kafkaConnMap[connID] = &kafkaConnState{
			Checksum: checksum,
			Client:   kafkaConn,
		}

		log.Printf(
			"new kafka connection '%s' added to the list because: %s\n",
			kafkaCfg.Name,
			strings.Join(updateConnReasons, ", "),
		)

		k.kafkaConnLock.Unlock()
		return
	}

	log.Printf("good connection on label '%s'\n", kafkaCfg.Name)
}

func (k *KafkaClientManager) manageConnection() {
	for {
		select {
		case t := <-k.tickerUpdateConn.C:
			// ping connection periodically..
			log.Printf("update connection at %s\n", t.Format(time.RFC3339Nano))
			k.updateConnections()

		case updatedCfg := <-k.watchConnConfig:
			log.Printf("new config updated from database: %+v", updatedCfg)

			if updatedCfg.ChangeType == storage.Put {
				k.addOrRefreshConnection(updatedCfg.Resource)
			}

			if updatedCfg.ChangeType == storage.Delete {
				k.deleteConn(k.getConnID(updatedCfg.Resource), "got update from database")
			}
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

	// ensure that only one progress update from database is occurred
	// this to minimize burst calling into connection store when we set timer too frequently.
	k.kafkaConnLock.RLock()
	if k.relabanceInProgress {
		k.kafkaConnLock.RUnlock()
		return
	}
	k.kafkaConnLock.RUnlock()

	outKafkaCfg, err := k.cfgOpt.kafkaConnStore.GetKafkaConnConfigs(context.Background())
	if err != nil {
		log.Printf("cannot list kafka config during rebalance, your connection may be outdated: %s\n", err)
		return
	}

	// set re-balance flag to true, to mitigate the same action called at the same time
	k.kafkaConnLock.Lock()
	k.relabanceInProgress = true
	k.kafkaConnLock.Unlock()

	ctx := context.Background()
	wg := &sync.WaitGroup{}

	for outKafkaCfg.Next() {
		kafkaCfg, _err := outKafkaCfg.KafkaBrokerConfig()
		if _err != nil {
			log.Printf("getting kafka config row error: %s\n", _err)
			continue
		}

		kafkaLabel := kafkaCfg.Name
		kafkaNs := kafkaCfg.Namespace
		log.Printf("check conn id %s exist or not with read lock\n", kafkaLabel)

		_errAcq := k.sem.Acquire(ctx, 1)
		if _errAcq != nil {
			log.Printf("cannot acquire lock when trying to update connection '%s' on ns '%s': %s\n", kafkaLabel, kafkaNs, _errAcq)
			continue
		}

		// add or refresh connection if not exist using go routine
		// so, we can have multiple try to open connection
		wg.Add(1)
		go func(_wg *sync.WaitGroup, _kafkaCfg types.KafkaBrokerConfig) {
			defer func() {
				_wg.Done()
				k.sem.Release(1)
			}()

			k.addOrRefreshConnection(_kafkaCfg)
		}(wg, kafkaCfg)
	}

	wg.Wait()

	// release the in progress flag
	k.kafkaConnLock.Lock()
	k.relabanceInProgress = false
	k.kafkaConnLock.Unlock()
}

func (k *KafkaClientManager) deleteConn(connID string, reasons ...string) {
	// delete unvisited connection from the connection list.
	log.Printf("deleting kafka connection '%s' because: %s\n", connID, strings.Join(reasons, "; "))
	k.kafkaConnLock.Lock()
	defer k.kafkaConnLock.Unlock()
	connState, exist := k.kafkaConnMap[connID]
	if !exist {
		return
	}

	if connState == nil && connState.Client == nil {
		return
	}

	// TODO: need inform the consumer that this already deleted

	_err := connState.Client.Close()
	if _err != nil {
		log.Printf("cannot close client '%s' when %s: %s\n", connID, strings.Join(reasons, "; "), _err)
		return
	}

	delete(k.kafkaConnMap, connID)
	return
}

func (k *KafkaClientManager) GetAllConn(ctx context.Context) (conn []ConnInfo) {
	k.kafkaConnLock.RLock()
	defer k.kafkaConnLock.RUnlock()

	syncMap := &sync.Map{}
	wg := &sync.WaitGroup{}
	for connID, kafkaState := range k.kafkaConnMap {
		wg.Add(1)

		// TODO: we need cancellation in Go routine
		go func(_wg *sync.WaitGroup, _map *sync.Map, _connID string, _kafkaState *kafkaConnState) {
			defer wg.Done()

			connInfo := ConnInfo{
				Label:   _connID,
				Brokers: make([]string, 0),
				Topics:  nil,
			}

			// check connection state
			for _, b := range _kafkaState.Client.Brokers() {
				if b == nil {
					continue
				}

				if _, _err := b.Connected(); _err != nil {
					errStr := fmt.Sprintf(
						"check broker '%d' connection got error: %s",
						b.ID(), _err.Error(),
					)

					if connInfo.Error != "" {
						connInfo.Error = fmt.Sprintf("%s; %s", connInfo.Error, errStr)
						continue
					}

					connInfo.Error = errStr
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

		}(wg, syncMap, connID, kafkaState)
	}

	wg.Wait()

	conn = make([]ConnInfo, 0)
	syncMap.Range(func(key, value any) bool {
		conn = append(conn, value.(ConnInfo))
		return true
	})

	sort.Slice(conn, func(i, j int) bool {
		if conn[i].Label < conn[j].Label {
			return true
		}

		return false
	})

	return
}

func (k *KafkaClientManager) GetConn(ctx context.Context, ns, name string) (sarama.Client, error) {

	connID := k.getConnIDScoped(ns, name)
	k.kafkaConnLock.RLock()
	client, exist := k.kafkaConnMap[connID]
	if !exist {
		k.kafkaConnLock.RUnlock()

		// also ensure key is not exist in config map
		k.kafkaConnLock.Lock()
		delete(k.kafkaConnMap, connID)
		k.kafkaConnLock.Unlock()
		return nil, fmt.Errorf("cannot get kafka connection for name '%s' ns '%s'", name, ns)
	}

	k.kafkaConnLock.RUnlock()

	if client == nil {
		k.kafkaConnLock.Lock()
		delete(k.kafkaConnMap, connID)
		k.kafkaConnLock.Unlock()
		return nil, fmt.Errorf("nil kafka client for name '%s'", name)
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

func specChecksum(kafkaConn types.KafkaBrokerSpec) (string, error) {
	specBytes, err := json.Marshal(kafkaConn)
	if err != nil {
		return "", err
	}

	h := sha256.New()
	_, err = io.Copy(h, bytes.NewReader(specBytes))
	if err != nil {
		err = fmt.Errorf("cannot copy to calculate kafka config checksum: %w", err)
		return "", err
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}
