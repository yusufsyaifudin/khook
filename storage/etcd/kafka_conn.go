package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/yusufsyaifudin/khook/pkg/types"
	"github.com/yusufsyaifudin/khook/storage"
	clientv3 "go.etcd.io/etcd/client/v3"
	"io"
	"sort"
	"sync"
	"time"
)

const (
	prefixKey = "kafkaconn:"
)

type kafkaConnStoreConfig struct {
	Endpoints []string
}

type Option func(*kafkaConnStoreConfig) error

func WithEndpoints(endpoints []string) Option {
	return func(config *kafkaConnStoreConfig) error {
		if len(endpoints) <= 0 {
			return fmt.Errorf("empty enpdoint list")
		}
		config.Endpoints = endpoints
		return nil
	}
}

type KafkaConnStore struct {
	kafkaConnStoreConfig *kafkaConnStoreConfig
	client               *clientv3.Client
}

var _ storage.KafkaConnStore = (*KafkaConnStore)(nil)

func NewKafkaConnStore(opts ...Option) (*KafkaConnStore, error) {
	opt := &kafkaConnStoreConfig{
		Endpoints: []string{"localhost:2379"},
	}

	for _, option := range opts {
		err := option(opt)
		if err != nil {
			return nil, err
		}
	}

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   opt.Endpoints,
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	return &KafkaConnStore{client: etcdClient}, nil
}

func (k *KafkaConnStore) PersistKafkaConnConfig(ctx context.Context, in storage.InPersistKafkaConnConfig) (out storage.OutPersistKafkaConnConfig, err error) {
	key := fmt.Sprintf("%s:%s:%s", prefixKey, in.Resource.Metadata.Namespace, in.Resource.Metadata.Name)

	data, err := json.Marshal(in.Resource)
	if err != nil {
		return
	}

	_, err = k.client.Put(ctx, key, string(data))
	if err != nil {
		return
	}

	getConnOut, err := k.GetKafkaConnConfig(ctx, storage.InGetKafkaConnConfig{
		Namespace: in.Resource.Metadata.Namespace,
		Name:      in.Resource.Metadata.Name,
	})
	if err != nil {
		return
	}

	out = storage.OutPersistKafkaConnConfig{
		Resource: getConnOut.Resource,
	}
	return
}

func (k *KafkaConnStore) GetKafkaConnConfig(ctx context.Context, in storage.InGetKafkaConnConfig) (out storage.OutGetKafkaConnConfig, err error) {
	key := fmt.Sprintf("%s:%s:%s", prefixKey, in.Namespace, in.Name)

	getResp, err := k.client.Get(ctx, key)
	if err != nil {
		return
	}

	respLen := len(getResp.Kvs)
	if respLen != 1 {
		err = fmt.Errorf("kafka config should only return one row: %w", err)
		return
	}

	kv := getResp.Kvs[0]
	var kafkaConn types.KafkaBrokerConfig
	err = json.Unmarshal(kv.Value, &kafkaConn)
	if err != nil {
		return
	}

	out = storage.OutGetKafkaConnConfig{
		Resource: kafkaConn,
	}
	return
}

func (k *KafkaConnStore) GetKafkaConnConfigs(ctx context.Context) (rows storage.KafkaConnConfigRows, err error) {
	getResp, err := k.client.Get(ctx, prefixKey, clientv3.WithPrefix())
	if err != nil {
		return
	}

	kafkaConfigs := make([]types.KafkaBrokerConfig, 0)
	for _, kv := range getResp.Kvs {
		if kv == nil {
			continue
		}

		var kafkaConn types.KafkaBrokerConfig
		_err := json.Unmarshal(kv.Value, &kafkaConn)
		if _err != nil {
			continue
		}

		kafkaConfigs = append(kafkaConfigs, kafkaConn)
	}

	sort.Slice(kafkaConfigs, func(i, j int) bool {
		return kafkaConfigs[i].Name < kafkaConfigs[j].Name
	})

	rows = &KafkaConfigRows{
		kafkaConfigs: kafkaConfigs,
	}
	return
}

func (k *KafkaConnStore) WatchKafkaConnConfig(ctx context.Context, out chan storage.OutWatchKafkaConnConfig) {
	watchChan := k.client.Watch(ctx, prefixKey, clientv3.WithPrefix())
	for resp := range watchChan {
		for _, ev := range resp.Events {
			if ev.Kv == nil {
				continue
			}

			var kafkaConn types.KafkaBrokerConfig
			err := json.Unmarshal(ev.Kv.Value, &kafkaConn)
			if err != nil {
				continue
			}

			var changeType storage.ChangeType
			switch ev.Type {
			case clientv3.EventTypePut:
				changeType = storage.Put
			case clientv3.EventTypeDelete:
				changeType = storage.Delete
			}

			out <- storage.OutWatchKafkaConnConfig{
				ChangeType: changeType,
				Resource:   kafkaConn,
			}
		}
	}
}

func (k *KafkaConnStore) Close() error {
	return k.client.Close()
}

type KafkaConfigRows struct {
	lock            sync.Mutex
	kafkaConfigs    []types.KafkaBrokerConfig
	currKafkaConfig *types.KafkaBrokerConfig
}

var _ storage.KafkaConnConfigRows = (*KafkaConfigRows)(nil)

func (w *KafkaConfigRows) Next() bool {
	w.lock.Lock()
	defer w.lock.Unlock()

	if len(w.kafkaConfigs) > 0 {
		w.currKafkaConfig, w.kafkaConfigs = &w.kafkaConfigs[0], w.kafkaConfigs[1:]
		return true
	}

	return false
}

func (w *KafkaConfigRows) KafkaBrokerConfig() (types.KafkaBrokerConfig, error) {
	w.lock.Lock()
	defer w.lock.Unlock()
	if w.currKafkaConfig == nil {
		return types.KafkaBrokerConfig{}, io.ErrUnexpectedEOF
	}

	return *w.currKafkaConfig, nil
}
