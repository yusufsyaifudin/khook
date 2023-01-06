package inmem

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/yusufsyaifudin/khook/pkg/validator"
	"io"
	"sync"

	"github.com/yusufsyaifudin/khook/storage"
)

type KafkaConnStore struct {
	store sync.Map
}

var _ storage.KafkaConnStore = (*KafkaConnStore)(nil)

func NewKafkaConnStore() *KafkaConnStore {
	return &KafkaConnStore{
		store: sync.Map{},
	}
}

func (k *KafkaConnStore) PersistKafkaConfig(ctx context.Context, in storage.InputPersistKafkaConfig) (out storage.OutPersistKafkaConfig, err error) {
	err = validator.Validate(in)
	if err != nil {
		err = fmt.Errorf("inmem: kafka config validation error: %w", err)
		return
	}

	kafkaCfg, _ := k.store.LoadOrStore(in.KafkaConfig.Label, in.KafkaConfig)

	b, err := json.Marshal(kafkaCfg)
	if err != nil {
		return
	}

	h := sha256.New()
	_, err = io.Copy(h, bytes.NewReader(b))
	if err != nil {
		err = fmt.Errorf("cannot copy to calculate config checksum: %w", err)
		return
	}

	checkSum := hex.EncodeToString(h.Sum(nil))

	out = storage.OutPersistKafkaConfig{
		CheckSum:    checkSum,
		KafkaConfig: kafkaCfg.(storage.KafkaConfig),
	}
	return
}

func (k *KafkaConnStore) GetKafkaConfigs(ctx context.Context) (rows storage.KafkaConfigRows, err error) {
	kafkaConfigs := make([]storage.KafkaConfig, 0)

	k.store.Range(func(key, value any) bool {
		kafkaConfig, ok := value.(storage.KafkaConfig)
		if ok {
			kafkaConfigs = append(kafkaConfigs, kafkaConfig)
		}

		return true
	})

	rows = &KafkaConfigRows{
		kafkaConfigs: kafkaConfigs,
	}
	return
}

func (k *KafkaConnStore) DeleteKafkaConfig(ctx context.Context, in storage.InputDeleteKafkaConfig) (out storage.OutDeleteKafkaConfig, err error) {
	err = validator.Validate(in)
	if err != nil {
		err = fmt.Errorf("inmem: input validation error: %w", err)
		return
	}

	k.store.Delete(in.Label)
	out = storage.OutDeleteKafkaConfig{Success: true}
	return
}

type KafkaConfigRows struct {
	lock            sync.Mutex
	kafkaConfigs    []storage.KafkaConfig
	currKafkaConfig *storage.KafkaConfig
}

var _ storage.KafkaConfigRows = (*KafkaConfigRows)(nil)

func (w *KafkaConfigRows) Next() bool {
	w.lock.Lock()
	defer w.lock.Unlock()

	if len(w.kafkaConfigs) > 0 {
		w.currKafkaConfig, w.kafkaConfigs = &w.kafkaConfigs[0], w.kafkaConfigs[1:]
		return true
	}

	return false
}

func (w *KafkaConfigRows) KafkaConfig() (storage.KafkaConfig, string, error) {
	w.lock.Lock()
	defer w.lock.Unlock()
	if w.currKafkaConfig == nil {
		return storage.KafkaConfig{}, "", io.ErrUnexpectedEOF
	}

	b, err := json.Marshal(w.currKafkaConfig)
	if err != nil {
		return storage.KafkaConfig{}, "", err
	}

	h := sha256.New()
	_, err = io.Copy(h, bytes.NewReader(b))
	if err != nil {
		err = fmt.Errorf("cannot copy to calculate config checksum: %w", err)
		return storage.KafkaConfig{}, "", err
	}

	checkSum := hex.EncodeToString(h.Sum(nil))
	return *w.currKafkaConfig, checkSum, nil
}
