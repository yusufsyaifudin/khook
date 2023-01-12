package manager

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/hashicorp/go-multierror"
	"github.com/yusufsyaifudin/khook/pkg/types"
	"github.com/yusufsyaifudin/khook/pkg/validator"
	"io"
	"log"
	"sort"
	"sync"
)

type ConsumerGroupInfo struct {
	Name              string               `validate:"required,resource_name"`
	Namespace         string               `validate:"required,resource_name"`
	ConsumerGroupConn sarama.ConsumerGroup `validate:"required"`
}

type ConnManager struct {
	lock sync.RWMutex

	checksum map[string]string

	// activeConsumer contains KafkaClientManager ConsumerGroup connection with the kafka client id as key.
	// SinkTarget label must be unique.
	activeConsumer map[string]*ConsumerGroupInfo

	// pausedConsumer contains paused KafkaClientManager ConsumerGroup connection.
	pausedConsumer map[string]*ConsumerGroupInfo
}

func NewConnManager() *ConnManager {
	c := &ConnManager{
		lock:           sync.RWMutex{},
		checksum:       make(map[string]string),
		activeConsumer: make(map[string]*ConsumerGroupInfo),
		pausedConsumer: make(map[string]*ConsumerGroupInfo),
	}

	return c
}

func (c *ConnManager) Add(conn *ConsumerGroupInfo, consumerCfg types.KafkaConsumerConfig) error {
	err := validator.Validate(conn)
	if err != nil {
		err = fmt.Errorf("cannot validate consumer group info: %w", err)
		return err
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	checksum, err := c.calculateChecksum(consumerCfg)
	if err != nil {
		return err
	}

	connID := c.getConnID(conn)
	c.activeConsumer[connID] = conn
	c.checksum[connID] = checksum
	return nil
}

func (c *ConnManager) IsUpdated(consumerCfg types.KafkaConsumerConfig) bool {
	c.lock.Lock()
	defer c.lock.Unlock()

	checksum, err := c.calculateChecksum(consumerCfg)
	if err != nil {
		return false
	}

	connID := c.getConnIDByNs(consumerCfg.Namespace, consumerCfg.Name)
	changed := c.checksum[connID] != checksum

	if !changed {
		log.Printf("consumer will not be updated because checksum is not changed: %s\n", checksum)
	}

	return changed
}

func (c *ConnManager) Delete(ns, name string) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	connID := c.getConnIDByNs(ns, name)
	delete(c.activeConsumer, connID)
	delete(c.pausedConsumer, connID)
	return nil
}

func (c *ConnManager) GetActive() []ConsumerGroupInfo {
	c.lock.RLock()
	defer c.lock.RUnlock()

	consumers := make([]ConsumerGroupInfo, 0)
	for _, info := range c.activeConsumer {
		if info == nil {
			continue
		}
		consumers = append(consumers, *info)
	}

	sort.Slice(consumers, func(i, j int) bool {
		return consumers[i].Name < consumers[j].Name
	})

	return consumers
}

func (c *ConnManager) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	var errCum error
	for connID, consumerInfo := range c.activeConsumer {
		if consumerInfo == nil {
			continue
		}

		if consumerInfo.ConsumerGroupConn == nil {
			continue
		}

		_err := consumerInfo.ConsumerGroupConn.Close()
		if _err != nil {
			errCum = multierror.Append(errCum, fmt.Errorf("consumer group '%s': %w", connID, _err))
		}
	}

	for consGroupName, consumerInfo := range c.pausedConsumer {
		if consumerInfo == nil {
			continue
		}

		if consumerInfo.ConsumerGroupConn == nil {
			continue
		}

		_err := consumerInfo.ConsumerGroupConn.Close()
		if _err != nil {
			errCum = multierror.Append(errCum, fmt.Errorf("consumer group '%s': %w", consGroupName, _err))
		}
	}

	return errCum
}

func (c *ConnManager) getConnID(conn *ConsumerGroupInfo) string {
	return c.getConnIDByNs(conn.Namespace, conn.Name)
}

func (c *ConnManager) getConnIDByNs(ns, name string) string {
	return fmt.Sprintf("%s:%s", ns, name)
}

func (c *ConnManager) calculateChecksum(consumerCfg types.KafkaConsumerConfig) (string, error) {
	specBytes, err := json.Marshal(consumerCfg.Spec)
	if err != nil {
		err = fmt.Errorf("cannot marshal consumer config spec: %w", err)
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
