package resourcemgr

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/yusufsyaifudin/khook/internal/pkg/sipper"
	"log"
	"time"
)

func (c *ConsumerManager) manageConsumers() {
	for {
		select {

		// Since ticker may call in instant mode, the WebhookStore should be locally cached!
		case t := <-c.ticker.C:
			log.Printf("update webhook list at %s\n", t.Format(time.RFC3339Nano))
			outGetWebhook, err := c.Config.WebhookStore.GetWebhooks(context.Background())
			if err != nil {
				log.Printf("update webhook list error %s at %s\n", err, t.Format(time.RFC3339Nano))
				continue
			}

			for outGetWebhook.Next() {
				webhook, _err := outGetWebhook.Webhook()
				if _err != nil {
					log.Printf("getting webhook row error: %s\n", _err)
					continue
				}

				webhookSrc := webhook.WebhookSource

				c.mKafkaConsGroup.RLock()
				_, consumerGroupExist := c.kafkaConsGroup[webhook.Label]
				if consumerGroupExist {
					log.Printf("already registered webhook %s to kafka %s\n", webhook.Label, webhookSrc.KafkaConfigLabel)

					c.mKafkaConsGroup.RUnlock()
					continue
				}

				_, consumerGroupPausedExist := c.kafkaPausedConsGroup[webhook.Label]
				if consumerGroupPausedExist {
					log.Printf("paused webhook %s to kafka %s already registered\n", webhook.Label, webhookSrc.KafkaConfigLabel)

					c.mKafkaConsGroup.RUnlock()
					continue
				}
				c.mKafkaConsGroup.RUnlock()

				log.Printf("add webhook %s to kafka %s\n", webhook.Label, webhookSrc.KafkaConfigLabel)
				client, err := c.Config.KafkaClientManager.GetConn(context.Background(), webhookSrc.KafkaConfigLabel)
				if err != nil {
					log.Printf(
						"get connection for webhhok '%s' kafka label '%s': %s\n",
						webhook.Label, webhookSrc.KafkaConfigLabel, err,
					)
					continue
				}

				if client.Closed() {
					// TODO: if current client is closed, delete all active webhook consumer related to this client.
					continue
				}

				consumerGroup := fmt.Sprintf("consumer-%s", webhook.Label)
				kafkaConsumerGroup, err := sarama.NewConsumerGroupFromClient(consumerGroup, client)
				if err != nil {
					log.Printf(
						"setup consumer group webhhok '%s' kafka label '%s': %s\n",
						webhook.Label, webhookSrc.KafkaConfigLabel, err,
					)
					continue
				}

				log.Println("[start] register webhook consumer", webhook.Label)

				// isConnUpOrErr will wait either nil error (come from Setup consumer handler)
				// or error from sarama.ConsumerGroupHandler.Consume inside function InitConsumer
				isConnUpOrErr := make(chan error, 1)
				processor := sipper.NewCloudEventSink(webhook, isConnUpOrErr, client)
				sipper.InitConsumer(context.Background(), &sipper.InputInitConsumer{
					ConsumerGroup: kafkaConsumerGroup,
					Topic:         webhookSrc.KafkaTopic,
					Processor:     processor,
					ErrorChan:     isConnUpOrErr,
				})

				isUp := <-isConnUpOrErr

				log.Println("[done] register webhook consumer", webhook.Label)
				if isUp == nil {
					log.Printf("add webhook %s to kafka %s success\n",
						webhook.Label,
						webhookSrc.KafkaConfigLabel,
					)

					c.mKafkaConsGroup.Lock()
					c.kafkaConsGroup[webhook.Label] = kafkaConsumerGroup
					c.mKafkaConsGroup.Unlock()
				}

				continue
			}
		}
	}
}
