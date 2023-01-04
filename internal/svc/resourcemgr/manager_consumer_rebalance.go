package resourcemgr

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/yusufsyaifudin/khook/internal/pkg/sipper"
	"github.com/yusufsyaifudin/khook/storage"
	"log"
	"time"
)

func (c *ConsumerManager) manageConsumers() {
	for {
		select {

		// Since ticker may called in instant mode, the WebhookStore should be locally cached!
		case t := <-c.ticker.C:
			log.Printf("update webhook list at %s\n", t.Format(time.RFC3339Nano))
			outGetWebhook, err := c.Config.WebhookStore.GetWebhooks(context.Background(), storage.InputGetWebhooks{})
			if err != nil {
				log.Printf("update webhook list error %s at %s\n", err, t.Format(time.RFC3339Nano))
				continue
			}

			for outGetWebhook.Next() {
				webhook := outGetWebhook.Webhook()
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
				client, err := c.Config.Consumer.GetConn(context.Background(), webhookSrc.KafkaConfigLabel)
				if err != nil {
					log.Printf(
						"get connection for webhhok '%s' kafka label '%s': %s\n",
						webhook.Label, webhookSrc.KafkaConfigLabel, err,
					)
					continue
				}

				if client.Closed() {
					// TODO: if current client is closed, delete all active webhook consumer related to this client.
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
				consumerUp := make(chan bool, 1)
				processor := sipper.NewCloudEventSink(webhook, consumerUp, client)

				sipper.InitConsumer(context.Background(), &sipper.InputInitConsumer{
					ConsumerGroup: kafkaConsumerGroup,
					Topic:         webhookSrc.KafkaTopic,
					Processor:     processor,
				})

				<-consumerUp // ensure that the consumer is up
				log.Println("[done] register webhook consumer", webhook.Label)

				log.Printf("add webhook %s to kafka %s success\n",
					webhook.Label,
					webhookSrc.KafkaConfigLabel,
				)

				c.mKafkaConsGroup.Lock()
				c.kafkaConsGroup[webhook.Label] = kafkaConsumerGroup
				c.mKafkaConsGroup.Unlock()

				continue
			}
		}
	}
}
