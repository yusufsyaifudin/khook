package sipper

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/protocol"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/google/uuid"
	"github.com/yusufsyaifudin/khook/storage"
	"log"
	"net/http"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// CloudEventSink represents a Sarama consumer group consumer
type CloudEventSink struct {
	webhookCfg             storage.Webhook
	isUp                   chan bool
	underlyingConn         sarama.Client
	cloudEventHTTPProtocol protocol.Sender
	//cloudEventsClient      cloudeventsClient.Client
}

var _ sarama.ConsumerGroupHandler = (*CloudEventSink)(nil)

func NewCloudEventSink(webhookCfg storage.Webhook, up chan bool, underlyingConn sarama.Client) *CloudEventSink {
	return &CloudEventSink{
		webhookCfg:     webhookCfg,
		isUp:           up,
		underlyingConn: underlyingConn,
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *CloudEventSink) Setup(session sarama.ConsumerGroupSession) error {
	log.Printf("[%s] setup consumer", c.webhookCfg.Label)

	cloudEventHTTPProtocol, err := cloudevents.NewHTTP(
		cloudevents.WithShutdownTimeout(30*time.Second),
		cloudevents.WithHeader("webhook-label", c.webhookCfg.Label),
	)
	if err != nil {
		return fmt.Errorf("cannot prepare cloud event for webhook label '%s': %w", c.webhookCfg.Label, err)
	}

	// Mark the c as ready
	c.cloudEventHTTPProtocol = cloudEventHTTPProtocol
	//c.cloudEventsClient = cloudEventClient
	c.isUp <- true

	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *CloudEventSink) Cleanup(session sarama.ConsumerGroupSession) error {
	log.Printf("[%s] clean up consumer", c.webhookCfg.Label)
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c *CloudEventSink) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case message := <-claim.Messages():
			oriMsg := message // shallow copy

			log.Printf("[%s] Message claimed: value = %s, timestamp = %v, topic = %s", c.webhookCfg.Label, string(oriMsg.Value), oriMsg.Timestamp, oriMsg.Topic)

			// Handle invalid cloud event oriMsg: https://github.com/cloudevents/sdk-go/blob/a7187527ab3278128c1b2a8fe9856d49ecddf25d/samples/kafka/message-handle-non-cloudevents/main.go
			// The logic is borrowing from here https://github.com/cloudevents/sdk-go/blob/a7187527ab3278128c1b2a8fe9856d49ecddf25d/protocol/kafka_sarama/v2/receiver.go#L55-L69
			// This is the release tag for v2.12.0 https://github.com/cloudevents/sdk-go/blob/v2.12.0/protocol/kafka_sarama/v2/receiver.go
			ceSaramaMsg := kafka_sarama.NewMessageFromConsumerMessage(oriMsg)
			cloudEventMsg := binding.UnwrapMessage(ceSaramaMsg)

			// If encoding is unknown, then the inputMessage is a non cloud event, and we need to convert it.
			if cloudEventMsg.ReadEncoding() == binding.EncodingUnknown {
				// We need to get the cloudEventMsg internals
				// Because the oriMsg could be wrapped by the protocol implementation
				// we need to unwrap it and then cast to the oriMsg representation specific to the protocol
				kafkaMessage, ok := binding.UnwrapMessage(cloudEventMsg).(*kafka_sarama.Message)
				if !ok {
					// This should be rare case, or never be triggered if the message is really from Kafka.
					// Since CloudEvent SDK uses Sarama, and this program uses Sarama, the type must equal.
					// But, when it's not, continue with error log.
					err := fmt.Errorf("error while casting the data into type *kafka_sarama.Message")
					if _err := ceSaramaMsg.Finish(err); _err != nil {
						log.Printf("finish cloud event error after casting to sarama.Message: %s\n", _err)
					}

					session.MarkMessage(oriMsg, "")
					session.Commit()
					continue
				}

				// Now let's create a new event
				cloudEvent := cloudevents.NewEvent()
				cloudEvent.SetID(uuid.New().String())
				cloudEvent.SetTime(time.Now())
				cloudEvent.SetType(c.webhookCfg.WebhookSink.CEType)
				cloudEvent.SetSource(oriMsg.Topic) // source containing Kafka topic

				// setting the content type if it blanks
				if kafkaMessage.ContentType == "" {
					var jsonObj interface{}
					if json.Unmarshal(kafkaMessage.Value, &jsonObj) == nil {
						kafkaMessage.ContentType = "application/json"
					} else {
						kafkaMessage.ContentType = "text/plain"
					}
				}

				if err := cloudEvent.SetData(kafkaMessage.ContentType, kafkaMessage.Value); err != nil {
					_err := fmt.Errorf("error while setting the event data: %w", err)
					log.Println(_err)

					if _err2 := ceSaramaMsg.Finish(_err); _err2 != nil {
						log.Printf("finish cloud event error after set data: %s\n", _err2)
					}

					session.MarkMessage(oriMsg, "")
					session.Commit()
					continue
				}

				cloudEventMsg = binding.ToMessage(&cloudEvent)
			}

			// setting the URL and exponential Retry Backoff using context.
			ctx := cloudevents.ContextWithTarget(context.Background(), c.webhookCfg.WebhookSink.URL)
			ctx = cloudevents.ContextWithRetriesExponentialBackoff(ctx, 10*time.Second, 3)

			errSend := c.cloudEventHTTPProtocol.Send(ctx, cloudEventMsg)
			if !cloudevents.IsACK(errSend) {
				// TODO send to DLQ if error occurred during send
			}

			// only to print log
			switch {
			case cloudevents.IsUndelivered(errSend):
				log.Printf("Failed to send: %v\n", errSend)

			case cloudevents.IsACK(errSend):
				log.Printf("Sending success!")

			default:
				var httpResult *cehttp.Result
				if cloudevents.ResultAs(errSend, &httpResult) {
					var err error
					if httpResult.StatusCode != http.StatusOK {
						err = fmt.Errorf(httpResult.Format, httpResult.Args...)
					}
					log.Printf("Sent with status code %d, error: %v\n", httpResult.StatusCode, err)
				}
			}

			log.Println("calling finish")
			if _err := ceSaramaMsg.Finish(errSend); _err != nil {
				log.Printf("finish cloud event error after sending: %s\n", _err)
			}

			// error or not, just commit the message
			session.MarkMessage(oriMsg, "")
			session.Commit()
			continue

		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/Shopify/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}
