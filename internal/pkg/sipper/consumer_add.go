package sipper

import (
	"context"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"time"
)

type InputInitConsumer struct {
	ConsumerGroup sarama.ConsumerGroup
	Topic         string
	Processor     sarama.ConsumerGroupHandler
}

func InitConsumer(ctx context.Context, isErrConsume chan error, in *InputInitConsumer) {
	go func() {
		sessionCtx, sessionCtxCancel := context.WithCancel(ctx)

		for {
			go handleConsumerErr(ctx, sessionCtx, sessionCtxCancel, in.ConsumerGroup)

			// `Consume` should be called inside an infinite loop, when a
			// server-side re-balance happens, the consumer session will need to be
			// recreated to get the new claims
			err := in.ConsumerGroup.Consume(sessionCtx, []string{in.Topic}, in.Processor)
			sessionCtxCancel()

			if err != nil {
				log.Printf("Error from consumer: %v\n", err)
				isErrConsume <- err
				return
			}

			select {
			case <-time.After(5 * time.Second):
				log.Println("CloudEventSink group returned, Rebalancing.")
			case <-ctx.Done():
				isErrConsume <- fmt.Errorf("CloudEventSink group cancelled. Stopping")
				log.Println("CloudEventSink group cancelled. Stopping")
				return
			}
		}
	}()

}

func handleConsumerErr(ctx, sessionCtx context.Context, sessionCtxCancel context.CancelFunc, consumerGroup sarama.ConsumerGroup) {
	errs := consumerGroup.Errors()

	for {
		select {
		case <-ctx.Done():
			return
		case <-sessionCtx.Done():
			return
		case err, ok := <-errs:
			if !ok {
				return
			}

			var errProc *sarama.ConsumerError
			if errors.As(err, &errProc) {
				log.Printf("error processing message (non-transient), shutting down processor: %v\n", err)
				sessionCtxCancel()
			}

			if err != nil {
				log.Printf("error during execution of consumer group: %v\n", err)
				sessionCtxCancel()
			}

		}
	}
}
