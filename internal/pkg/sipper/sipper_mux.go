package sipper

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/yusufsyaifudin/khook/internal/pkg/sipper/sippercloudevents"
	"github.com/yusufsyaifudin/khook/pkg/validator"
	"github.com/yusufsyaifudin/khook/storage"
)

func SelectProcessor(target storage.SinkTarget, chanReadyOrErr chan error) (sarama.ConsumerGroupHandler, error) {
	err := validator.Validate(target)
	if err != nil {
		err = fmt.Errorf("validation error on sink target: %w", err)
		return nil, err
	}

	switch target.Type {
	case "cloudevents":
		return sippercloudevents.NewCloudEventSink(target, chanReadyOrErr), nil
	}

	return nil, fmt.Errorf("unhandled type '%s' on consumer label '%s'", target.Type, target.Label)
}
