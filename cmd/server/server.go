package server

import (
	"context"
	"fmt"
	"github.com/yusufsyaifudin/khook/internal/pkg/kafkaclientmgr"
	"github.com/yusufsyaifudin/khook/internal/svc/resourcemgr"
	"github.com/yusufsyaifudin/khook/storage/inmem"
	"github.com/yusufsyaifudin/khook/transport"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Server struct{}

func (s *Server) Run() error {
	ctx := context.Background()
	inMemKafkaConnStore := inmem.NewKafkaConnStore()
	inMemWebhookStore := inmem.NewWebhookStore()

	consumerKafka, err := kafkaclientmgr.NewKafkaClientManager(
		kafkaclientmgr.WithConnStore(inMemKafkaConnStore),
		kafkaclientmgr.WithRefreshConnInterval(3*time.Second),
		kafkaclientmgr.WithUpdateConnInterval(10*time.Second),
	)
	if err != nil {
		err = fmt.Errorf("cannot prepare kafka client connection manager: %w", err)
		return err
	}

	consumerMgr, err := resourcemgr.NewConsumerManager(resourcemgr.ConsumerManagerConfig{
		KafkaConnStore:     inMemKafkaConnStore,
		WebhookStore:       inMemWebhookStore,
		KafkaClientManager: consumerKafka,
	})
	if err != nil {
		err = fmt.Errorf("cannot prepare webhook consumer manager: %w", err)
		return err
	}

	transportHTTP := transport.NewHTTP(transport.HttpCfg{
		ConsumerManager: consumerMgr,
	})

	httpPort := fmt.Sprintf(":%d", 3333)
	h2s := &http2.Server{}
	httpServer := &http.Server{
		Addr:    httpPort,
		Handler: h2c.NewHandler(transportHTTP, h2s), // HTTP/2 Cleartext handler
	}

	log.Println("system: up and running...")

	var apiErrChan = make(chan error, 1)
	go func() {
		log.Println("http transport: done running on port", httpPort)
		apiErrChan <- httpServer.ListenAndServe()
	}()

	// ** listen for sigterm signal
	var signalChan = make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	select {
	case <-signalChan:
		log.Println("system: exiting...")
		log.Println("http transport: exiting...")
		if _err := httpServer.Shutdown(ctx); _err != nil {
			log.Println("http transport", _err)
		}

		if _err := consumerMgr.Close(); _err != nil {
			log.Fatalln("error close client kafka", _err)
		}

		if _err := consumerKafka.Close(); _err != nil {
			log.Fatalln("error close consumer kafka", _err)
		}

	case _err := <-apiErrChan:
		if _err != nil {
			log.Fatalln("http transport", _err)
		}
	}

	return nil
}
