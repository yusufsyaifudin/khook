package main

import (
	"context"
	"fmt"
	"github.com/yusufsyaifudin/khook/internal/pkg/kafkamgr"
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

func main() {
	ctx := context.Background()
	inMemKafkaConnStore := inmem.NewKafkaConnStore()
	inMemWebhookStore := inmem.NewWebhookStore()

	consumerKafka, err := kafkamgr.NewKafka(
		kafkamgr.WithConnStore(inMemKafkaConnStore),
		kafkamgr.WithRebuildConnInterval(3*time.Second),
	)
	if err != nil {
		log.Fatalln(err)
		return
	}

	consumerMgr, err := resourcemgr.NewConsumerManager(resourcemgr.ConsumerManagerConfig{
		KafkaConnStore: inMemKafkaConnStore,
		WebhookStore:   inMemWebhookStore,
		Consumer:       consumerKafka,
	})
	if err != nil {
		log.Fatalln(err)
		return
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

	var apiErrChan = make(chan error, 1)
	go func() {
		log.Println("http transport: done running on port", httpPort)
		apiErrChan <- httpServer.ListenAndServe()
	}()

	log.Println("system: up and running...")

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

	case _err := <-apiErrChan:
		if _err != nil {
			log.Fatalln("http transport", _err)
		}
	}
}
