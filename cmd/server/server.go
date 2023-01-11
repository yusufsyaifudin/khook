package server

import (
	"context"
	"fmt"
	"github.com/sony/sonyflake"
	"github.com/yusufsyaifudin/khook/config"
	"github.com/yusufsyaifudin/khook/internal/pkg/kafkaclientmgr"
	"github.com/yusufsyaifudin/khook/internal/pkg/kafkaconsumermgr"
	"github.com/yusufsyaifudin/khook/internal/svc/resourcesvc"
	"github.com/yusufsyaifudin/khook/storage"
	"github.com/yusufsyaifudin/khook/storage/etcd"
	"github.com/yusufsyaifudin/khook/storage/inmem"
	"github.com/yusufsyaifudin/khook/storage/postgres"
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

	cfg := &config.ServerConfig{}
	err := cfg.Load()
	if err != nil {
		err = fmt.Errorf("cannot load config: %w", err)
		return err
	}

	sf := sonyflake.NewSonyflake(sonyflake.Settings{
		StartTime: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
	})

	if sf == nil {
		return fmt.Errorf("sonyflake instance is nil, so we cannot generate uid in our system")
	}

	if _, _err := sf.NextID(); _err != nil {
		return fmt.Errorf("we try to generate uid using sonyflake, but it failed: %w", _err)
	}

	var kafkaConnStore storage.KafkaConnStore = inmem.NewKafkaConnStore()
	var kafkaConsumerStore storage.KafkaConsumerStore = inmem.NewKafkaConsumerStore()

	if cfg.Storage.KafkaConnStore.Etcd != nil {
		kafkaConnStore, err = etcd.NewKafkaConnStore(etcd.WithEndpoints(cfg.Storage.KafkaConnStore.Etcd.Endpoints))
		if err != nil {
			err = fmt.Errorf("cannot use etcd for kafka conn store: %w", err)
			return err
		}

		log.Println("kafka connection store using etcd")
	}

	if cfg.Storage.KafkaConnStore.Postgres != nil {
		sqlDB, err := cfg.Storage.KafkaConnStore.Postgres.OpenConn()
		if err != nil {
			err = fmt.Errorf("cannot open db conn for kafka conn store: %w", err)
			return err
		}

		kafkaConnStore, err = postgres.NewKafkaConnStore(postgres.WithDB(sqlDB), postgres.WithSonyFlake(sf))
		if err != nil {
			err = fmt.Errorf("cannot open db conn for kafka conn store: %w", err)
			return err
		}

		log.Println("kafka connection store using postgres")
	}

	// ** Prepare Kafka client connection manager
	var kafkaClientManager kafkaclientmgr.Manager
	kafkaClientManager, err = kafkaclientmgr.NewKafkaClientManager(
		kafkaclientmgr.WithConnStore(kafkaConnStore),
		kafkaclientmgr.WithUpdateConnInterval(10*time.Second),
	)
	if err != nil {
		err = fmt.Errorf("cannot prepare kafka client connection manager: %w", err)
		return err
	}

	var kafkaConsumerManager kafkaconsumermgr.Manager
	kafkaConsumerManager, err = kafkaconsumermgr.NewKafkaConsumerManager(
		kafkaconsumermgr.WithConnStore(kafkaConsumerStore),
		kafkaconsumermgr.WithClientConnectionManager(kafkaClientManager),
		kafkaconsumermgr.WithUpdateInterval(3*time.Second),
	)
	if err != nil {
		err = fmt.Errorf("cannot prepare kafka consumer manager: %w", err)
		return err
	}

	resourceSvc, err := resourcesvc.NewResourceService(resourcesvc.ConsumerManagerConfig{
		KafkaConnStore:       kafkaConnStore,
		KafkaConsumerStore:   kafkaConsumerStore,
		KafkaClientManager:   kafkaClientManager,
		KafkaConsumerManager: kafkaConsumerManager,
	})
	if err != nil {
		err = fmt.Errorf("cannot prepare resource service: %w", err)
		return err
	}

	transportHTTP := transport.NewHTTP(transport.HttpCfg{
		ResourceSvc: resourceSvc,
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

		if _err := kafkaConsumerManager.Close(); _err != nil {
			log.Fatalln("error close client kafka", _err)
		}

		if _err := kafkaClientManager.Close(); _err != nil {
			log.Fatalln("error close consumer kafka", _err)
		}

	case _err := <-apiErrChan:
		if _err != nil {
			log.Fatalln("http transport", _err)
		}
	}

	return nil
}
