package app

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"time"

	"order-service/internal/config"

	"github.com/edamasop/messaging"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
)

func Run() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := config.Load()

	// 1. Initialize PostgreSQL
	dbPool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("Unable to connect to PostgreSQL: %v", err)
	}
	defer dbPool.Close()

	if err := dbPool.Ping(ctx); err != nil {
		log.Fatalf("PostgreSQL ping failed: %v", err)
	}
	log.Info("Successfully connected to PostgreSQL")

	consumerCfg := messaging.ConsumerConfig{

		BootstrapServers: strings.Join(cfg.KafkaBrokers, ","),
		GroupID:          cfg.KafkaGroupID,
		Topics:           []string{cfg.KafkaTopic},
		EnableLogging:    true,
		LogOutput:        os.Stdout,
		ManualCommit:     true,
		MaxRetries:       3,
		ErrorBackoff:     2 * time.Second,
		ReadTimeout:      5 * time.Second,

		EnableDLQ: true,
		DLQTopic:  cfg.KafkaTopic + ".dlq",

		HealthCheckInterval:       15 * time.Second,
		UnhealthyFailureThreshold: 5,
		HealthFailureWindow:       1 * time.Minute,
	}

	consumer, err := messaging.NewKafkaConsumer(consumerCfg)
	if err != nil {
		log.Fatalf("Failed to initialize Kafka consumer: %v", err)
	}

	consumer.RegisterHandler("OrderCreated", func(data json.RawMessage) (bool, error) {
		log.Infof("Processing OrderCreated event raw payload: %string", string(data))
		return false, nil
	})

	go func() {
		log.Info("Starting message consumption engine...")
		consumer.Start(ctx)
	}()

	defer func() {
		log.Info("Cleaning up resources and shutting down messaging nodes...")
		if err := consumer.Close(); err != nil {
			log.Errorf("Error while stopping consumer gracefully: %v", err)
		}
	}()

	select {}
}
