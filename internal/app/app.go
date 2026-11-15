package app

import (
	"context"
	"net/http"
	"order-service/internal/repository"
	"order-service/internal/server"
	"order-service/internal/service"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"order-service/internal/config"
	delivery "order-service/internal/delivery/http"

	"github.com/edamasop/events"
	"github.com/edamasop/messaging"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
)

func Run() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg := config.Load()

	log := logrus.New()
	log.SetOutput(os.Stdout)

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

	repos := repository.NewRepositories(dbPool)
	services := service.NewServices(repos, logrus.NewEntry(log))

	// 2. Consume the events that move an order forward. The order service is
	// the read side of the saga: it reacts to payments and shipments instead of
	// publishing to itself.
	consumer, err := messaging.NewKafkaConsumer(messaging.ConsumerConfig{
		BootstrapServers: strings.Join(cfg.KafkaBrokers, ","),
		GroupID:          cfg.KafkaGroupID,
		Topics:           cfg.KafkaConsumerTopics,
		EnableLogging:    true,
		LogOutput:        os.Stdout,
		ManualCommit:     true,
		MaxRetries:       3,
		ErrorBackoff:     2 * time.Second,
		ReadTimeout:      5 * time.Second,

		EnableDLQ: true,
		// One DLQ per service: with more than one consumed topic a per-topic
		// DLQ would make it harder to see which stream produced a poison
		// message.
		DLQTopic: cfg.KafkaProducerTopic + ".dlq",

		HealthCheckInterval:       15 * time.Second,
		UnhealthyFailureThreshold: 5,
		HealthFailureWindow:       1 * time.Minute,
	})
	if err != nil {
		log.Fatalf("Failed to initialize Kafka consumer: %v", err)
	}

	orderEvents := service.NewOrderEventConsumer(services.Order, logrus.NewEntry(log))

	consumer.RegisterHandler(
		string(events.PaymentStatusSuccessful),
		orderEvents.HandlePaymentSuccessful,
	)
	// There is no dedicated "delivered" event: delivery is reported as
	// shipment.updated with Status == "DELIVERED", so the handler filters by
	// status and ignores the "shipping" update.
	consumer.RegisterHandler(
		string(events.ShipmentUpdated),
		orderEvents.HandleShipmentDelivered,
	)

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

	// 3. Publish order events through the transactional outbox.
	producer, err := messaging.NewKafkaProducer(messaging.ProducerConfig{
		BootstrapServers:       strings.Join(cfg.KafkaBrokers, ","),
		Topic:                  cfg.KafkaProducerTopic,
		EnableLogging:          true,
		LogOutput:              os.Stdout,
		ErrOutput:              os.Stderr,
		MaxAttempts:            3,
		AllowAutoTopicCreation: true,
	})
	if err != nil {
		log.Fatalf("Failed to initialize Kafka producer: %v", err)
	}
	log.Info("Successfully connected to Kafka producer")

	handlers := delivery.NewHandlers(services)
	router := delivery.NewRouter(handlers)

	outboxPoller := service.NewOutboxPoller(repos.Outbox, producer, logrus.NewEntry(log))
	outboxPoller.Start(ctx)

	svr, err := server.NewServer(cfg, router)
	if err != nil {
		log.Fatalf("Unable to init svr: %v", err)
	}

	go func() {
		log.Info("Successfully started server on port: ", cfg.Port)
		err = svr.Run()
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("Unable to start svr: %v", err)
		}
	}()

	<-ctx.Done()
	log.Info("Shutting down signal received...")
	defer svr.Shutdown(ctx)
}
