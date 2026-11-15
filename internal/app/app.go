package app

import (
	"context"
	"errors"
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

	if err := cfg.Validate(); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}

	entry := logrus.NewEntry(log)
	startedAt := time.Now()

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
	services := service.NewServices(repos, entry)

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

	orderEvents := service.NewOrderEventConsumer(services.Order, entry)

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

	outboxPoller := service.NewOutboxPoller(repos.Outbox, producer, entry)
	outboxPoller.Start(ctx)

	handlerChain := delivery.NewRouter(delivery.NewHandlers(services), delivery.Options{
		AllowedOrigins: cfg.AllowedOrigins,
		Monitoring: delivery.MonitoringDeps{
			Service:  "order-service",
			DB:       dbPool,
			Consumer: consumer,
			Producer: producer,
			Outbox:   outboxPoller,
			Log:      entry,
			Started:  startedAt,
		},
	})

	svr, err := server.NewServer(cfg, handlerChain)
	if err != nil {
		log.Fatalf("Unable to init svr: %v", err)
	}

	serverErr := make(chan error, 1)
	go func() {
		log.Info("Successfully started server on port: ", cfg.Port)
		serverErr <- svr.Run()
	}()

	select {
	case <-ctx.Done():
		log.Info("Shutting down signal received...")
	case err := <-serverErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.WithError(err).Error("HTTP server stopped unexpectedly")
		}
	}

	// Stop the background loops before the resources they use are closed.
	// cancel() also covers the "server failed" path, where the signal context
	// is still alive.
	cancel()

	// 1. Stop accepting new requests and let in-flight ones finish. A fresh
	// context is required: the signal context is already cancelled, so passing
	// it here would abort the drain immediately.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	if err := svr.Shutdown(shutdownCtx); err != nil {
		log.WithError(err).Error("graceful HTTP shutdown failed")
	}

	// 2. Let the poller finish the batch it is currently publishing.
	outboxPoller.Wait()

	// 3. Flush the producer, then stop consuming.
	if err := producer.Close(); err != nil {
		log.WithError(err).Error("failed to close Kafka producer")
	}
	if err := consumer.Close(); err != nil {
		log.WithError(err).Error("failed to close Kafka consumer")
	}

	log.Info("shutdown complete")
}
