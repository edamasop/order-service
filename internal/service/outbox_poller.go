package service

import (
	"context"
	"order-service/internal/repository"
	"time"

	"github.com/edamasop/messaging"
	"github.com/sirupsen/logrus"
)

const batchSize = 20

type OutboxPoller struct {
	producer messaging.Producer
	repo     repository.Outbox
	ticker   *time.Ticker
	log      *logrus.Entry
}

func NewOutboxPoller(repo repository.Outbox, producer messaging.Producer, log *logrus.Entry) *OutboxPoller {
	return &OutboxPoller{
		repo:     repo,
		producer: producer,
		log:      log.WithField("service", "OutboxPoller"),
		ticker:   time.NewTicker(time.Second * 3),
	}
}

func (p *OutboxPoller) Start(ctx context.Context) {
	p.log.WithField("func", "Start").Info("Starting OutboxPoller")
	go func() {
		defer p.ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-p.ticker.C:
				events, err := p.repo.GetUnpublished(ctx, batchSize)
				if err != nil {
					p.log.Warnf("GetUnpublished err: %v", err)
					continue
				}

				for _, event := range events {
					err = p.producer.Produce(ctx, event.EventType, event)
					if err != nil {
						p.log.Errorf("Couldn't published event into producer: %v", err)
						continue
					}

					err = p.repo.MarkPublished(ctx, event.ID)
					if err != nil {
						p.log.Errorf("Couldn't mark published event: %v", err)
					}
				}
			}
		}
	}()
}
