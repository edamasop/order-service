package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/edamasop/events"
	"github.com/jackc/pgx/v5"
	"github.com/sirupsen/logrus"

	"order-service/internal/model"
	"order-service/internal/repository"
	"order-service/internal/schema"
)

type OrderService struct {
	orderRepository  repository.Order
	outboxRepository repository.Outbox
	txManager        repository.TxManager
	log              *logrus.Entry
}

func NewOrderService(
	orderRepository repository.Order,
	outboxRepository repository.Outbox,
	txManager repository.TxManager,
	log *logrus.Entry,
) *OrderService {
	return &OrderService{
		orderRepository:  orderRepository,
		outboxRepository: outboxRepository,
		txManager:        txManager,
		log:              log,
	}
}

func (s *OrderService) Create(ctx context.Context, dto *schema.OrderCreate) error {
	return s.txManager.WithinTransaction(ctx, func(ctx context.Context) error {
		order := &model.Order{
			CustomerID:  dto.CustomerID,
			OrderNumber: dto.OrderNumber,
			Status:      model.OrderStatusCreated,
			TotalAmount: dto.TotalAmount,
			Currency:    dto.Currency,
		}

		if err := s.orderRepository.Create(ctx, order); err != nil {
			s.log.WithError(err).Error("failed to create order")
			return err
		}

		if err := s.createOutboxEvent(ctx, order, events.OrderCreated); err != nil {
			s.log.WithError(err).Error("failed to create outbox event")
			return err
		}

		s.log.WithField("order_id", order.ID).Info("order created")

		return nil
	})
}

func (s *OrderService) GetByID(ctx context.Context, id int64) (*schema.OrderResponse, error) {
	order, err := s.orderRepository.GetByID(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrOrderNotFound
		}

		s.log.WithError(err).WithField("order_id", id).Error("failed to get order")
		return nil, err
	}

	return &schema.OrderResponse{
		ID:          order.ID,
		CustomerID:  order.CustomerID,
		OrderNumber: order.OrderNumber,
		Status:      order.Status,
		Currency:    order.Currency,
		TotalAmount: order.TotalAmount,
	}, nil
}

func (s *OrderService) Update(ctx context.Context, id int64, dto *schema.OrderUpdate) error {
	return s.txManager.WithinTransaction(ctx, func(ctx context.Context) error {
		order, err := s.orderRepository.GetByID(ctx, id)
		if err != nil {
			s.log.WithError(err).WithField("order_id", id).Error("failed to get order")
			return err
		}

		order.OrderNumber = dto.OrderNumber
		order.Status = dto.Status
		order.TotalAmount = dto.TotalAmount
		order.Currency = dto.Currency

		if err := s.orderRepository.Update(ctx, order); err != nil {
			s.log.WithError(err).Error("failed to update order")
			return err
		}

		if err := s.createOutboxEvent(ctx, order, events.OrderUpdated); err != nil {
			s.log.WithError(err).Error("failed to create outbox event")
			return err
		}

		s.log.WithField("order_id", order.ID).Info("order updated")

		return nil
	})
}

func (s *OrderService) Delete(ctx context.Context, id int64) error {
	return s.txManager.WithinTransaction(ctx, func(ctx context.Context) error {
		order, err := s.orderRepository.GetByID(ctx, id)
		if err != nil {
			s.log.WithError(err).WithField("order_id", id).Error("failed to get order")
			return err
		}

		if err := s.orderRepository.Delete(ctx, id); err != nil {
			s.log.WithError(err).Error("failed to delete order")
			return err
		}

		if err := s.createOutboxEvent(ctx, order, events.OrderDeleted); err != nil {
			s.log.WithError(err).Error("failed to create outbox event")
			return err
		}

		s.log.WithField("order_id", order.ID).Info("order deleted")

		return nil
	})
}

func (s *OrderService) List(ctx context.Context) ([]schema.OrderResponse, error) {
	//TODO implement me
	s.log.WithField("func", "List").Trace("implement me")
	return []schema.OrderResponse{}, nil
}

// MarkPaid applies the payment.successful event. It is idempotent: an order
// that is already paid or completed is left untouched, so a redelivered event
// neither changes the state nor emits a second outbox event.
func (s *OrderService) MarkPaid(ctx context.Context, orderID int64) error {
	return s.applyStatusTransition(ctx, orderID, events.OrderPaid, func(order *model.Order) (bool, error) {
		switch order.Status {
		case model.OrderStatusPaid, model.OrderStatusCompleted:
			// Already applied, or already further along: never move backwards.
			return false, nil
		case model.OrderStatusCancelled:
			return false, fmt.Errorf("%w: order %d", ErrOrderCancelled, orderID)
		}

		order.Status = model.OrderStatusPaid
		return true, nil
	})
}

// MarkCompleted applies the shipment.delivered event.
//
// A payment and a shipment event travel through different topics, so their
// relative order is not guaranteed: delivery may be consumed before the
// payment event. A delivered order is therefore allowed to go straight to
// COMPLETED instead of being stuck waiting for the payment event.
func (s *OrderService) MarkCompleted(ctx context.Context, orderID int64) error {
	return s.applyStatusTransition(ctx, orderID, events.OrderCompleted, func(order *model.Order) (bool, error) {
		switch order.Status {
		case model.OrderStatusCompleted:
			return false, nil
		case model.OrderStatusCancelled:
			return false, fmt.Errorf("%w: order %d", ErrOrderCancelled, orderID)
		}

		order.Status = model.OrderStatusCompleted
		return true, nil
	})
}

// applyStatusTransition serializes concurrent updates of one order with
// SELECT ... FOR UPDATE, then persists the new state and its outbox event in
// the same transaction.
func (s *OrderService) applyStatusTransition(
	ctx context.Context,
	orderID int64,
	eventType events.EventType,
	apply func(order *model.Order) (changed bool, err error),
) error {
	if orderID <= 0 {
		return fmt.Errorf("%w: order id must be positive", ErrInvalidArgument)
	}

	return s.txManager.WithinTransaction(ctx, func(ctx context.Context) error {
		order, err := s.orderRepository.GetByIDForUpdate(ctx, orderID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrOrderNotFound
			}

			return fmt.Errorf("get order %d for update: %w", orderID, err)
		}

		changed, err := apply(order)
		if err != nil {
			return err
		}
		if !changed {
			s.log.WithField("order_id", orderID).Debug("order event already applied; nothing to do")
			return nil
		}

		if err := s.orderRepository.Update(ctx, order); err != nil {
			return fmt.Errorf("update order %d: %w", orderID, err)
		}

		if err := s.createOutboxEvent(ctx, order, eventType); err != nil {
			return err
		}

		s.log.WithFields(logrus.Fields{
			"order_id": orderID,
			"status":   order.Status,
		}).Info("order status changed")

		return nil
	})
}

func (s *OrderService) createOutboxEvent(
	ctx context.Context,
	order *model.Order,
	eventType events.EventType,
) error {
	payload, err := json.Marshal(events.OrderPayload{
		ID:          order.ID,
		OrderNumber: order.OrderNumber,
		CustomerID:  order.CustomerID,
		Status:      string(order.Status),
		TotalAmount: order.TotalAmount,
		Currency:    order.Currency,
	})
	if err != nil {
		return err
	}

	event := &model.OutboxEvent{
		AggregateID: order.ID,
		EventType:   string(eventType),
		Payload:     payload,
		Published:   false,
	}

	return s.outboxRepository.Create(ctx, event)
}
