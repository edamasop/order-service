package service

import (
	"context"
	"encoding/json"

	"github.com/edamasop/events"
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
