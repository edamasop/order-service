package service

import (
	"context"
	"order-service/internal/repository"

	"order-service/internal/schema"

	"github.com/sirupsen/logrus"
)

type Order interface {
	Create(ctx context.Context, dto *schema.OrderCreate) error
	GetByID(ctx context.Context, id int64) (*schema.OrderResponse, error)
	Update(ctx context.Context, id int64, dto *schema.OrderUpdate) error
	Delete(ctx context.Context, id int64) error
	List(ctx context.Context) ([]schema.OrderResponse, error)

	// MarkPaid and MarkCompleted are driven by Kafka events, not by HTTP.
	MarkPaid(ctx context.Context, orderID int64) error
	MarkCompleted(ctx context.Context, orderID int64) error
}

type Services struct {
	Order Order
}

func NewServices(repos *repository.Repositories, entry *logrus.Entry) *Services {
	return &Services{
		Order: NewOrderService(repos.Order, repos.Outbox, repos.TxManager, entry),
	}
}
