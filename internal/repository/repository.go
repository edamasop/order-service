package repository

import (
	"context"
	"order-service/internal/model"
	"order-service/internal/repository/postgres"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Order interface {
	Create(ctx context.Context, order *model.Order) error
	GetByID(ctx context.Context, id int64) (*model.Order, error)
	Update(ctx context.Context, order *model.Order) error
	Delete(ctx context.Context, id int64) error
}

type Outbox interface {
	Create(ctx context.Context, event *model.OutboxEvent) error
	GetByID(ctx context.Context, id int64) (*model.OutboxEvent, error)
	Update(ctx context.Context, event *model.OutboxEvent) error
	Delete(ctx context.Context, id int64) error

	GetUnpublished(ctx context.Context, limit int) ([]*model.OutboxEvent, error)
	MarkPublished(ctx context.Context, id int64) error
}

type Repositories struct {
	TxManager TxManager
	Order     Order
	Outbox    Outbox
}

func NewRepository(db *pgxpool.Pool) *Repositories {
	return &Repositories{
		TxManager: postgres.NewTxManager(db),
		Order:     postgres.NewOrderRepository(db),
		Outbox:    postgres.NewOutboxRepository(db),
	}
}
