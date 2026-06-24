package repository

import (
	"context"
	"order-service/internal/model"
)

type OrderRepository interface {
	Create(ctx context.Context, order *model.Order) error
	GetByID(ctx context.Context, id int64) (*model.Order, error)
	Update(ctx context.Context, order *model.Order) error
	Delete(ctx context.Context, id int64) error
}

type OutboxRepository interface {
	Create(ctx context.Context, event *model.OutboxEvent) error
	GetByID(ctx context.Context, id int64) (*model.OutboxEvent, error)
	Update(ctx context.Context, event *model.OutboxEvent) error
	Delete(ctx context.Context, id int64) error
}
