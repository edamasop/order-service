package model

import (
	"time"

	"github.com/shopspring/decimal"
)

type Order struct {
	ID          int64
	OrderNumber string
	CustomerID  int64
	Status      OrderStatus
	TotalAmount decimal.Decimal
	Currency    string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type OrderStatus string

const (
	OrderStatusPending   OrderStatus = "PENDING"
	OrderStatusCreated   OrderStatus = "CREATED"
	OrderStatusPaid      OrderStatus = "PAID"
	OrderStatusCancelled OrderStatus = "CANCELLED"
	OrderStatusCompleted OrderStatus = "COMPLETED"
)

type OutboxEvent struct {
	ID          int64
	AggregateID int64
	EventType   string
	Payload     []byte
	CreatedAt   time.Time
	ProcessedAt *time.Time
	Published   bool
}
