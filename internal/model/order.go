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

// OutboxEvent mirrors a single orders_outbox row.
//
// The JSON tags are not used for the Kafka message itself (the poller publishes
// Payload verbatim), but they keep logs and any ad-hoc JSON dump readable
// instead of exposing Go field names.
type OutboxEvent struct {
	ID          int64      `json:"id"`
	AggregateID int64      `json:"aggregate_id"`
	EventType   string     `json:"event_type"`
	Payload     []byte     `json:"payload"`
	CreatedAt   time.Time  `json:"created_at"`
	ProcessedAt *time.Time `json:"processed_at,omitempty"`
	LockedUntil *time.Time `json:"locked_until,omitempty"`
	Attempts    int64      `json:"attempts"`
	Published   bool       `json:"published"`
	// Parked marks an event that exhausted its publish attempts and will no
	// longer be claimed.
	Parked bool `json:"parked"`
	// LastError keeps the reason an event was parked, for manual inspection.
	LastError *string `json:"last_error,omitempty"`
}
