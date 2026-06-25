package schema

import (
	"order-service/internal/model"

	"github.com/shopspring/decimal"
)

type OrderCreate struct {
	CustomerID  int64           `json:"customer_id"`
	Currency    string          `json:"currency"`
	TotalAmount decimal.Decimal `json:"total_amount"`
	OrderNumber string          `json:"order_number"`
}

type OrderUpdate struct {
	Status      model.OrderStatus `json:"status"`
	Currency    string            `json:"currency"`
	TotalAmount decimal.Decimal   `json:"total_amount"`
	OrderNumber string            `json:"order_number"`
}

type OrderResponse struct {
	ID          int64             `json:"id"`
	CustomerID  int64             `json:"customer_id"`
	OrderNumber string            `json:"order_number"`
	Status      model.OrderStatus `json:"status"`
	Currency    string            `json:"currency"`
	TotalAmount decimal.Decimal   `json:"total_amount"`
}
