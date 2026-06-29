package http

import (
	"order-service/internal/delivery/http/v1"
	"order-service/internal/service"
)

type Handlers struct {
	OrderHandler *v1.OrderHandler
}

func NewHandlers(services *service.Services) *Handlers {
	oh := v1.NewOrderHandler(services.Order)
	return &Handlers{
		OrderHandler: oh,
	}
}
