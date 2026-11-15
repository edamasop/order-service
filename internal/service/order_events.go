package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/edamasop/events"
	"github.com/sirupsen/logrus"
)

// orderEventTimeout bounds one state change triggered by a Kafka event. The
// handler signature has no context of its own, so this is where a stuck
// database call is cut off.
const orderEventTimeout = 15 * time.Second

// OrderEventConsumer turns payment and shipment events into order status
// changes. Kafka delivers at least once, therefore every handler must be
// idempotent: a redelivered event must not change the order twice or emit a
// duplicate outbox event.
type OrderEventConsumer struct {
	orders Order
	log    *logrus.Entry
}

func NewOrderEventConsumer(orders Order, log *logrus.Entry) *OrderEventConsumer {
	return &OrderEventConsumer{
		orders: orders,
		log:    log.WithField("consumer", "order-events"),
	}
}

// HandlePaymentSuccessful moves the order to PAID.
func (c *OrderEventConsumer) HandlePaymentSuccessful(data json.RawMessage) (bool, error) {
	var payment events.PaymentPayload
	if err := json.Unmarshal(data, &payment); err != nil {
		c.log.WithError(err).Warn("invalid payment.successful payload")
		return false, fmt.Errorf("decode payment.successful payload: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), orderEventTimeout)
	defer cancel()

	if err := c.orders.MarkPaid(ctx, payment.OrderID); err != nil {
		return c.handleError(err, "payment.successful", payment.OrderID)
	}

	c.log.WithFields(logrus.Fields{
		"order_id":   payment.OrderID,
		"payment_id": payment.ID,
	}).Info("order marked as paid")

	return false, nil
}

// shipmentStatusDelivered is the status the shipment service puts into
// ShipmentPayload.Status once the parcel reached the customer.
const shipmentStatusDelivered = "DELIVERED"

// HandleShipmentDelivered moves the order to COMPLETED. It is registered for
// shipment.updated, which is also emitted when delivery starts, so anything
// that is not an actual delivery is acknowledged and ignored.
func (c *OrderEventConsumer) HandleShipmentDelivered(data json.RawMessage) (bool, error) {
	var shipment events.ShipmentPayload
	if err := json.Unmarshal(data, &shipment); err != nil {
		c.log.WithError(err).Warn("invalid shipment.updated payload")
		return false, fmt.Errorf("decode shipment.updated payload: %w", err)
	}

	if shipment.Status != shipmentStatusDelivered {
		c.log.WithFields(logrus.Fields{
			"order_id":    shipment.OrderID,
			"shipment_id": shipment.ID,
			"status":      shipment.Status,
		}).Debug("shipment event is not a delivery; ignored")

		return false, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), orderEventTimeout)
	defer cancel()

	if err := c.orders.MarkCompleted(ctx, shipment.OrderID); err != nil {
		return c.handleError(err, "shipment.delivered", shipment.OrderID)
	}

	c.log.WithFields(logrus.Fields{
		"order_id":    shipment.OrderID,
		"shipment_id": shipment.ID,
	}).Info("order marked as completed")

	return false, nil
}

// handleError decides whether Kafka should retry. Business rejections (unknown
// order, cancelled order, bad payload) can never succeed on a retry, so they
// are logged and acknowledged; everything else is retried and eventually lands
// in the DLQ.
func (c *OrderEventConsumer) handleError(err error, event string, orderID int64) (bool, error) {
	if errors.Is(err, ErrInvalidArgument) ||
		errors.Is(err, ErrOrderNotFound) ||
		errors.Is(err, ErrOrderCancelled) ||
		errors.Is(err, ErrInvalidTransition) {
		c.log.WithError(err).WithFields(logrus.Fields{
			"event":    event,
			"order_id": orderID,
		}).Warn("event cannot be applied to the order; acknowledged without retry")
		return false, err
	}

	c.log.WithError(err).WithFields(logrus.Fields{
		"event":    event,
		"order_id": orderID,
	}).Error("failed to apply order event; will retry")
	return true, err
}
