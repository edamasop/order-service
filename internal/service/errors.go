package service

import "errors"

var (
	// ErrInvalidArgument marks a request that can never succeed, so consumers
	// must not retry it.
	ErrInvalidArgument = errors.New("invalid argument")

	// ErrOrderNotFound is returned when an event points at an order this
	// service does not know about.
	ErrOrderNotFound = errors.New("order not found")

	// ErrOrderCancelled means the event would resurrect a cancelled order.
	// Retrying cannot help, so consumers drop it after logging.
	ErrOrderCancelled = errors.New("order is cancelled")

	// ErrInvalidTransition means the requested status change is not allowed
	// from the current state.
	ErrInvalidTransition = errors.New("invalid order status transition")
)
