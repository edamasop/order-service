package v1

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"order-service/internal/model"
	"order-service/internal/schema"
)

type OrderService interface {
	Create(ctx context.Context, dto *schema.OrderCreate) error
	GetByID(ctx context.Context, id int64) (*schema.OrderResponse, error)
	Update(ctx context.Context, id int64, dto *schema.OrderUpdate) error
	Delete(ctx context.Context, id int64) error
	List(ctx context.Context) ([]schema.OrderResponse, error)
}

type OrderHandler struct {
	service OrderService
}

func NewOrderHandler(service OrderService) *OrderHandler {
	return &OrderHandler{
		service: service,
	}
}

func (h *OrderHandler) Create(w http.ResponseWriter, r *http.Request) {
	var dto schema.OrderCreate

	if err := json.NewDecoder(r.Body).Decode(&dto); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	err := h.service.Create(r.Context(), &dto)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusCreated, "OrderHandler successfully created")
}

func (h *OrderHandler) GetByID(w http.ResponseWriter, r *http.Request) {
	id, err := orderID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	order, err := h.service.GetByID(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	writeJSON(w, http.StatusOK, order)
}

func (h *OrderHandler) List(w http.ResponseWriter, r *http.Request) {
	orders, err := h.service.List(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, orders)
}

func (h *OrderHandler) Update(w http.ResponseWriter, r *http.Request) {
	id, err := orderID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var dto schema.OrderUpdate

	if err := json.NewDecoder(r.Body).Decode(&dto); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	err = h.service.Update(r.Context(), id, &dto)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, "OrderHandler successfully updated")
}

func (h *OrderHandler) Delete(w http.ResponseWriter, r *http.Request) {
	id, err := orderID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := h.service.Delete(r.Context(), id); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func orderID(r *http.Request) (int64, error) {
	id := r.PathValue("id")
	if id == "" {
		return 0, errors.New("id is required")
	}

	return strconv.ParseInt(id, 10, 64)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

var _ model.OrderStatus
