package v1

import "net/http"

func RegisterOrderRoutes(
	mux *http.ServeMux,
	handler *OrderHandler,
) {
	mux.HandleFunc("POST /v1/orders", handler.Create)

	mux.HandleFunc("GET /v1/orders", handler.List)

	mux.HandleFunc("GET /v1/orders/{id}", handler.GetByID)

	mux.HandleFunc("PUT /v1/orders/{id}", handler.Update)

	mux.HandleFunc("DELETE /v1/orders/{id}", handler.Delete)
}
