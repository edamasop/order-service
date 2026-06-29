package http

import (
	"net/http"
	"order-service/internal/delivery/http/v1"
)

func NewRouter(hls *Handlers) http.Handler {
	mux := http.NewServeMux()
	v1.RegisterOrderRoutes(mux, hls.OrderHandler)
	return mux
}
