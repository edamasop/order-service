package http

import (
	"net/http"

	"order-service/internal/delivery/http/v1"
)

// Options carries everything the HTTP layer needs besides the route handlers.
type Options struct {
	AllowedOrigins []string
	Monitoring     MonitoringDeps
}

func NewRouter(hls *Handlers, opts Options) http.Handler {
	mux := http.NewServeMux()
	v1.RegisterOrderRoutes(mux, hls.OrderHandler)

	httpStats := newHTTPMetrics()
	registerMonitoring(mux, opts.Monitoring, httpStats)

	// Outermost first: a panic anywhere below is recovered, every response
	// carries a request id, and every request is logged and counted.
	return chain(
		mux,
		withRecovery(opts.Monitoring.Log),
		withRequestID,
		withCORS(opts.AllowedOrigins),
		withAccessLog(opts.Monitoring.Log),
		withMetrics(httpStats),
	)
}

func chain(handler http.Handler, middlewares ...func(http.Handler) http.Handler) http.Handler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		handler = middlewares[i](handler)
	}

	return handler
}
