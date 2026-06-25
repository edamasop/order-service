package server

import (
	"context"
	"fmt"
	"net/http"
	"order-service/internal/config"
	"time"
)

type Server struct {
	server *http.Server
}

func NewServer(cfg *config.Config, handler *handler.Handler) (*Server, error) {
	httpHandler, err := handler.Init(cfg)
	if err != nil {
		return nil, err
	}

	return &Server{
		server: &http.Server{
			Addr:           fmt.Sprintf(":%s", cfg.Port),
			Handler:        httpHandler,
			ReadTimeout:    30 * time.Second,
			WriteTimeout:   60 * time.Second,
			MaxHeaderBytes: 2 << 20, // 2MB
		},
	}, nil
}

func (s *Server) Run() error {
	return s.server.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}
