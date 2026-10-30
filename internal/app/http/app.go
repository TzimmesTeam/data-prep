package http

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	prephandler "github.com/TzimmesTeam/data-prep/internal/handler/prep"
	"github.com/gin-gonic/gin"
)

type App struct {
	server *http.Server
	log    *slog.Logger
}

func New(port int, handler *prephandler.Handler, log *slog.Logger) *App {
	router := gin.New()
	router.Use(gin.Recovery())

	router.POST("/prep", handler.Prep)
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: router,
	}

	return &App{server: server, log: log}
}

func (a *App) Run() error {
	a.log.Info("starting http server", slog.String("addr", a.server.Addr))
	return a.server.ListenAndServe()
}

func (a *App) Stop(ctx context.Context) error {
	a.log.Info("stopping http server")
	return a.server.Shutdown(ctx)
}
