package http

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type App struct {
	server *http.Server
	log    *slog.Logger
}

func New(port int, readiness func(context.Context) bool, log *slog.Logger) *App {
	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	router.GET("/health/ready", func(c *gin.Context) {
		if !readiness(c.Request.Context()) {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ready"})
	})

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
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
