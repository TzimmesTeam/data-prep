package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/TzimmesTeam/data-prep/internal/app"
	"github.com/TzimmesTeam/data-prep/internal/config"
	"github.com/TzimmesTeam/data-prep/pkg/logger"
)

func main() {
	cfg := config.MustLoad()
	log := logger.New(cfg.Env)

	application, err := app.New(cfg, log)
	if err != nil {
		log.Error("failed to init app", "error", err)
		os.Exit(1)
	}

	log.Info("application initialized")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		log.Info("shutdown signal received")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.HTTP.ShutdownTimeout)
		defer cancel()
		if err := application.Stop(shutdownCtx); err != nil {
			log.Error("failed to stop app", "error", err)
		}
	}()

	if err := application.Run(); err != nil {
		log.Error("application run failed", "error", err)
		os.Exit(1)
	}
	log.Info("application stopped")
}
