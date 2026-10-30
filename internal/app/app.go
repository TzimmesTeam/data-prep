package app

import (
	"context"
	"log/slog"

	httpapp "github.com/TzimmesTeam/data-prep/internal/app/http"
	"github.com/TzimmesTeam/data-prep/internal/config"
	prephandler "github.com/TzimmesTeam/data-prep/internal/handler/prep"
	prepservice "github.com/TzimmesTeam/data-prep/internal/service/prep"
)

type App struct {
	HTTP *httpapp.App
	log  *slog.Logger
}

func New(cfg *config.Config, log *slog.Logger) (*App, error) {
	svc := prepservice.New(cfg.Batch.PartitionSize, cfg.Batch.MaxSize, log)
	handler := prephandler.NewHandler(svc)
	httpServer := httpapp.New(cfg.HTTP.Port, handler, log)

	return &App{
		HTTP: httpServer,
		log:  log,
	}, nil
}

func (a *App) Run() error {
	a.log.Info("starting application")
	return a.HTTP.Run()
}

func (a *App) Stop(ctx context.Context) error {
	a.log.Info("stopping application")
	return a.HTTP.Stop(ctx)
}
