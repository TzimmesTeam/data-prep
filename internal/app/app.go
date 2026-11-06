package app

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	httpapp "github.com/TzimmesTeam/data-prep/internal/app/http"
	"github.com/TzimmesTeam/data-prep/internal/config"
	prephandler "github.com/TzimmesTeam/data-prep/internal/handler/prep"
	prepservice "github.com/TzimmesTeam/data-prep/internal/service/prep"
	s3storage "github.com/TzimmesTeam/data-prep/internal/storage/s3"
	"github.com/TzimmesTeam/data-prep/internal/transport/jetstream"
)

type App struct {
	HTTP  *httpapp.App
	Queue *jetstream.Transport
	log   *slog.Logger
}

func New(cfg *config.Config, log *slog.Logger) (*App, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.StartupTimeout)
	defer cancel()
	store, err := s3storage.New(ctx, s3storage.Config{
		Region:          cfg.S3.Region,
		Endpoint:        cfg.S3.Endpoint,
		AccessKeyID:     cfg.S3.AccessKeyID,
		SecretAccessKey: cfg.S3.SecretAccessKey,
		UsePathStyle:    cfg.S3.UsePathStyle,
		Buckets:         cfg.S3.Buckets,
	})
	if err != nil {
		return nil, err
	}
	if err := store.CheckConfiguredBuckets(ctx); err != nil {
		return nil, err
	}

	svc := prepservice.New(store, prephandler.NewParser(log), prepservice.Options{
		MaxRows:          cfg.Batch.MaxRows,
		PartitionRows:    cfg.Batch.PartitionRows,
		PartitionBytes:   cfg.Batch.PartitionBytes,
		RejectedLogLimit: cfg.Batch.RejectedLogLimit,
	}, log)
	queue := jetstream.New(jetstream.Config{
		URL:             cfg.JetStream.URL,
		TasksStream:     cfg.JetStream.TasksStream,
		ResultsStream:   cfg.JetStream.ResultsStream,
		TaskSubject:     cfg.JetStream.TaskSubject,
		ResultSubject:   cfg.JetStream.ResultSubject,
		FailedSubject:   cfg.JetStream.FailedSubject,
		ConsumerDurable: cfg.JetStream.ConsumerDurable,
		Parallelism:     cfg.JetStream.Parallelism,
		DeliveryLimit:   cfg.JetStream.DeliveryLimit,
		DialTimeout:     cfg.JetStream.DialTimeout,
		ReconnectDelay:  cfg.JetStream.ReconnectDelay,
		FetchWait:       cfg.JetStream.FetchWait,
		AckWait:         cfg.JetStream.AckWait,
		TaskTimeout:     cfg.JetStream.TaskTimeout,
	}, svc, log)
	httpServer := httpapp.New(cfg.HTTP.Port, func(ctx context.Context) bool {
		return queue.Ready() && store.CheckConfiguredBuckets(ctx) == nil
	}, log)

	return &App{HTTP: httpServer, Queue: queue, log: log}, nil
}

func (a *App) Run() error {
	a.log.Info("starting application")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queueDone := make(chan error, 1)
	go func() {
		queueDone <- a.Queue.Run(ctx)
	}()

	httpErr := a.HTTP.Run()
	cancel()
	queueErr := <-queueDone
	if httpErr != nil && !errors.Is(httpErr, http.ErrServerClosed) {
		return httpErr
	}
	return queueErr
}

func (a *App) Stop(ctx context.Context) error {
	a.log.Info("stopping application")
	return a.HTTP.Stop(ctx)
}
