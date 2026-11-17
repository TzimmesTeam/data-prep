package app

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	httpapp "github.com/TzimmesTeam/data-prep/internal/app/http"
	"github.com/TzimmesTeam/data-prep/internal/client/controlplane"
	"github.com/TzimmesTeam/data-prep/internal/config"
	prephandler "github.com/TzimmesTeam/data-prep/internal/handler/prep"
	prepservice "github.com/TzimmesTeam/data-prep/internal/service/prep"
	s3storage "github.com/TzimmesTeam/data-prep/internal/storage/s3"
	sqstransport "github.com/TzimmesTeam/data-prep/internal/transport/sqs"
)

type App struct {
	HTTP  *httpapp.App
	Queue *sqstransport.Transport
	log   *slog.Logger
}

func New(cfg *config.Config, log *slog.Logger) (*App, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.StartupTimeout)
	defer cancel()
	store, err := s3storage.New(ctx, &s3storage.Config{
		Region:          cfg.S3.Region,
		Endpoint:        cfg.S3.Endpoint,
		AccessKeyID:     cfg.S3.AccessKeyID,
		SecretAccessKey: cfg.S3.SecretAccessKey,
		UsePathStyle:    cfg.S3.UsePathStyle,
		Buckets:         []string{cfg.S3.RawBucket, cfg.S3.CuratedBucket, cfg.S3.QuarantineBucket},
	})
	if err != nil {
		return nil, err
	}
	if checkErr := store.CheckConfiguredBuckets(ctx); checkErr != nil {
		return nil, checkErr
	}

	svc := prepservice.New(store, prephandler.NewParser(log), prepservice.Buckets{
		Curated:    cfg.S3.CuratedBucket,
		Quarantine: cfg.S3.QuarantineBucket,
	}, log)
	notifier := controlplane.New(controlplane.Config{
		URL:         cfg.ControlPlane.URL,
		Timeout:     cfg.ControlPlane.Timeout,
		MaxAttempts: cfg.ControlPlane.MaxAttempts,
		RetryDelay:  cfg.ControlPlane.RetryDelay,
	})
	queue, err := sqstransport.New(&sqstransport.Config{
		Region:            cfg.SQS.Region,
		Endpoint:          cfg.SQS.Endpoint,
		QueueURL:          cfg.SQS.QueueURL,
		AccessKeyID:       cfg.SQS.AccessKeyID,
		SecretAccessKey:   cfg.SQS.SecretAccessKey,
		RawBucket:         cfg.S3.RawBucket,
		WaitTimeSeconds:   cfg.SQS.WaitTimeSeconds,
		VisibilityTimeout: cfg.SQS.VisibilityTimeout,
		ReconnectDelay:    cfg.SQS.ReconnectDelay,
	}, svc, notifier, log)
	if err != nil {
		return nil, err
	}
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
