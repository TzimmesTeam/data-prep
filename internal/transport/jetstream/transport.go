package jetstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TzimmesTeam/data-prep/internal/domain/models"
	prepservice "github.com/TzimmesTeam/data-prep/internal/service/prep"
	"github.com/nats-io/nats.go"
	js "github.com/nats-io/nats.go/jetstream"
)

type Config struct {
	URL             string
	TasksStream     string
	ResultsStream   string
	TaskSubject     string
	ResultSubject   string
	FailedSubject   string
	ConsumerDurable string
	Parallelism     int
	DeliveryLimit   int
	DialTimeout     time.Duration
	ReconnectDelay  time.Duration
	FetchWait       time.Duration
	AckWait         time.Duration
	TaskTimeout     time.Duration
}

type Transport struct {
	cfg     Config
	service *prepservice.Service
	log     *slog.Logger
	ready   atomic.Bool
	js      js.JetStream
}

func New(cfg Config, service *prepservice.Service, log *slog.Logger) *Transport {
	return &Transport{cfg: cfg, service: service, log: log}
}

func (t *Transport) Ready() bool {
	return t.ready.Load()
}

func (t *Transport) Run(ctx context.Context) error {
	defer t.ready.Store(false)
	for {
		err := t.run(ctx)
		if ctx.Err() != nil {
			return nil
		}
		t.ready.Store(false)
		if err != nil {
			t.log.ErrorContext(ctx, "JetStream connection failed", "error", err)
		}
		timer := time.NewTimer(t.cfg.ReconnectDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

func (t *Transport) run(ctx context.Context) error {
	conn, err := nats.Connect(
		t.cfg.URL,
		nats.Timeout(t.cfg.DialTimeout),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(t.cfg.ReconnectDelay),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			t.ready.Store(false)
			if err != nil {
				t.log.Error("JetStream disconnected", "error", err)
			}
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			t.ready.Store(true)
			t.log.Info("JetStream reconnected")
		}),
	)
	if err != nil {
		return fmt.Errorf("dial NATS: %w", err)
	}
	defer conn.Close()

	t.js, err = js.New(conn)
	if err != nil {
		return fmt.Errorf("create JetStream context: %w", err)
	}
	consumer, err := t.declareTopology(ctx)
	if err != nil {
		return err
	}
	t.ready.Store(true)
	t.log.InfoContext(ctx, "JetStream topology ready")

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 1)
	var workers sync.WaitGroup
	for i := 0; i < t.cfg.Parallelism; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if err := t.consume(workerCtx, consumer); err != nil && workerCtx.Err() == nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}()
	}

	select {
	case <-ctx.Done():
		cancel()
		workers.Wait()
		return nil
	case err := <-errCh:
		cancel()
		workers.Wait()
		return err
	}
}

func (t *Transport) declareTopology(ctx context.Context) (js.Consumer, error) {
	if _, err := t.js.CreateOrUpdateStream(ctx, js.StreamConfig{
		Name:      t.cfg.TasksStream,
		Subjects:  []string{t.cfg.TaskSubject},
		Retention: js.WorkQueuePolicy,
		Storage:   js.FileStorage,
	}); err != nil {
		return nil, fmt.Errorf("declare tasks stream: %w", err)
	}
	if _, err := t.js.CreateOrUpdateStream(ctx, js.StreamConfig{
		Name:       t.cfg.ResultsStream,
		Subjects:   []string{t.cfg.ResultSubject, t.cfg.FailedSubject},
		Retention:  js.LimitsPolicy,
		Storage:    js.FileStorage,
		Duplicates: 5 * time.Minute,
	}); err != nil {
		return nil, fmt.Errorf("declare results stream: %w", err)
	}
	stream, err := t.js.Stream(ctx, t.cfg.TasksStream)
	if err != nil {
		return nil, fmt.Errorf("load tasks stream: %w", err)
	}
	consumer, err := stream.CreateOrUpdateConsumer(ctx, js.ConsumerConfig{
		Durable:    t.cfg.ConsumerDurable,
		AckPolicy:  js.AckExplicitPolicy,
		MaxDeliver: t.cfg.DeliveryLimit,
		AckWait:    t.cfg.AckWait,
	})
	if err != nil {
		return nil, fmt.Errorf("declare tasks consumer: %w", err)
	}
	return consumer, nil
}

func (t *Transport) consume(ctx context.Context, consumer js.Consumer) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		batch, err := consumer.Fetch(1, js.FetchMaxWait(t.cfg.FetchWait))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			return fmt.Errorf("fetch task: %w", err)
		}
		for message := range batch.Messages() {
			if err := t.handleTask(ctx, message); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
		}
		if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("fetch task batch: %w", err)
		}
	}
}

func (t *Transport) handleTask(ctx context.Context, message js.Msg) error {
	var req models.PrepRequest
	if err := json.Unmarshal(message.Data(), &req); err != nil {
		result := models.PrepResult{Status: "error", Error: "invalid task JSON: " + err.Error()}
		if err := t.publishResult(ctx, result); err != nil {
			return err
		}
		return message.Ack()
	}

	taskCtx, cancel := context.WithTimeout(ctx, t.cfg.TaskTimeout)
	defer cancel()
	result, err := t.service.Prep(taskCtx, req)
	if err == nil {
		if err := t.publishResult(ctx, result); err != nil {
			return err
		}
		return message.Ack()
	}
	var permanent *prepservice.PermanentError
	if errors.As(err, &permanent) {
		if err := t.publishResult(ctx, result); err != nil {
			return err
		}
		return message.Ack()
	}

	metadata, metadataErr := message.Metadata()
	if metadataErr != nil {
		return fmt.Errorf("read task delivery metadata: %w", metadataErr)
	}
	if int(metadata.NumDelivered) >= t.cfg.DeliveryLimit {
		result.Status = "error"
		result.Error = "JetStream delivery limit exceeded: " + err.Error()
		if err := t.publishResult(ctx, result); err != nil {
			return err
		}
		if err := t.publishFailed(ctx, message, result); err != nil {
			return err
		}
		return message.Ack()
	}

	t.log.ErrorContext(ctx, "task processing will be retried",
		"run_id", req.RunID,
		"batch_id", req.BatchID,
		"attempt", req.Attempt,
		"delivery", metadata.NumDelivered,
		"error", err,
	)
	if err := message.NakWithDelay(t.cfg.ReconnectDelay); err != nil {
		return fmt.Errorf("nack task: %w", err)
	}
	return nil
}

func (t *Transport) publishResult(ctx context.Context, result models.PrepResult) error {
	body, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal result: %w", err)
	}
	messageID := fmt.Sprintf("%s/%s/%d/%s", result.RunID, result.BatchID, result.Attempt, result.SourceRevision)
	if result.RunID == "" && result.BatchID == "" {
		messageID = fmt.Sprintf("invalid/%d", time.Now().UnixNano())
	}
	message := &nats.Msg{
		Subject: t.cfg.ResultSubject,
		Data:    body,
		Header: nats.Header{
			"run_id":   []string{result.RunID},
			"batch_id": []string{result.BatchID},
			"attempt":  []string{fmt.Sprint(result.Attempt)},
			"status":   []string{result.Status},
		},
	}
	if _, err := t.js.PublishMsg(ctx, message, js.WithMsgID(messageID)); err != nil {
		return fmt.Errorf("publish result: %w", err)
	}
	return nil
}

func (t *Transport) publishFailed(ctx context.Context, task js.Msg, result models.PrepResult) error {
	messageID := fmt.Sprintf("%s/%s/%d/%s/failed", result.RunID, result.BatchID, result.Attempt, result.SourceRevision)
	message := &nats.Msg{
		Subject: t.cfg.FailedSubject,
		Data:    append([]byte(nil), task.Data()...),
		Header: nats.Header{
			"run_id":   []string{result.RunID},
			"batch_id": []string{result.BatchID},
			"attempt":  []string{fmt.Sprint(result.Attempt)},
			"error":    []string{result.Error},
		},
	}
	if _, err := t.js.PublishMsg(ctx, message, js.WithMsgID(messageID)); err != nil {
		return fmt.Errorf("publish failed task: %w", err)
	}
	t.log.ErrorContext(ctx, "task moved to failed subject",
		"run_id", result.RunID,
		"batch_id", result.BatchID,
		"attempt", result.Attempt,
		"error", result.Error,
	)
	return nil
}
