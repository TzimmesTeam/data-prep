package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/TzimmesTeam/data-prep/internal/domain/models"
)

type Config struct {
	URL         string
	Timeout     time.Duration
	MaxAttempts int
	RetryDelay  time.Duration
}

type Client struct {
	cfg    Config
	client *http.Client
}

func New(cfg Config) *Client {
	return &Client{cfg: cfg, client: &http.Client{Timeout: cfg.Timeout}}
}

func (c *Client) Notify(ctx context.Context, snapshotID string) error {
	body, err := json.Marshal(models.Callback{SnapshotID: snapshotID})
	if err != nil {
		return fmt.Errorf("encode callback: %w", err)
	}
	var lastErr error
	for attempt := 1; attempt <= c.cfg.MaxAttempts; attempt++ {
		lastErr = c.send(ctx, body)
		if lastErr == nil {
			return nil
		}
		if attempt == c.cfg.MaxAttempts || !retryable(lastErr) {
			break
		}
		timer := time.NewTimer(time.Duration(attempt) * c.cfg.RetryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return lastErr
}

func (c *Client) send(ctx context.Context, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.URL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create callback request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return nil
	}
	return &statusError{status: response.StatusCode}
}

type statusError struct {
	status int
}

func (e *statusError) Error() string {
	return fmt.Sprintf("control plane returned HTTP %d", e.status)
}

func retryable(err error) bool {
	if status, ok := err.(*statusError); ok {
		return status.status >= 500
	}
	return true
}
