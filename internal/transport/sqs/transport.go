package sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/TzimmesTeam/data-prep/internal/domain/models"
	prepservice "github.com/TzimmesTeam/data-prep/internal/service/prep"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type Config struct {
	Region            string
	Endpoint          string
	QueueURL          string
	AccessKeyID       string
	SecretAccessKey   string
	RawBucket         string
	WaitTimeSeconds   int32
	VisibilityTimeout int32
	ReconnectDelay    time.Duration
}

type Notifier interface {
	Notify(context.Context, string) error
}

type Transport struct {
	cfg     *Config
	service *prepservice.Service
	notify  Notifier
	log     *slog.Logger
	client  *awssqs.Client
	ready   atomic.Bool
}

func New(cfg *Config, service *prepservice.Service, notify Notifier, log *slog.Logger) (*Transport, error) {
	options := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(cfg.Region)}
	if cfg.AccessKeyID != "" {
		options = append(options, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID,
			cfg.SecretAccessKey,
			"",
		)))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), options...)
	if err != nil {
		return nil, fmt.Errorf("load SQS AWS config: %w", err)
	}
	client := awssqs.NewFromConfig(awsCfg, func(options *awssqs.Options) {
		if cfg.Endpoint != "" {
			options.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	})
	return &Transport{cfg: cfg, service: service, notify: notify, log: log, client: client}, nil
}

func (t *Transport) Ready() bool {
	return t.ready.Load()
}

func (t *Transport) Run(ctx context.Context) error {
	defer t.ready.Store(false)
	t.ready.Store(true)
	for ctx.Err() == nil {
		output, err := t.client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
			QueueUrl:            aws.String(t.cfg.QueueURL),
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     t.cfg.WaitTimeSeconds,
			VisibilityTimeout:   t.cfg.VisibilityTimeout,
		})
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			t.ready.Store(false)
			t.log.ErrorContext(ctx, "SQS receive failed", "error", err)
			if err := sleepContext(ctx, t.cfg.ReconnectDelay); err != nil {
				return nil
			}
			t.ready.Store(true)
			continue
		}
		t.ready.Store(true)
		for _, message := range output.Messages {
			if err := t.handleMessage(ctx, message); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				t.log.ErrorContext(ctx, "SQS message processing failed; message will become visible again",
					"message_id", aws.ToString(message.MessageId), "error", err)
			}
		}
	}
	return nil
}

func (t *Transport) handleMessage(ctx context.Context, message types.Message) error {
	var event s3Event
	if err := json.Unmarshal([]byte(aws.ToString(message.Body)), &event); err != nil {
		return fmt.Errorf("decode S3 event: %w", err)
	}
	if len(event.Records) == 0 {
		return fmt.Errorf("S3 event has no records")
	}
	for _, record := range event.Records {
		if !strings.HasPrefix(record.EventName, "s3:ObjectCreated:") {
			continue
		}
		if record.S3.Bucket.Name != t.cfg.RawBucket {
			return fmt.Errorf("unexpected source bucket %q", record.S3.Bucket.Name)
		}
		delivery, err := parseDelivery(record.S3.Bucket.Name, record.S3.Object.Key)
		if err != nil {
			return err
		}
		result, err := t.service.Prep(ctx, &delivery)
		if err != nil {
			return err
		}
		if err := t.notify.Notify(ctx, delivery.SnapshotID); err != nil {
			return fmt.Errorf("notify control plane: %w", err)
		}
		t.log.InfoContext(ctx, "delivery completed",
			"snapshot_id", delivery.SnapshotID,
			"curated_uri", result.CuratedURI,
			"quarantine_uri", result.QuarantineURI,
			"deals_in", result.Stats.DealsIn,
			"deals_ok", result.Stats.DealsOK,
			"deals_rejected", result.Stats.DealsRejected,
		)
	}
	if message.ReceiptHandle == nil {
		return fmt.Errorf("SQS message has no receipt handle")
	}
	if _, err := t.client.DeleteMessage(ctx, &awssqs.DeleteMessageInput{
		QueueUrl:      aws.String(t.cfg.QueueURL),
		ReceiptHandle: message.ReceiptHandle,
	}); err != nil {
		return fmt.Errorf("delete SQS message: %w", err)
	}
	return nil
}

type s3Event struct {
	Records []struct {
		EventName string `json:"eventName"`
		S3        struct {
			Bucket struct {
				Name string `json:"name"`
			} `json:"bucket"`
			Object struct {
				Key string `json:"key"`
			} `json:"object"`
		} `json:"s3"`
	} `json:"Records"`
}

func parseDelivery(bucket, encodedKey string) (models.Delivery, error) {
	key, err := url.QueryUnescape(encodedKey)
	if err != nil {
		return models.Delivery{}, fmt.Errorf("decode S3 object key: %w", err)
	}
	parts := strings.Split(key, "/")
	if len(parts) != 4 || strings.ToLower(path.Ext(parts[3])) != ".csv" {
		return models.Delivery{}, fmt.Errorf("unexpected RAW CSV key %q", key)
	}
	delivery := models.Delivery{Bucket: bucket, Key: key}
	for _, part := range parts[:3] {
		name, value, ok := strings.Cut(part, "=")
		if !ok || value == "" {
			return models.Delivery{}, fmt.Errorf("invalid RAW key segment %q", part)
		}
		switch name {
		case "source":
			delivery.Source = value
		case "ingest_date":
			if _, err := time.Parse("2006-01-02", value); err != nil {
				return models.Delivery{}, fmt.Errorf("invalid ingest_date %q", value)
			}
			delivery.IngestDate = value
		case "snapshot_id":
			delivery.SnapshotID = value
		default:
			return models.Delivery{}, fmt.Errorf("unexpected RAW key segment %q", part)
		}
	}
	if delivery.Source == "" || delivery.IngestDate == "" || delivery.SnapshotID == "" {
		return models.Delivery{}, fmt.Errorf("RAW key must contain source, ingest_date and snapshot_id")
	}
	return delivery, nil
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
