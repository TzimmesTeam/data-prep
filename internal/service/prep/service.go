package prep

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path"

	"github.com/TzimmesTeam/data-prep/internal/domain/models"
	prepparser "github.com/TzimmesTeam/data-prep/internal/handler/prep"
	s3storage "github.com/TzimmesTeam/data-prep/internal/storage/s3"
)

type Buckets struct {
	Curated    string
	Quarantine string
}

type ObjectStore interface {
	HeadSource(context.Context, s3storage.URI) (models.Source, error)
	OpenSource(context.Context, s3storage.URI, models.Source) (io.ReadCloser, error)
	Put(context.Context, s3storage.URI, io.Reader, int64, string) error
}

type Parser interface {
	Parse(io.Reader, models.SourceFormat, prepparser.Sink) error
}

type Service struct {
	store   ObjectStore
	parser  Parser
	buckets Buckets
	log     *slog.Logger
}

func New(store ObjectStore, parser Parser, buckets Buckets, log *slog.Logger) *Service {
	return &Service{store: store, parser: parser, buckets: buckets, log: log}
}

func completeDelivery(delivery *models.Delivery) bool {
	return delivery != nil && delivery.Bucket != "" && delivery.Key != "" && delivery.Source != "" &&
		delivery.IngestDate != "" && delivery.SnapshotID != ""
}

func (s *Service) Prep(ctx context.Context, delivery *models.Delivery) (models.PrepResult, error) {
	result := models.PrepResult{}
	if !completeDelivery(delivery) {
		return result, fmt.Errorf("incomplete delivery")
	}
	result.Delivery = *delivery
	format, err := models.SourceFormatFromKey(delivery.Key)
	if err != nil {
		return result, err
	}
	sourceURI := s3storage.URI{Bucket: delivery.Bucket, Key: delivery.Key}
	source, err := s.store.HeadSource(ctx, sourceURI)
	if err != nil {
		return result, err
	}
	result.Source = source
	body, err := s.store.OpenSource(ctx, sourceURI, source)
	if err != nil {
		return result, err
	}

	sink, err := newParquetSink(s.log)
	if err != nil {
		_ = body.Close()
		return result, err
	}
	defer sink.cleanup()

	parseErr := s.parser.Parse(body, format, sink)
	closeErr := body.Close()
	if parseErr != nil {
		return result, fmt.Errorf("parse %s: %w", format, parseErr)
	}
	if closeErr != nil {
		return result, closeErr
	}
	if err := sink.closeWriters(); err != nil {
		return result, err
	}
	result.Stats = sink.stats

	key := path.Join(
		"source="+delivery.Source,
		"ingest_date="+delivery.IngestDate,
		"snapshot_id="+delivery.SnapshotID,
		"deals.parquet",
	)
	curatedURI := s3storage.URI{Bucket: s.buckets.Curated, Key: key}
	if err := sink.upload(ctx, s.store, sink.dealsFile, curatedURI); err != nil {
		return result, err
	}
	result.CuratedURI = curatedURI.String()
	if sink.stats.DealsRejected > 0 {
		quarantineURI := s3storage.URI{Bucket: s.buckets.Quarantine, Key: key}
		if err := sink.upload(ctx, s.store, sink.rejectedFile, quarantineURI); err != nil {
			return result, err
		}
		result.QuarantineURI = quarantineURI.String()
	}

	s.log.InfoContext(ctx, "prep completed",
		"snapshot_id", delivery.SnapshotID,
		"source_format", format.String(),
		"source_revision", source.Revision,
		"deals_in", sink.stats.DealsIn,
		"deals_ok", sink.stats.DealsOK,
		"deals_rejected", sink.stats.DealsRejected,
	)
	return result, nil
}
