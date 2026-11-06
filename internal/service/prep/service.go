package prep

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"path"

	"github.com/TzimmesTeam/data-prep/internal/domain/models"
	prepparser "github.com/TzimmesTeam/data-prep/internal/handler/prep"
	s3storage "github.com/TzimmesTeam/data-prep/internal/storage/s3"
	"github.com/aws/smithy-go"
	"github.com/google/uuid"
)

type Options struct {
	MaxRows          int
	PartitionRows    int
	PartitionBytes   int
	RejectedLogLimit int
}

type ObjectStore interface {
	CheckBucket(context.Context, s3storage.URI) error
	HeadSource(context.Context, s3storage.URI) (models.Source, error)
	OpenSource(context.Context, s3storage.URI, models.Source) (io.ReadCloser, error)
	Put(context.Context, s3storage.URI, []byte, string) error
}

type CSVParser interface {
	Parse(io.Reader, prepparser.Sink) error
}

type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string {
	return e.Err.Error()
}

func (e *PermanentError) Unwrap() error {
	return e.Err
}

type Service struct {
	store  ObjectStore
	parser CSVParser
	opts   Options
	log    *slog.Logger
}

func New(store ObjectStore, parser CSVParser, opts Options, log *slog.Logger) *Service {
	return &Service{store: store, parser: parser, opts: opts, log: log}
}

func (s *Service) Prep(ctx context.Context, req models.PrepRequest) (models.PrepResult, error) {
	result := models.PrepResult{RunID: req.RunID, BatchID: req.BatchID, Attempt: req.Attempt, Status: "error"}
	sourceURI, outputURI, err := validateRequest(req)
	if err != nil {
		result.Error = err.Error()
		return result, &PermanentError{Err: err}
	}
	if err := s.store.CheckBucket(ctx, sourceURI); err != nil {
		result.Error = err.Error()
		return result, err
	}
	if err := s.store.CheckBucket(ctx, outputURI); err != nil {
		result.Error = err.Error()
		return result, err
	}

	source, err := s.store.HeadSource(ctx, sourceURI)
	if err != nil {
		result.Error = err.Error()
		return result, err
	}
	result.SourceRevision = source.Revision
	body, err := s.store.OpenSource(ctx, sourceURI, source)
	if err != nil {
		result.Error = err.Error()
		return result, err
	}

	revisionKey := base64.RawURLEncoding.EncodeToString([]byte(source.Revision))
	outputBase := outputURI.Join("source=" + revisionKey)
	sink := newBatchSink(ctx, s.store, outputBase, s.opts, s.log)
	parseErr := s.parser.Parse(body, sink)
	closeErr := body.Close()
	if parseErr != nil {
		result.Stats = sink.stats
		result.Error = parseErr.Error()
		if isTransient(parseErr) {
			return result, parseErr
		}
		return result, &PermanentError{Err: parseErr}
	}
	if closeErr != nil {
		result.Error = closeErr.Error()
		return result, closeErr
	}
	if err := sink.Close(); err != nil {
		result.Stats = sink.stats
		result.Error = err.Error()
		return result, err
	}

	manifest := models.Manifest{
		SchemaVersion: 1,
		RunID:         req.RunID,
		BatchID:       req.BatchID,
		Attempt:       req.Attempt,
		Source:        source,
		Deals:         append([]models.Partition{}, sink.deals.partitions...),
		Rejected:      append([]models.Partition{}, sink.rejected.partitions...),
		Stats:         sink.stats,
	}
	manifestBody, err := json.Marshal(manifest)
	if err != nil {
		result.Error = err.Error()
		return result, &PermanentError{Err: err}
	}
	manifestURI := outputBase.Join("manifest.json")
	if err := s.store.Put(ctx, manifestURI, manifestBody, "application/json"); err != nil {
		result.Error = err.Error()
		return result, err
	}

	result.Status = "success"
	result.ManifestURI = manifestURI.String()
	result.Stats = sink.stats
	result.Error = ""
	s.log.InfoContext(ctx, "prep completed",
		slog.String("run_id", req.RunID),
		slog.String("batch_id", req.BatchID),
		slog.Int("attempt", req.Attempt),
		slog.String("source_revision", source.Revision),
		slog.Int("deals_in", sink.stats.DealsIn),
		slog.Int("deals_ok", sink.stats.DealsOK),
		slog.Int("deals_rejected", sink.stats.DealsRejected),
		slog.Int("parts_out", sink.stats.PartsOut),
	)
	return result, nil
}

func validateRequest(req models.PrepRequest) (s3storage.URI, s3storage.URI, error) {
	if _, err := uuid.Parse(req.RunID); err != nil {
		return s3storage.URI{}, s3storage.URI{}, fmt.Errorf("invalid run_id: %w", err)
	}
	if _, err := uuid.Parse(req.BatchID); err != nil {
		return s3storage.URI{}, s3storage.URI{}, fmt.Errorf("invalid batch_id: %w", err)
	}
	if req.Attempt < 1 {
		return s3storage.URI{}, s3storage.URI{}, fmt.Errorf("attempt must be positive")
	}
	sourceURI, err := s3storage.ParseURI(req.SourceURI)
	if err != nil {
		return s3storage.URI{}, s3storage.URI{}, fmt.Errorf("invalid source_uri: %w", err)
	}
	outputURI, err := s3storage.ParseURI(req.OutputPrefix)
	if err != nil {
		return s3storage.URI{}, s3storage.URI{}, fmt.Errorf("invalid output_prefix: %w", err)
	}
	expectedSuffix := fmt.Sprintf("attempt=%d", req.Attempt)
	if path.Base(outputURI.Key) != expectedSuffix {
		return s3storage.URI{}, s3storage.URI{}, fmt.Errorf("output_prefix must end with %s", expectedSuffix)
	}
	return sourceURI, outputURI, nil
}

func isTransient(err error) bool {
	var permanent *PermanentError
	if errors.As(err, &permanent) {
		return false
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

type batchSink struct {
	ctx          context.Context
	deals        *partitionWriter
	rejected     *partitionWriter
	stats        models.Stats
	maxRows      int
	rejectedLogs int
	log          *slog.Logger
}

func newBatchSink(ctx context.Context, store ObjectStore, base s3storage.URI, opts Options, log *slog.Logger) *batchSink {
	return &batchSink{
		ctx:          ctx,
		deals:        newPartitionWriter(ctx, store, base, "deals", opts.PartitionRows, opts.PartitionBytes),
		rejected:     newPartitionWriter(ctx, store, base, "rejected", opts.PartitionRows, opts.PartitionBytes),
		maxRows:      opts.MaxRows,
		rejectedLogs: opts.RejectedLogLimit,
		log:          log,
	}
}

func (s *batchSink) Deal(deal models.Deal) error {
	if err := s.countInput(); err != nil {
		return err
	}
	if err := s.deals.Write(deal); err != nil {
		return err
	}
	s.stats.DealsOK++
	s.stats.PartsOut = len(s.deals.partitions)
	if s.deals.rows > 0 {
		s.stats.PartsOut++
	}
	return nil
}

func (s *batchSink) Rejected(deal models.RejectedDeal) error {
	if err := s.countInput(); err != nil {
		return err
	}
	if err := s.rejected.Write(deal); err != nil {
		return err
	}
	s.stats.DealsRejected++
	if s.rejectedLogs > 0 {
		s.log.WarnContext(s.ctx, "deal rejected",
			slog.String("deal_id", deal.DealID),
			slog.Int("row", deal.Row),
			slog.Any("reasons", deal.Reasons),
		)
		s.rejectedLogs--
	}
	return nil
}

func (s *batchSink) Close() error {
	if err := s.deals.Close(); err != nil {
		return err
	}
	if err := s.rejected.Close(); err != nil {
		return err
	}
	s.stats.PartsOut = len(s.deals.partitions)
	return nil
}

func (s *batchSink) countInput() error {
	s.stats.DealsIn++
	if s.maxRows > 0 && s.stats.DealsIn > s.maxRows {
		return &PermanentError{Err: fmt.Errorf("batch too large: %d > %d", s.stats.DealsIn, s.maxRows)}
	}
	return nil
}

type partitionWriter struct {
	ctx        context.Context
	store      ObjectStore
	base       s3storage.URI
	kind       string
	maxRows    int
	maxBytes   int
	buffer     bytes.Buffer
	rows       int
	partitions []models.Partition
}

func newPartitionWriter(ctx context.Context, store ObjectStore, base s3storage.URI, kind string, maxRows, maxBytes int) *partitionWriter {
	return &partitionWriter{ctx: ctx, store: store, base: base, kind: kind, maxRows: maxRows, maxBytes: maxBytes}
}

func (w *partitionWriter) Write(value any) error {
	line, err := json.Marshal(value)
	if err != nil {
		return &PermanentError{Err: fmt.Errorf("marshal %s: %w", w.kind, err)}
	}
	line = append(line, '\n')
	if len(line) > w.maxBytes {
		return &PermanentError{Err: fmt.Errorf("%s row exceeds partition byte limit: %d > %d", w.kind, len(line), w.maxBytes)}
	}
	if w.rows > 0 && (w.rows >= w.maxRows || w.buffer.Len()+len(line) > w.maxBytes) {
		if err := w.flush(); err != nil {
			return err
		}
	}
	if _, err := w.buffer.Write(line); err != nil {
		return err
	}
	w.rows++
	return nil
}

func (w *partitionWriter) Close() error {
	return w.flush()
}

func (w *partitionWriter) flush() error {
	if w.rows == 0 {
		return nil
	}
	partNumber := len(w.partitions) + 1
	uri := w.base.Join(w.kind, fmt.Sprintf("part-%06d.ndjson", partNumber))
	body := append([]byte(nil), w.buffer.Bytes()...)
	if err := w.store.Put(w.ctx, uri, body, "application/x-ndjson"); err != nil {
		return err
	}
	w.partitions = append(w.partitions, models.Partition{URI: uri.String(), SizeBytes: int64(len(body)), Rows: w.rows})
	w.buffer.Reset()
	w.rows = 0
	return nil
}
