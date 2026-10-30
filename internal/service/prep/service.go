package prep

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/TzimmesTeam/data-prep/internal/domain/models"
	"github.com/TzimmesTeam/data-prep/pkg/batch"
)

type Service struct {
	partitionSize int
	maxSize       int
	log           *slog.Logger
}

func New(partitionSize, maxSize int, log *slog.Logger) *Service {
	return &Service{
		partitionSize: partitionSize,
		maxSize:       maxSize,
		log:           log,
	}
}

func (s *Service) Prep(
	ctx context.Context,
	req models.PrepRequest,
	deals []models.Deal,
	rejected []models.RejectedDeal,
) models.PrepResponse {
	s.log.InfoContext(ctx, "prep started",
		slog.String("run_id", req.RunID),
		slog.String("batch_id", req.BatchID),
		slog.Int("attempt", req.Attempt),
	)

	if len(deals) > s.maxSize {
		errMsg := fmt.Sprintf("batch too large: %d > %d", len(deals), s.maxSize)
		s.log.ErrorContext(ctx, errMsg)
		return models.PrepResponse{
			RunID:   req.RunID,
			BatchID: req.BatchID,
			Attempt: req.Attempt,
			Status:  "error",
			Errors:  []string{errMsg},
		}
	}

	partitions := batch.Partition(deals, s.partitionSize)

	rejectedIDs := make([]string, 0, len(rejected))
	for _, r := range rejected {
		rejectedIDs = append(rejectedIDs, r.DealID)
		s.log.WarnContext(ctx, "deal rejected",
			slog.String("deal_id", r.DealID),
			slog.String("reason", r.Reason),
		)
	}

	s.log.InfoContext(ctx, "prep completed",
		slog.Int("deals_in", len(deals)+len(rejected)),
		slog.Int("deals_ok", len(deals)),
		slog.Int("deals_rejected", len(rejected)),
		slog.Int("partitions", len(partitions)),
	)

	return models.PrepResponse{
		RunID:   req.RunID,
		BatchID: req.BatchID,
		Attempt: req.Attempt,
		Status:  "success",
		Stats: models.Stats{
			DealsIn:       len(deals) + len(rejected),
			DealsOK:       len(deals),
			DealsRejected: len(rejected),
		},
		Partitions:  partitions,
		RejectedIDs: rejectedIDs,
	}
}
