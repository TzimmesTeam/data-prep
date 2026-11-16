package prep

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/TzimmesTeam/data-prep/internal/domain/models"
	s3storage "github.com/TzimmesTeam/data-prep/internal/storage/s3"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/parquet-go/parquet-go"
)

type parquetSink struct {
	dealsFile     *os.File
	rejectedFile  *os.File
	dealsWriter   *parquet.GenericWriter[models.Deal]
	rejectsWriter *parquet.GenericWriter[models.RejectedDeal]
	stats         models.Stats
	log           *slog.Logger
}

func newParquetSink(log *slog.Logger) (*parquetSink, error) {
	dealsFile, err := os.CreateTemp("", "data-prep-deals-*.parquet")
	if err != nil {
		return nil, fmt.Errorf("create curated temporary file: %w", err)
	}
	rejectedFile, err := os.CreateTemp("", "data-prep-rejected-*.parquet")
	if err != nil {
		dealsFile.Close()
		os.Remove(dealsFile.Name())
		return nil, fmt.Errorf("create quarantine temporary file: %w", err)
	}
	return &parquetSink{
		dealsFile:     dealsFile,
		rejectedFile:  rejectedFile,
		dealsWriter:   parquet.NewGenericWriter[models.Deal](dealsFile),
		rejectsWriter: parquet.NewGenericWriter[models.RejectedDeal](rejectedFile),
		log:           log,
	}, nil
}

func (s *parquetSink) Deal(deal models.Deal) error {
	ensureLists(&deal)
	if _, err := s.dealsWriter.Write([]models.Deal{deal}); err != nil {
		return fmt.Errorf("write curated Parquet row: %w", err)
	}
	s.stats.DealsIn++
	s.stats.DealsOK++
	return nil
}

func (s *parquetSink) Rejected(deal models.RejectedDeal) error {
	if _, err := s.rejectsWriter.Write([]models.RejectedDeal{deal}); err != nil {
		return fmt.Errorf("write quarantine Parquet row: %w", err)
	}
	s.stats.DealsIn++
	s.stats.DealsRejected++
	s.log.Warn("deal rejected",
		"row", deal.Row,
		"deal_id", aws.ToString(deal.DealID),
		"reasons", deal.Reasons,
	)
	return nil
}

func (s *parquetSink) closeWriters() error {
	if err := s.dealsWriter.Close(); err != nil {
		return fmt.Errorf("close curated Parquet writer: %w", err)
	}
	if err := s.rejectsWriter.Close(); err != nil {
		return fmt.Errorf("close quarantine Parquet writer: %w", err)
	}
	return nil
}

func (s *parquetSink) upload(ctx context.Context, store ObjectStore, file *os.File, uri s3storage.URI) error {
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat Parquet file: %w", err)
	}
	if _, err := file.Seek(0, 0); err != nil {
		return fmt.Errorf("rewind Parquet file: %w", err)
	}
	if err := store.Put(ctx, uri, file, info.Size(), "application/vnd.apache.parquet"); err != nil {
		return err
	}
	return nil
}

func (s *parquetSink) cleanup() {
	if s.dealsFile != nil {
		s.dealsFile.Close()
		os.Remove(s.dealsFile.Name())
	}
	if s.rejectedFile != nil {
		s.rejectedFile.Close()
		os.Remove(s.rejectedFile.Name())
	}
}

func ensureLists(deal *models.Deal) {
	if deal.InterestPayment == nil {
		deal.InterestPayment = []int64{}
	}
	if deal.PrincipalPaymentDates == nil {
		deal.PrincipalPaymentDates = []int64{}
	}
	if deal.PrincipalPaymentAmounts == nil {
		deal.PrincipalPaymentAmounts = []float64{}
	}
	if deal.RepricingDtSchedule == nil {
		deal.RepricingDtSchedule = []int64{}
	}
	if deal.RepricingDtRateScheduleDates == nil {
		deal.RepricingDtRateScheduleDates = []int64{}
	}
	if deal.RepricingDtRateScheduleRates == nil {
		deal.RepricingDtRateScheduleRates = []float64{}
	}
	if deal.RepricingDtRateScheduleFixedDates == nil {
		deal.RepricingDtRateScheduleFixedDates = []int64{}
	}
	if deal.RepricingDtRateScheduleFixedRates == nil {
		deal.RepricingDtRateScheduleFixedRates = []float64{}
	}
}
