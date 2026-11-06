package prep

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/TzimmesTeam/data-prep/internal/domain/models"
)

var knownHeaders = []string{
	"report_dttm", "discriminator", "deal_id", "currency_id", "start_principal_amt",
	"current_principal_amt", "deal_dttm", "maturity_dttm", "rate_type_cd", "interest_rate",
	"base_indicator_rate", "base_indicator", "interest_payment_rule_start_dttm",
	"interest_payment_rule_period_cd", "interest_payment_rule_day_num", "day_convention_cd",
	"regular_payment_rule_start_dttm", "regular_payment_rule_period_cd", "regular_payment_rule_day_num",
	"repricing_rule_start_dttm", "repricing_rule_period_cd", "repricing_rule_day_num",
	"accrued_interest", "product", "behavior_model_cd", "interest_payment", "principal_payment",
	"repricing_dttm_schedule", "repricing_dttm_rate_schedule", "repricing_dttm_rate_schedule_fixed",
	"cap_limit_interest_rate", "floor_limit_interest_rate", "sub_rate_type_cd", "sub_interest_rate",
	"sub_float_indicator", "sub_payment_rule_period_cd", "sub_payment_rule_day_num",
	"sub_payment_rule_start_dttm", "sub_repricing_rule_start_dttm", "sub_repricing_rule_period_cd",
	"sub_repricing_rule_day_num", "sub_cap_limit_interest_rate", "sub_floor_limit_interest_rate",
	"sub_coeff_index", "deal_report_dttm", "sub_repricing_rule_first_event_flg", "indicator_lag_cd",
	"sub_indicator_lag_cd",
}

var knownHeaderSet = func() map[string]struct{} {
	result := make(map[string]struct{}, len(knownHeaders))
	for _, header := range knownHeaders {
		result[header] = struct{}{}
	}
	return result
}()

var discriminatorCodes = map[string]int32{"ANN": 1, "RGM": 2, "SIMPLE": 3}
var rateTypeCodes = map[string]int32{"FIXED": 1, "FLOAT": 2}
var dayConventionCodes = map[string]int32{
	"ACT/ACT": 1, "ACT/ACT ISDA": 2, "ACT/365": 3,
	"ACT/360": 4, "30/360": 5, "30E/360": 6,
}

type Sink interface {
	Deal(models.Deal) error
	Rejected(models.RejectedDeal) error
}

type Parser struct {
	log *slog.Logger
}

func NewParser(log *slog.Logger) *Parser {
	return &Parser{log: log}
}

func (p *Parser) Parse(r io.Reader, sink Sink) error {
	reader := csv.NewReader(r)
	reader.Comma = ';'
	reader.TrimLeadingSpace = true

	headers, err := reader.Read()
	if err != nil {
		return fmt.Errorf("read headers: %w", err)
	}

	columns := make(map[string]int, len(headers))
	for i, header := range headers {
		header = strings.TrimSpace(header)
		if _, known := knownHeaderSet[header]; !known {
			p.log.Warn("ignoring unknown CSV header", "header", header, "position", i+1)
			continue
		}
		if _, exists := columns[header]; exists {
			return fmt.Errorf("duplicate header: %s", header)
		}
		columns[header] = i
	}
	var missing []string
	for _, header := range knownHeaders {
		if _, ok := columns[header]; !ok {
			missing = append(missing, header)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("required headers are missing: %s", strings.Join(missing, ", "))
	}
	reader.FieldsPerRecord = len(headers)

	seen := make(map[int64]struct{})
	for rowNumber := 2; ; rowNumber++ {
		row, readErr := reader.Read()
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if errors.Is(readErr, csv.ErrFieldCount) {
			rejected := models.RejectedDeal{Row: rowNumber, Reasons: []string{readErr.Error()}}
			if idx := columns["deal_id"]; idx < len(row) {
				rejected.DealID = strings.TrimSpace(row[idx])
			}
			if err := sink.Rejected(rejected); err != nil {
				return fmt.Errorf("write rejected row %d: %w", rowNumber, err)
			}
			continue
		}
		if readErr != nil {
			return fmt.Errorf("read row %d: %w", rowNumber, readErr)
		}

		rawDealID := strings.TrimSpace(row[columns["deal_id"]])
		deal, reasons := parseDeal(row, columns)
		if len(reasons) > 0 {
			if err := sink.Rejected(models.RejectedDeal{DealID: rawDealID, Row: rowNumber, Reasons: reasons}); err != nil {
				return fmt.Errorf("write rejected row %d: %w", rowNumber, err)
			}
			continue
		}
		if deal.DealID != nil {
			if _, exists := seen[*deal.DealID]; exists {
				if err := sink.Rejected(models.RejectedDeal{DealID: rawDealID, Row: rowNumber, Reasons: []string{"duplicate deal_id"}}); err != nil {
					return fmt.Errorf("write rejected row %d: %w", rowNumber, err)
				}
				continue
			}
			seen[*deal.DealID] = struct{}{}
		}
		if err := sink.Deal(deal); err != nil {
			return fmt.Errorf("write deal row %d: %w", rowNumber, err)
		}
	}
}

type rowDecoder struct {
	row     []string
	columns map[string]int
	reasons []string
}

func parseDeal(row []string, columns map[string]int) (models.Deal, []string) {
	d := rowDecoder{row: row, columns: columns}
	deal := models.Deal{
		ReportDt:                      d.timestamp("report_dttm"),
		Discriminator:                 d.enum("discriminator", discriminatorCodes),
		DealID:                        d.int64("deal_id"),
		CurrencyID:                    d.currency("currency_id"),
		StartPrincipalAmt:             d.float64("start_principal_amt"),
		CurrentPrincipalAmt:           d.float64("current_principal_amt"),
		DealDt:                        d.timestamp("deal_dttm"),
		MaturityDt:                    d.timestamp("maturity_dttm"),
		RateTypeCd:                    d.enum("rate_type_cd", rateTypeCodes),
		InterestRate:                  d.float64("interest_rate"),
		BaseIndicatorRate:             d.float64("base_indicator_rate"),
		BaseIndicator:                 d.string("base_indicator"),
		InterestPaymentRuleStartDt:    d.timestamp("interest_payment_rule_start_dttm"),
		InterestPaymentRulePeriodCd:   d.period("interest_payment_rule_period_cd"),
		InterestPaymentRuleDayNum:     d.int32("interest_payment_rule_day_num"),
		DayConventionCd:               d.enum("day_convention_cd", dayConventionCodes),
		RegularPaymentRuleStartDt:     d.timestamp("regular_payment_rule_start_dttm"),
		RegularPaymentRulePeriodCd:    d.period("regular_payment_rule_period_cd"),
		RegularPaymentRuleDayNum:      d.int32("regular_payment_rule_day_num"),
		RepricingRuleStartDt:          d.timestamp("repricing_rule_start_dttm"),
		RepricingRulePeriodCd:         d.period("repricing_rule_period_cd"),
		RepricingRuleDayNum:           d.int32("repricing_rule_day_num"),
		AccruedInterest:               d.float64("accrued_interest"),
		Product:                       d.string("product"),
		BehaviorModelCd:               d.string("behavior_model_cd"),
		InterestPayment:               d.dates("interest_payment"),
		CapLimitInterestRate:          d.float64("cap_limit_interest_rate"),
		FloorLimitInterestRate:        d.float64("floor_limit_interest_rate"),
		SubRateTypeCd:                 d.enum("sub_rate_type_cd", rateTypeCodes),
		SubInterestRate:               d.float64("sub_interest_rate"),
		SubFloatIndicator:             d.string("sub_float_indicator"),
		SubPaymentRulePeriodCd:        d.period("sub_payment_rule_period_cd"),
		SubPaymentRuleDayNum:          d.int32("sub_payment_rule_day_num"),
		SubPaymentRuleStartDt:         d.timestamp("sub_payment_rule_start_dttm"),
		SubRepricingRuleStartDt:       d.timestamp("sub_repricing_rule_start_dttm"),
		SubRepricingRulePeriodCd:      d.period("sub_repricing_rule_period_cd"),
		SubRepricingRuleDayNum:        d.int32("sub_repricing_rule_day_num"),
		SubCapLimitInterestRate:       d.float64("sub_cap_limit_interest_rate"),
		SubFloorLimitInterestRate:     d.float64("sub_floor_limit_interest_rate"),
		SubCoeffIndex:                 d.float64("sub_coeff_index"),
		DealReportDt:                  d.timestamp("deal_report_dttm"),
		SubRepricingRuleFirstEventFlg: d.int32("sub_repricing_rule_first_event_flg"),
		IndicatorLagCd:                d.lag("indicator_lag_cd"),
		SubIndicatorLagCd:             d.lag("sub_indicator_lag_cd"),
	}

	deal.PrincipalPaymentDates, deal.PrincipalPaymentAmounts = d.datedNumbers("principal_payment", false)
	deal.RepricingDtSchedule = d.dates("repricing_dttm_schedule")
	deal.RepricingDtRateScheduleDates, deal.RepricingDtRateScheduleRates = d.datedNumbers("repricing_dttm_rate_schedule", true)
	deal.RepricingDtRateScheduleFixedDates, deal.RepricingDtRateScheduleFixedRates = d.datedNumbers("repricing_dttm_rate_schedule_fixed", true)

	if deal.DealDt != nil && deal.MaturityDt != nil && *deal.MaturityDt <= *deal.DealDt {
		d.add("maturity_dttm", "must be after deal_dttm")
	}

	return deal, d.reasons
}

func (d *rowDecoder) raw(name string) string {
	idx, ok := d.columns[name]
	if !ok || idx >= len(d.row) {
		return ""
	}
	return strings.TrimSpace(d.row[idx])
}

func (d *rowDecoder) string(name string) *string {
	value := d.raw(name)
	if value == "" {
		return nil
	}
	return &value
}

func (d *rowDecoder) enum(name string, allowed map[string]int32) *int32 {
	value := d.raw(name)
	if value == "" {
		return nil
	}
	code, ok := allowed[value]
	if !ok {
		d.add(name, "invalid value: "+value)
		return nil
	}
	return &code
}

func (d *rowDecoder) currency(name string) *int32 {
	value := d.raw(name)
	if value == "" {
		return nil
	}
	code, ok := currencyCodes[value]
	if !ok {
		d.add(name, "unknown ISO 4217 currency: "+value)
		return nil
	}
	return &code
}

func (d *rowDecoder) int64(name string) *int64 {
	value := d.raw(name)
	if value == "" {
		return nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		d.add(name, "invalid int64: "+value)
		return nil
	}
	return &parsed
}

func (d *rowDecoder) int32(name string) *int32 {
	value := d.raw(name)
	if value == "" {
		return nil
	}
	parsed, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		d.add(name, "invalid int32: "+value)
		return nil
	}
	result := int32(parsed)
	return &result
}

func (d *rowDecoder) float64(name string) *float64 {
	value := d.raw(name)
	if value == "" {
		return nil
	}
	parsed, err := parseFloat(value)
	if err != nil {
		d.add(name, err.Error())
		return nil
	}
	return &parsed
}

func parseFloat(value string) (float64, error) {
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return 0, fmt.Errorf("invalid float64: %s", value)
	}
	return parsed, nil
}

func (d *rowDecoder) timestamp(name string) *int64 {
	value := d.raw(name)
	if value == "" {
		return nil
	}
	parsed, err := models.ParseDate(value)
	if err != nil {
		d.add(name, err.Error())
		return nil
	}
	result := parsed.Time.Unix()
	return &result
}

func (d *rowDecoder) period(name string) *int32 {
	value := d.raw(name)
	if value == "" {
		return nil
	}
	parsed, err := parsePeriod(value, false)
	if err != nil {
		d.add(name, err.Error())
		return nil
	}
	return &parsed
}

func (d *rowDecoder) lag(name string) *int32 {
	value := d.raw(name)
	if value == "" {
		return nil
	}
	parsed, err := parsePeriod(value, true)
	if err != nil {
		d.add(name, err.Error())
		return nil
	}
	return &parsed
}

func parsePeriod(value string, signed bool) (int32, error) {
	if value == "ENDDATE" && !signed {
		return 0, nil
	}
	sign := int64(1)
	if strings.HasPrefix(value, "-") {
		if !signed {
			return 0, fmt.Errorf("invalid period: %s", value)
		}
		sign = -1
		value = value[1:]
	}
	if len(value) < 2 {
		return 0, fmt.Errorf("invalid period: %s", value)
	}
	unit := value[len(value)-1]
	magnitude, err := strconv.ParseInt(value[:len(value)-1], 10, 64)
	if err != nil || magnitude <= 0 {
		return 0, fmt.Errorf("invalid period: %s", value)
	}
	var code int64
	switch unit {
	case 'M':
		code = magnitude
	case 'Y':
		code = magnitude * 12
	case 'D':
		code = 1000000 + magnitude
	default:
		return 0, fmt.Errorf("invalid period: %s", value)
	}
	code *= sign
	if code < -1<<31 || code > 1<<31-1 {
		return 0, fmt.Errorf("period out of int32 range: %s", value)
	}
	return int32(code), nil
}

func (d *rowDecoder) dates(name string) []int64 {
	value := d.raw(name)
	if value == "" {
		return nil
	}
	parts := strings.Fields(value)
	result := make([]int64, 0, len(parts))
	for _, part := range parts {
		parsed, err := models.ParseDate(part)
		if err != nil {
			d.add(name, err.Error())
			continue
		}
		result = append(result, parsed.Time.Unix())
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	unique := result[:0]
	for i, value := range result {
		if i == 0 || value != result[i-1] {
			unique = append(unique, value)
		}
	}
	return unique
}

type datedNumber struct {
	date  int64
	value float64
}

func (d *rowDecoder) datedNumbers(name string, sortByDate bool) ([]int64, []float64) {
	value := d.raw(name)
	if value == "" {
		return nil, nil
	}
	parts := strings.Fields(value)
	entries := make([]datedNumber, 0, len(parts))
	for _, part := range parts {
		if strings.Count(part, ",") != 1 {
			d.add(name, "invalid date/number entry: "+part)
			continue
		}
		dateValue, numberValue, _ := strings.Cut(part, ",")
		date, dateErr := models.ParseDate(dateValue)
		number, numberErr := parseFloat(numberValue)
		if dateErr != nil || numberErr != nil {
			d.add(name, "invalid date/number entry: "+part)
			continue
		}
		entries = append(entries, datedNumber{date: date.Time.Unix(), value: number})
	}
	if sortByDate {
		sort.SliceStable(entries, func(i, j int) bool { return entries[i].date < entries[j].date })
	}
	dates := make([]int64, 0, len(entries))
	numbers := make([]float64, 0, len(entries))
	for _, entry := range entries {
		dates = append(dates, entry.date)
		numbers = append(numbers, entry.value)
	}
	return dates, numbers
}

func (d *rowDecoder) add(name, reason string) {
	d.reasons = append(d.reasons, fmt.Sprintf("%s %s", name, reason))
}
