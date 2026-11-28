package prep

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

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

var requiredHeaders = []string{
	"report_dttm", "deal_id", "discriminator", "current_principal_amt",
	"maturity_dttm", "rate_type_cd", "interest_rate", "day_convention_cd",
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
	Deal(*models.Deal) error
	Rejected(models.RejectedDeal) error
}

type Parser struct {
	log *slog.Logger
}

func NewParser(log *slog.Logger) *Parser {
	return &Parser{log: log}
}

func (p *Parser) Parse(r io.Reader, format models.SourceFormat, sink Sink) error {
	reader, err := newRowReader(r, format)
	if err != nil {
		return err
	}
	parseErr := p.parse(reader, sink)
	closeErr := reader.Close()
	if parseErr != nil {
		return parseErr
	}
	return closeErr
}

func (p *Parser) parse(reader rowReader, sink Sink) error {
	columns, err := p.readColumns(reader)
	if err != nil {
		return err
	}
	excelDates, date1904 := reader.ExcelDates()
	opts := decodeOptions{excel: excelDates, date1904: date1904}

	seen := make(map[int64]struct{})
	for {
		eof, err := p.parseNext(reader, columns, opts, seen, sink)
		if err != nil {
			return err
		}
		if eof {
			return nil
		}
	}
}

func (p *Parser) readColumns(reader rowReader) (map[string]int, error) {
	headers, _, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read headers: %w", err)
	}
	columns := make(map[string]int, len(headers))
	for i, header := range headers {
		header = strings.TrimSpace(strings.TrimPrefix(header, "\ufeff"))
		if header == "" {
			continue
		}
		if _, known := knownHeaderSet[header]; !known {
			p.log.Warn("ignoring unknown header", "header", header, "position", i+1)
			continue
		}
		if _, exists := columns[header]; exists {
			return nil, fmt.Errorf("duplicate header: %s", header)
		}
		columns[header] = i
	}
	missing := make([]string, 0, len(requiredHeaders))
	for _, header := range requiredHeaders {
		if _, ok := columns[header]; !ok {
			missing = append(missing, header)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("required headers are missing: %s", strings.Join(missing, ", "))
	}
	reader.SetWidth(len(headers))
	return columns, nil
}

func (p *Parser) parseNext(reader rowReader, columns map[string]int, opts decodeOptions, seen map[int64]struct{}, sink Sink) (bool, error) {
	row, rowNumber, readErr := reader.Read()
	if errors.Is(readErr, io.EOF) {
		return true, nil
	}
	if errors.Is(readErr, errFieldCount) {
		return false, rejectMalformedRow(row, columns, sink, rowNumber, readErr)
	}
	if readErr != nil {
		return false, fmt.Errorf("read row %d: %w", rowNumber, readErr)
	}
	return false, p.processRow(row, columns, opts, seen, sink, rowNumber)
}

func rejectMalformedRow(row []string, columns map[string]int, sink Sink, rowNumber int, readErr error) error {
	rejected := models.RejectedDeal{Row: int64(rowNumber), Reasons: []string{readErr.Error()}}
	if idx, ok := columns["deal_id"]; ok && idx < len(row) {
		rejected.DealID = stringPointer(strings.TrimSpace(row[idx]))
	}
	if err := sink.Rejected(rejected); err != nil {
		return fmt.Errorf("write rejected row %d: %w", rowNumber, err)
	}
	return nil
}

func (p *Parser) processRow(row []string, columns map[string]int, opts decodeOptions, seen map[int64]struct{}, sink Sink, rowNumber int) error {
	rawDealID := strings.TrimSpace(row[columns["deal_id"]])
	decoder := rowDecoder{row: row, columns: columns, opts: opts}
	deal := parseDeal(&decoder)
	warnings := normalizeDeal(&deal, &decoder)
	reasons := decoder.reasons
	for _, warning := range warnings {
		p.log.Warn("deal normalized with warning", "row", rowNumber, "deal_id", rawDealID, "warning", warning)
	}
	if len(reasons) > 0 {
		return rejectRow(sink, rawDealID, rowNumber, reasons)
	}
	if deal.DealID != nil {
		if _, exists := seen[*deal.DealID]; exists {
			return rejectRow(sink, rawDealID, rowNumber, []string{"duplicate deal_id"})
		}
		seen[*deal.DealID] = struct{}{}
	}
	if err := sink.Deal(&deal); err != nil {
		return fmt.Errorf("write deal row %d: %w", rowNumber, err)
	}
	return nil
}

func rejectRow(sink Sink, dealID string, rowNumber int, reasons []string) error {
	rejected := models.RejectedDeal{DealID: stringPointer(dealID), Row: int64(rowNumber), Reasons: reasons}
	if err := sink.Rejected(rejected); err != nil {
		return fmt.Errorf("write rejected row %d: %w", rowNumber, err)
	}
	return nil
}

type decodeOptions struct {
	excel bool
	date1904 bool
}

type rowDecoder struct {
	row     []string
	columns map[string]int
	opts    decodeOptions
	reasons []string
}

func parseDeal(d *rowDecoder) models.Deal {
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

	deal.PrincipalPaymentDates, deal.PrincipalPaymentAmounts = d.datedNumbers("principal_payment")
	deal.RepricingDtSchedule = d.dates("repricing_dttm_schedule")
	deal.RepricingDtRateScheduleDates, deal.RepricingDtRateScheduleRates = d.datedNumbers("repricing_dttm_rate_schedule")
	deal.RepricingDtRateScheduleFixedDates, deal.RepricingDtRateScheduleFixedRates = d.datedNumbers("repricing_dttm_rate_schedule_fixed")
	return deal
}

func normalizeDeal(deal *models.Deal, d *rowDecoder) []string {
	normalizeCommon(deal, d)
	warnings := normalizeDiscriminator(deal, d)
	normalizeRateType(deal, d)
	normalizeSubsidy(deal, d)
	normalizeRepricingRules(deal, d)
	normalizeCalendars(deal)
	validateMaturity(deal, d)
	return warnings
}

func normalizeCommon(deal *models.Deal, d *rowDecoder) {
	for _, name := range requiredHeaders {
		if d.raw(name) == "" {
			d.add(name, "is required")
		}
	}
	if deal.DealDt == nil {
		if deal.AccruedInterest != nil {
			deal.DealDt = copyPointer(deal.ReportDt)
		} else {
			d.add("deal_dttm", "is required when accrued_interest is empty")
		}
	}
	if deal.DealReportDt == nil {
		deal.DealReportDt = copyPointer(deal.ReportDt)
	}
}

func normalizeDiscriminator(deal *models.Deal, d *rowDecoder) []string {
	if deal.Discriminator == nil {
		return nil
	}
	var warnings []string
	switch *deal.Discriminator {
	case 1:
		requireCalendar(deal.RegularPaymentRuleStartDt, deal.RegularPaymentRulePeriodCd, "regular_payment_rule", d)
		clearInterestRule(deal)
	case 2:
		if deal.RegularPaymentRulePeriodCd != nil {
			requireCalendar(deal.RegularPaymentRuleStartDt, deal.RegularPaymentRulePeriodCd, "regular_payment_rule", d)
		} else if deal.InterestPaymentRulePeriodCd == nil {
			warnings = append(warnings, "RGM has no regular or interest payment rule; all payments are due at maturity")
		}
	case 3:
		if deal.RegularPaymentRulePeriodCd != nil {
			requireCalendar(deal.RegularPaymentRuleStartDt, deal.RegularPaymentRulePeriodCd, "regular_payment_rule", d)
		}
	}
	if *deal.Discriminator != 1 && deal.InterestPaymentRulePeriodCd != nil {
		requireCalendar(deal.InterestPaymentRuleStartDt, deal.InterestPaymentRulePeriodCd, "interest_payment_rule", d)
	}
	return warnings
}

func normalizeRateType(deal *models.Deal, d *rowDecoder) {
	if deal.RateTypeCd == nil {
		return
	}
	switch *deal.RateTypeCd {
	case 1:
		clearFloating(deal)
	case 2:
		if deal.BaseIndicator == nil {
			d.add("base_indicator", "is required for FLOAT")
		}
	}
}

func normalizeSubsidy(deal *models.Deal, d *rowDecoder) {
	if deal.SubRateTypeCd == nil {
		clearSubsidy(deal)
		return
	}
	if deal.SubInterestRate == nil {
		d.add("sub_interest_rate", "is required for subsidy")
	}
	requireCalendar(deal.SubPaymentRuleStartDt, deal.SubPaymentRulePeriodCd, "sub_payment_rule", d)
	if *deal.SubRateTypeCd == 2 && deal.SubFloatIndicator == nil {
		d.add("sub_float_indicator", "is required for FLOAT subsidy")
	}
	if *deal.SubRateTypeCd == 2 && deal.SubCoeffIndex == nil {
		deal.SubCoeffIndex = floatPointer(1)
	}
	if *deal.SubRateTypeCd == 1 {
		clearFloatingSubsidy(deal)
	}
}

func normalizeRepricingRules(deal *models.Deal, d *rowDecoder) {
	if deal.RepricingRulePeriodCd != nil {
		requireCalendar(deal.RepricingRuleStartDt, deal.RepricingRulePeriodCd, "repricing_rule", d)
	}
	if deal.SubRepricingRulePeriodCd != nil {
		requireCalendar(deal.SubRepricingRuleStartDt, deal.SubRepricingRulePeriodCd, "sub_repricing_rule", d)
	}
}

func normalizeCalendars(deal *models.Deal) {
	normalizeCalendar(&deal.InterestPaymentRuleStartDt, deal.InterestPaymentRulePeriodCd, &deal.InterestPaymentRuleDayNum, true)
	normalizeCalendar(&deal.RegularPaymentRuleStartDt, deal.RegularPaymentRulePeriodCd, &deal.RegularPaymentRuleDayNum, true)
	normalizeCalendar(&deal.RepricingRuleStartDt, deal.RepricingRulePeriodCd, &deal.RepricingRuleDayNum, false)
	normalizeCalendar(&deal.SubPaymentRuleStartDt, deal.SubPaymentRulePeriodCd, &deal.SubPaymentRuleDayNum, true)
	normalizeCalendar(&deal.SubRepricingRuleStartDt, deal.SubRepricingRulePeriodCd, &deal.SubRepricingRuleDayNum, true)
}

func validateMaturity(deal *models.Deal, d *rowDecoder) {
	if deal.DealDt != nil && deal.MaturityDt != nil && *deal.MaturityDt <= *deal.DealDt {
		d.add("maturity_dttm", "must be after deal_dttm")
	}
}

func requireCalendar(start *int64, period *int32, prefix string, d *rowDecoder) {
	if period == nil {
		d.add(prefix+"_period_cd", "is required")
		return
	}
	if *period != 0 && start == nil {
		d.add(prefix+"_start_dttm", "is required for a non-ENDDATE period")
	}
}

func normalizeCalendar(start **int64, period *int32, day **int32, defaultDay bool) {
	if period == nil || *period == 0 {
		*start = nil
		*day = nil
		return
	}
	if *start == nil {
		return
	}
	if defaultDay && *day == nil {
		value := int32(time.Unix(**start, 0).UTC().Day()) // #nosec G115 -- calendar days are always in [1, 31].
		*day = &value
	}
}

func clearInterestRule(deal *models.Deal) {
	deal.InterestPaymentRuleStartDt = nil
	deal.InterestPaymentRulePeriodCd = nil
	deal.InterestPaymentRuleDayNum = nil
}

func clearFloating(deal *models.Deal) {
	deal.BaseIndicator = nil
	deal.CapLimitInterestRate = nil
	deal.FloorLimitInterestRate = nil
	deal.IndicatorLagCd = nil
	deal.RepricingRuleStartDt = nil
	deal.RepricingRulePeriodCd = nil
	deal.RepricingRuleDayNum = nil
	deal.RepricingDtSchedule = []int64{}
	deal.RepricingDtRateScheduleDates = []int64{}
	deal.RepricingDtRateScheduleRates = []float64{}
	deal.RepricingDtRateScheduleFixedDates = []int64{}
	deal.RepricingDtRateScheduleFixedRates = []float64{}
}

func clearSubsidy(deal *models.Deal) {
	deal.SubInterestRate = nil
	deal.SubFloatIndicator = nil
	deal.SubPaymentRulePeriodCd = nil
	deal.SubPaymentRuleDayNum = nil
	deal.SubPaymentRuleStartDt = nil
	deal.SubCoeffIndex = nil
	clearFloatingSubsidy(deal)
	deal.SubRepricingRuleFirstEventFlg = nil
}

func clearFloatingSubsidy(deal *models.Deal) {
	deal.SubFloatIndicator = nil
	deal.SubRepricingRuleStartDt = nil
	deal.SubRepricingRulePeriodCd = nil
	deal.SubRepricingRuleDayNum = nil
	deal.SubCapLimitInterestRate = nil
	deal.SubFloorLimitInterestRate = nil
	deal.SubIndicatorLagCd = nil
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
	return stringPointer(value)
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
	parsed, err := d.parseInt(value, 64)
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
	parsed, err := d.parseInt(value, 32)
	if err != nil || parsed < math.MinInt32 || parsed > math.MaxInt32 {
		d.add(name, "invalid int32: "+value)
		return nil
	}
	result := int32(parsed)
	return &result
}

const maxExactFloatInt = 1 << 53

func (d *rowDecoder) parseInt(value string, bitSize int) (int64, error) {
	parsed, err := strconv.ParseInt(value, 10, bitSize)
	if err == nil || !d.opts.excel {
		return parsed, err
	}
	number, floatErr := strconv.ParseFloat(value, 64)
	if floatErr != nil || number != math.Trunc(number) || math.Abs(number) > maxExactFloatInt {
		return 0, err
	}
	limit := float64(int64(1) << (bitSize - 1))
	if number < -limit || number >= limit {
		return 0, err
	}
	return int64(number), nil
}

func (d *rowDecoder) parseDate(value string) (models.Date, error) {
	parsed, err := models.ParseDate(value)
	if err == nil || !d.opts.excel {
		return parsed, err
	}
	serial, floatErr := strconv.ParseFloat(value, 64)
	if floatErr != nil || serial <= 0 || math.IsInf(serial, 0) {
		return models.Date{}, err
	}
	converted, ok := excelSerialDate(serial, d.opts.date1904)
	if !ok {
		return models.Date{}, err
	}
	return converted, nil
}

const maxExcelSerial = 2958465

func excelSerialDate(serial float64, date1904 bool) (models.Date, bool) {
	if serial < 1 || serial > maxExcelSerial {
		return models.Date{}, false
	}
	days := int(math.Floor(serial))
	if date1904 {
		return models.NewDate(time.Date(1904, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, days)), true
	}
	if days < 61 {
		return models.NewDate(time.Date(1899, 12, 31, 0, 0, 0, 0, time.UTC).AddDate(0, 0, days)), true
	}
	return models.NewDate(time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC).AddDate(0, 0, days)), true
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
	parsed, err := d.parseDate(value)
	if err != nil {
		d.add(name, err.Error())
		return nil
	}
	result := parsed.Unix()
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
		value = strings.TrimPrefix(value, "-")
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
	case 'Q':
		code = magnitude * 3
	case 'D':
		code = 1000000 + magnitude
	case 'W':
		code = 1000000 + magnitude*7
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
	result := make([]int64, 0)
	if value == "" {
		return result
	}
	parts := strings.Fields(value)
	result = make([]int64, 0, len(parts))
	for _, part := range parts {
		parsed, err := d.parseDate(part)
		if err != nil {
			d.add(name, err.Error())
			continue
		}
		result = append(result, parsed.Unix())
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

func (d *rowDecoder) datedNumbers(name string) ([]int64, []float64) {
	dates := make([]int64, 0)
	numbers := make([]float64, 0)
	value := d.raw(name)
	if value == "" {
		return dates, numbers
	}
	parts := strings.Fields(value)
	entries := make([]datedNumber, 0, len(parts))
	for _, part := range parts {
		if strings.Count(part, ",") != 1 {
			d.add(name, "invalid date/number entry: "+part)
			continue
		}
		dateValue, numberValue, _ := strings.Cut(part, ",")
		date, dateErr := d.parseDate(dateValue)
		number, numberErr := parseFloat(numberValue)
		if dateErr != nil || numberErr != nil {
			d.add(name, "invalid date/number entry: "+part)
			continue
		}
		entries = append(entries, datedNumber{date: date.Unix(), value: number})
	}
	sort.SliceStable(entries, func(i, j int) bool { return entries[i].date < entries[j].date })
	unique := entries[:0]
	for _, entry := range entries {
		if len(unique) > 0 && entry.date == unique[len(unique)-1].date {
			if entry.value != unique[len(unique)-1].value {
				d.add(name, fmt.Sprintf("conflicting values for date %s", time.Unix(entry.date, 0).UTC().Format(models.DateLayout)))
			}
			continue
		}
		unique = append(unique, entry)
	}
	for _, entry := range unique {
		dates = append(dates, entry.date)
		numbers = append(numbers, entry.value)
	}
	return dates, numbers
}

func (d *rowDecoder) add(name, reason string) {
	d.reasons = append(d.reasons, fmt.Sprintf("%s %s", name, reason))
}

func stringPointer(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func floatPointer(value float64) *float64 {
	return &value
}

func copyPointer[T any](value *T) *T {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
