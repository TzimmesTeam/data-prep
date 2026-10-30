package prep

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/TzimmesTeam/data-prep/internal/domain/models"
)

type Parser struct{}

func NewParser() *Parser {
	return &Parser{}
}

func (p *Parser) Parse(r io.Reader) ([]models.Deal, []models.RejectedDeal, error) {
	reader := csv.NewReader(r)
	reader.Comma = ';'
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1

	headers, err := reader.Read()
	if err != nil {
		return nil, nil, fmt.Errorf("read headers: %w", err)
	}

	colIndex := make(map[string]int, len(headers))
	for i, h := range headers {
		colIndex[strings.TrimSpace(h)] = i
	}

	var deals []models.Deal
	var rejected []models.RejectedDeal

	lineNum := 1
	for {
		row, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("read row %d: %w", lineNum, err)
		}
		lineNum++

		deal, reason := parseDeal(row, colIndex)
		if reason != "" {
			rejected = append(rejected, models.RejectedDeal{
				DealID: deal.DealID,
				Reason: reason,
			})
			continue
		}
		deals = append(deals, deal)
	}

	return deals, rejected, nil
}

func parseDeal(row []string, colIndex map[string]int) (models.Deal, string) {
	deal := models.Deal{}

	getStr := func(key string) string {
		idx, ok := colIndex[key]
		if !ok || idx >= len(row) {
			return ""
		}
		return strings.TrimSpace(row[idx])
	}

	deal.DealID = getStr("deal_id")
	if deal.DealID == "" {
		return deal, "deal_id is empty"
	}

	deal.Discriminator = getStr("discriminator")
	if deal.Discriminator == "" {
		return deal, "discriminator is empty"
	}
	if deal.Discriminator != "ANN" && deal.Discriminator != "RGM" && deal.Discriminator != "SIMPLE" {
		return deal, fmt.Sprintf("invalid discriminator: %s", deal.Discriminator)
	}

	deal.CurrencyID = getStr("currency_id")

	principalStr := getStr("current_principal_amt")
	if principalStr == "" {
		return deal, "current_principal_amt is empty"
	}
	principal, err := strconv.ParseFloat(principalStr, 64)
	if err != nil {
		return deal, fmt.Sprintf("invalid current_principal_amt: %s", principalStr)
	}
	if principal <= 0 {
		return deal, "current_principal_amt must be positive"
	}
	deal.CurrentPrincipalAmt = principal

	dealDtStr := getStr("deal_dttm")
	if dealDtStr == "" {
		return deal, "deal_dttm is empty"
	}
	dealDt, err := parseDate(dealDtStr)
	if err != nil {
		return deal, fmt.Sprintf("invalid deal_dttm: %s", dealDtStr)
	}
	deal.DealDt = dealDt

	maturityStr := getStr("maturity_dttm")
	if maturityStr == "" {
		return deal, "maturity_dttm is empty"
	}
	maturityDt, err := parseDate(maturityStr)
	if err != nil {
		return deal, fmt.Sprintf("invalid maturity_dttm: %s", maturityStr)
	}
	if !maturityDt.After(dealDt) {
		return deal, "maturity_dttm <= deal_dttm"
	}
	deal.MaturityDt = maturityDt

	deal.RateTypeCd = getStr("rate_type_cd")
	if deal.RateTypeCd == "" {
		return deal, "rate_type_cd is empty"
	}
	if deal.RateTypeCd != "FIXED" && deal.RateTypeCd != "FLOAT" {
		return deal, fmt.Sprintf("invalid rate_type_cd: %s", deal.RateTypeCd)
	}

	rateStr := getStr("interest_rate")
	if rateStr == "" {
		return deal, "interest_rate is empty"
	}
	rate, err := strconv.ParseFloat(rateStr, 64)
	if err != nil {
		return deal, fmt.Sprintf("invalid interest_rate: %s", rateStr)
	}
	if rate < 0 {
		return deal, "interest_rate is negative"
	}
	deal.InterestRate = rate

	deal.DayConventionCd = getStr("day_convention_cd")
	if deal.DayConventionCd == "" {
		return deal, "day_convention_cd is empty"
	}
	valid := map[string]bool{"ACT/365": true, "ACT/ACT": true, "ACT/ACT ISDA": true}
	if !valid[deal.DayConventionCd] {
		return deal, fmt.Sprintf("invalid day_convention_cd: %s", deal.DayConventionCd)
	}

	if startStr := getStr("regular_payment_rule_start_dttm"); startStr != "" {
		if t, err := parseDate(startStr); err == nil {
			deal.RegularPaymentRuleStartDt = &t
		}
	}

	deal.RegularPaymentRulePeriodCd = getStr("regular_payment_rule_period_cd")
	deal.Product = getStr("product")
	deal.BehaviorModelCd = getStr("behavior_model_cd")

	return deal, ""
}

func parseDate(s string) (time.Time, error) {
	layouts := []string{"02.01.2006", "2006-01-02", "02/01/2006"}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unknown date format: %s", s)
}
