package models

import "time"

type Deal struct {
	DealID                     string     `json:"deal_id"`
	Discriminator              string     `json:"discriminator"`
	CurrencyID                 string     `json:"currency_id"`
	CurrentPrincipalAmt        float64    `json:"current_principal_amt"`
	DealDt                     time.Time  `json:"deal_dttm"`
	MaturityDt                 time.Time  `json:"maturity_dttm"`
	RateTypeCd                 string     `json:"rate_type_cd"`
	InterestRate               float64    `json:"interest_rate"`
	DayConventionCd            string     `json:"day_convention_cd"`
	RegularPaymentRuleStartDt  *time.Time `json:"regular_payment_rule_start_dttm,omitempty"`
	RegularPaymentRulePeriodCd string     `json:"regular_payment_rule_period_cd,omitempty"`
	Product                    string     `json:"product,omitempty"`
	BehaviorModelCd            string     `json:"behavior_model_cd,omitempty"`
}
