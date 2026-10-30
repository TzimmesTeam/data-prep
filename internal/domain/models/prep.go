package models

type PrepRequest struct {
	RunID   string `json:"run_id"`
	BatchID string `json:"batch_id"`
	Attempt int    `json:"attempt"`
}

type PrepResponse struct {
	RunID       string   `json:"run_id"`
	BatchID     string   `json:"batch_id"`
	Attempt     int      `json:"attempt"`
	Status      string   `json:"status"` // "success" | "error"
	Stats       Stats    `json:"stats"`
	Partitions  [][]Deal `json:"partitions"`
	RejectedIDs []string `json:"rejected_ids"`
	Errors      []string `json:"errors,omitempty"`
}

type Stats struct {
	DealsIn       int `json:"deals_in"`
	DealsOK       int `json:"deals_ok"`
	DealsRejected int `json:"deals_rejected"`
}

type RejectedDeal struct {
	DealID string `json:"deal_id"`
	Reason string `json:"reason"`
}
