package models

type PrepRequest struct {
	RunID        string `json:"run_id"`
	BatchID      string `json:"batch_id"`
	Attempt      int    `json:"attempt"`
	SourceURI    string `json:"source_uri"`
	OutputPrefix string `json:"output_prefix"`
}

type Stats struct {
	DealsIn       int `json:"deals_in"`
	DealsOK       int `json:"deals_ok"`
	DealsRejected int `json:"deals_rejected"`
	PartsOut      int `json:"parts_out"`
}

type RejectedDeal struct {
	DealID  string   `json:"deal_id,omitempty"`
	Row     int      `json:"row"`
	Reasons []string `json:"reasons"`
}

type Source struct {
	URI       string `json:"uri"`
	Revision  string `json:"revision"`
	VersionID string `json:"version_id,omitempty"`
	ETag      string `json:"etag,omitempty"`
	SizeBytes int64  `json:"size_bytes"`
}

type Partition struct {
	URI       string `json:"uri"`
	SizeBytes int64  `json:"size_bytes"`
	Rows      int    `json:"rows"`
}

type Manifest struct {
	SchemaVersion int         `json:"schema_version"`
	RunID         string      `json:"run_id"`
	BatchID       string      `json:"batch_id"`
	Attempt       int         `json:"attempt"`
	Source        Source      `json:"source"`
	Deals         []Partition `json:"deals"`
	Rejected      []Partition `json:"rejected"`
	Stats         Stats       `json:"stats"`
}

type PrepResult struct {
	RunID          string `json:"run_id"`
	BatchID        string `json:"batch_id"`
	Attempt        int    `json:"attempt"`
	SourceRevision string `json:"source_revision,omitempty"`
	Status         string `json:"status"`
	ManifestURI    string `json:"manifest_uri,omitempty"`
	Stats          Stats  `json:"stats"`
	Error          string `json:"error,omitempty"`
}
