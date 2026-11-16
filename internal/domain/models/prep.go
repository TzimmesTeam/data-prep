package models

type Delivery struct {
	Bucket     string
	Key        string
	Source     string
	IngestDate string
	SnapshotID string
}

type Stats struct {
	DealsIn       int
	DealsOK       int
	DealsRejected int
}

type RejectedDeal struct {
	DealID  *string  `parquet:"deal_id,optional"`
	Row     int64    `parquet:"row"`
	Reasons []string `parquet:"reasons"`
}

type Source struct {
	URI       string
	Revision  string
	VersionID string
	ETag      string
	SizeBytes int64
}

type PrepResult struct {
	Delivery      Delivery
	Source        Source
	CuratedURI    string
	QuarantineURI string
	Stats         Stats
}

type Callback struct {
	SnapshotID string `json:"snapshot_id"`
}
