package models

import (
	"fmt"
	"path"
	"strings"
)

type SourceFormat int

const (
	FormatCSV SourceFormat = iota + 1
	FormatXLSX
)

func (f SourceFormat) String() string {
	switch f {
	case FormatCSV:
		return "csv"
	case FormatXLSX:
		return "xlsx"
	default:
		return "unknown"
	}
}

func SourceFormatFromKey(key string) (SourceFormat, error) {
	switch strings.ToLower(path.Ext(key)) {
	case ".csv":
		return FormatCSV, nil
	case ".xlsx", ".xlsm":
		return FormatXLSX, nil
	default:
		return 0, fmt.Errorf("unsupported source format of %q: expected .csv, .xlsx or .xlsm", key)
	}
}
