package models

import (
	"encoding/json"
	"fmt"
	"time"
)

const DateLayout = "2006-01-02"

type Date struct {
	time.Time
}

func NewDate(t time.Time) Date {
	return Date{Time: time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)}
}

func ParseDate(value string) (Date, error) {
	for _, layout := range []string{DateLayout, "02.01.2006", "02/01/2006"} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return NewDate(parsed), nil
		}
	}
	return Date{}, fmt.Errorf("unknown date format: %s", value)
}

func (d Date) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Format(DateLayout))
}

func (d *Date) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	parsed, err := ParseDate(value)
	if err != nil {
		return err
	}
	*d = parsed
	return nil
}
