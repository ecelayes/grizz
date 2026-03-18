package dataframe

import (
	"fmt"
	"time"

	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func DateRange(start, end string, interval string) (*DataFrame, error) {
	startTime, err := time.Parse("2006-01-02", start)
	if err != nil {
		return nil, err
	}

	endTime, err := time.Parse("2006-01-02", end)
	if err != nil {
		return nil, err
	}

	duration, err := parseInterval(interval)
	if err != nil {
		return nil, err
	}

	var dates []string
	for t := startTime; !t.After(endTime); t = t.Add(duration) {
		dates = append(dates, t.Format("2006-01-02"))
	}

	df := New()
	dateSeries := series.NewStringSeries("date", memory.DefaultAllocator, dates, nil)
	df.AddSeries(dateSeries)
	return df, nil
}

func DateTimeRange(start, end string, interval string) (*DataFrame, error) {
	startTime, err := time.Parse("2006-01-02T15:04:05", start)
	if err != nil {
		return nil, err
	}

	endTime, err := time.Parse("2006-01-02T15:04:05", end)
	if err != nil {
		return nil, err
	}

	duration, err := parseInterval(interval)
	if err != nil {
		return nil, err
	}

	var dates []string
	for t := startTime; !t.After(endTime); t = t.Add(duration) {
		dates = append(dates, t.Format("2006-01-02T15:04:05"))
	}

	df := New()
	dateSeries := series.NewStringSeries("datetime", memory.DefaultAllocator, dates, nil)
	df.AddSeries(dateSeries)
	return df, nil
}

func parseInterval(interval string) (time.Duration, error) {
	switch interval {
	case "1s":
		return time.Second, nil
	case "1m":
		return time.Minute, nil
	case "1h":
		return time.Hour, nil
	case "1d":
		return 24 * time.Hour, nil
	case "1w":
		return 7 * 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("unsupported interval: %s", interval)
	}
}
