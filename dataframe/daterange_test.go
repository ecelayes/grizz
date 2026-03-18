package dataframe

import (
	"testing"

	"github.com/ecelayes/grizz/series"
)

func TestDateRange(t *testing.T) {
	df, err := DateRange("2024-01-01", "2024-01-07", "1d")
	if err != nil {
		t.Fatalf("DateRange failed: %v", err)
	}

	if df.NumRows() != 7 {
		t.Errorf("Expected 7 rows, got %d", df.NumRows())
	}

	col, err := df.ColByName("date")
	if err != nil {
		t.Fatalf("ColByName failed: %v", err)
	}

	strCol := col.(*series.StringSeries)
	if strCol.Value(0) != "2024-01-01" {
		t.Errorf("Expected first date 2024-01-01, got %s", strCol.Value(0))
	}
	if strCol.Value(6) != "2024-01-07" {
		t.Errorf("Expected last date 2024-01-07, got %s", strCol.Value(6))
	}
}

func TestDateRangeHours(t *testing.T) {
	df, err := DateTimeRange("2024-01-01T00:00:00", "2024-01-01T03:00:00", "1h")
	if err != nil {
		t.Fatalf("DateTimeRange failed: %v", err)
	}

	if df.NumRows() != 4 {
		t.Errorf("Expected 4 rows, got %d", df.NumRows())
	}
}

func TestDateRangeInvalidInterval(t *testing.T) {
	_, err := DateRange("2024-01-01", "2024-01-07", "invalid")
	if err == nil {
		t.Error("Expected error for invalid interval")
	}
}
