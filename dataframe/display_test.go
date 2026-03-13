package dataframe

import (
	"testing"

	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestDataFrameString(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	df.AddSeries(series.NewFloat64Series("b", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0}, nil))

	df.Show()
}

func TestDataFrameEmpty(t *testing.T) {
	df := New()

	if df.NumRows() != 0 {
		t.Errorf("Expected 0 rows, got %d", df.NumRows())
	}
	if df.NumCols() != 0 {
		t.Errorf("Expected 0 cols, got %d", df.NumCols())
	}
}
