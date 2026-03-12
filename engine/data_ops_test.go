package engine

import (
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestApplyDropNulls(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 0, 30, 0}, []bool{true, false, true, false}))

	result, err := applyDropNulls(df)
	if err != nil {
		t.Fatalf("applyDropNulls failed: %v", err)
	}
	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestApplyDistinct(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 10, 20, 20, 30}, nil))

	result, err := applyDistinct(df)
	if err != nil {
		t.Fatalf("applyDistinct failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplyDistinctFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.5, 1.5, 2.5, 3.5}, nil))

	result, err := applyDistinct(df)
	if err != nil {
		t.Fatalf("applyDistinct failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplyDistinctBool(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewBooleanSeries("flag", memory.DefaultAllocator, []bool{true, true, false, false, true}, nil))

	result, err := applyDistinct(df)
	if err != nil {
		t.Fatalf("applyDistinct failed: %v", err)
	}
	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}
