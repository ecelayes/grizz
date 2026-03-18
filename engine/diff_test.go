package engine

import (
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestDiffInt64(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("value", memory.DefaultAllocator, []int64{10, 20, 30, 40}, nil))

	result, err := applyDiff(df, expr.Diff(expr.Col("value")), memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("applyDiff failed: %v", err)
	}

	intResult := result.(*series.Int64Series)
	if intResult.Len() != 4 {
		t.Errorf("Expected 4 elements, got %d", intResult.Len())
	}
	if !intResult.IsNull(0) {
		t.Error("First element should be null (no previous value)")
	}
	if intResult.Value(1) != 10 {
		t.Errorf("Expected 10, got %d", intResult.Value(1))
	}
}

func TestDiffFloat64(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.0, 3.0, 6.0, 10.0}, nil))

	result, err := applyDiff(df, expr.Diff(expr.Col("value")), memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("applyDiff failed: %v", err)
	}

	floatResult := result.(*series.Float64Series)
	if floatResult.Len() != 4 {
		t.Errorf("Expected 4 elements, got %d", floatResult.Len())
	}
	if floatResult.Value(1) != 2.0 {
		t.Errorf("Expected 2.0, got %f", floatResult.Value(1))
	}
}

func TestDiffPeriods(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("value", memory.DefaultAllocator, []int64{10, 20, 30, 40, 50}, nil))

	result, err := applyDiff(df, expr.DiffPeriods(expr.Col("value"), 2), memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("applyDiff with periods failed: %v", err)
	}

	intResult := result.(*series.Int64Series)
	if intResult.Len() != 5 {
		t.Errorf("Expected 5 elements, got %d", intResult.Len())
	}
	if !intResult.IsNull(0) && !intResult.IsNull(1) {
		t.Error("First two elements should be null")
	}
}

func TestPctChangeInt64(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("value", memory.DefaultAllocator, []int64{100, 200, 400, 800}, nil))

	result, err := applyPctChange(df, expr.PctChange(expr.Col("value")), memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("applyPctChange failed: %v", err)
	}

	floatResult := result.(*series.Float64Series)
	if floatResult.Len() != 4 {
		t.Errorf("Expected 4 elements, got %d", floatResult.Len())
	}
	if floatResult.Value(1) != 1.0 {
		t.Errorf("Expected 1.0 (100%% change), got %f", floatResult.Value(1))
	}
}

func TestPctChangeFloat64(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{100.0, 150.0, 225.0}, nil))

	result, err := applyPctChange(df, expr.PctChange(expr.Col("value")), memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("applyPctChange failed: %v", err)
	}

	floatResult := result.(*series.Float64Series)
	if floatResult.Len() != 3 {
		t.Errorf("Expected 3 elements, got %d", floatResult.Len())
	}
	if floatResult.Value(1) != 0.5 {
		t.Errorf("Expected 0.5 (50%% change), got %f", floatResult.Value(1))
	}
}
