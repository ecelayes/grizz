package engine

import (
	"math"
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestEwmMeanInt64(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("value", memory.DefaultAllocator, []int64{1, 2, 3, 4, 5}, nil))

	result, err := applyEwmMean(df, expr.EwmMean(expr.Col("value")), memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("applyEwmMean failed: %v", err)
	}

	floatResult := result.(*series.Float64Series)
	if floatResult.Len() != 5 {
		t.Errorf("Expected 5 elements, got %d", floatResult.Len())
	}
	if floatResult.Value(0) != 1.0 {
		t.Errorf("Expected first value to be 1.0, got %f", floatResult.Value(0))
	}
}

func TestEwmMeanFloat64(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0, 4.0, 5.0}, nil))

	result, err := applyEwmMean(df, expr.EwmMean(expr.Col("value")), memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("applyEwmMean failed: %v", err)
	}

	floatResult := result.(*series.Float64Series)
	if floatResult.Len() != 5 {
		t.Errorf("Expected 5 elements, got %d", floatResult.Len())
	}
	if floatResult.Value(0) != 1.0 {
		t.Errorf("Expected first value to be 1.0, got %f", floatResult.Value(0))
	}
	if math.Abs(floatResult.Value(1)-1.5) > 0.001 {
		t.Errorf("Expected second value ~1.5, got %f", floatResult.Value(1))
	}
}

func TestEwmMeanAlpha(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0}, nil))

	result, err := applyEwmMean(df, expr.EwmMeanAlpha(expr.Col("value"), 0.3), memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("applyEwmMean with alpha failed: %v", err)
	}

	floatResult := result.(*series.Float64Series)
	if math.Abs(floatResult.Value(0)-1.0) > 0.001 {
		t.Errorf("Expected first value to be 1.0, got %f", floatResult.Value(0))
	}
}

func TestEwmMeanMinPeriods(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0, 4.0}, nil))

	result, err := applyEwmMean(df, expr.EwmMeanAlphaMinPeriods(expr.Col("value"), 0.5, 3), memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("applyEwmMean with minPeriods failed: %v", err)
	}

	floatResult := result.(*series.Float64Series)
	if floatResult.Len() != 4 {
		t.Errorf("Expected 4 elements, got %d", floatResult.Len())
	}
	if !floatResult.IsNull(0) || !floatResult.IsNull(1) {
		t.Error("First two values should be null with minPeriods=3")
	}
}
