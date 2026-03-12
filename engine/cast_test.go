package engine

import (
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	grizzarrows "github.com/ecelayes/grizz/internal/arrow"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestApplyWithColumnsCastFloatToInt(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.5, 2.7, 3.3}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Cast(expr.Col("value"), grizzarrows.Int64),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsCastIntToFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("value", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Cast(expr.Col("value"), grizzarrows.Float64),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsCastStringToInt(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("value", memory.DefaultAllocator, []string{"1", "2", "3"}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Cast(expr.Col("value"), grizzarrows.Int64),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsCastFloatToString(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.5, 2.5, 3.5}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Cast(expr.Col("value"), grizzarrows.String),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsCastIntToString(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("value", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Cast(expr.Col("value"), grizzarrows.String),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsCastStringToFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("value", memory.DefaultAllocator, []string{"1.5", "2.5", "3.5"}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Cast(expr.Col("value"), grizzarrows.Float64),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}
