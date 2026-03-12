package engine

import (
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestApplyWithColumnsCoalesce(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{10, 0, 30}, []bool{true, false, true}))
	df.AddSeries(series.NewInt64Series("b", memory.DefaultAllocator, []int64{0, 20, 30}, []bool{false, true, true}))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Coalesce(expr.Col("a"), expr.Col("b")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 3 {
		t.Errorf("Expected 3 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsCoalesceFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("a", memory.DefaultAllocator, []float64{10.5, 0.0, 30.5}, []bool{true, false, true}))
	df.AddSeries(series.NewFloat64Series("b", memory.DefaultAllocator, []float64{0.0, 20.5, 30.5}, []bool{false, true, true}))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Coalesce(expr.Col("a"), expr.Col("b")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 3 {
		t.Errorf("Expected 3 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsCoalesceString(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("a", memory.DefaultAllocator, []string{"hello", "", "world"}, []bool{true, false, true}))
	df.AddSeries(series.NewStringSeries("b", memory.DefaultAllocator, []string{"", "foo", "bar"}, []bool{false, true, true}))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Coalesce(expr.Col("a"), expr.Col("b")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 3 {
		t.Errorf("Expected 3 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsCoalesceBoolean(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewBooleanSeries("a", memory.DefaultAllocator, []bool{true, false, true}, []bool{true, false, true}))
	df.AddSeries(series.NewBooleanSeries("b", memory.DefaultAllocator, []bool{false, true, false}, []bool{false, true, true}))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Coalesce(expr.Col("a"), expr.Col("b")),
	})
	if err != nil {
		t.Fatalf("Coalesce with Boolean should work: %v", err)
	}
	if result.NumCols() != 3 {
		t.Errorf("Expected 3 columns, got %d", result.NumCols())
	}
}
