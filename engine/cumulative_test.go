package engine

import (
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestCumSum(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3, 4}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.CumSum(expr.Col("a")).Alias("cumsum"),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}

	resCol, err := result.ColByName("cumsum")
	if err != nil {
		t.Fatalf("ColByName failed: %v", err)
	}
	res := resCol.(*series.Float64Series)

	if res.Value(0) != 1 {
		t.Errorf("Expected 1 at index 0, got %v", res.Value(0))
	}
	if res.Value(1) != 3 {
		t.Errorf("Expected 3 at index 1, got %v", res.Value(1))
	}
	if res.Value(2) != 6 {
		t.Errorf("Expected 6 at index 2, got %v", res.Value(2))
	}
	if res.Value(3) != 10 {
		t.Errorf("Expected 10 at index 3, got %v", res.Value(3))
	}
}

func TestCumProd(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("a", memory.DefaultAllocator, []float64{2, 3, 4}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.CumProd(expr.Col("a")).Alias("cumprod"),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}

	resCol, err := result.ColByName("cumprod")
	if err != nil {
		t.Fatalf("ColByName failed: %v", err)
	}
	res := resCol.(*series.Float64Series)

	if res.Value(0) != 2 {
		t.Errorf("Expected 2 at index 0, got %v", res.Value(0))
	}
	if res.Value(1) != 6 {
		t.Errorf("Expected 6 at index 1, got %v", res.Value(1))
	}
	if res.Value(2) != 24 {
		t.Errorf("Expected 24 at index 2, got %v", res.Value(2))
	}
}

func TestCumMin(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("a", memory.DefaultAllocator, []float64{5, 3, 4, 1}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.CumMin(expr.Col("a")).Alias("cummin"),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}

	resCol, err := result.ColByName("cummin")
	if err != nil {
		t.Fatalf("ColByName failed: %v", err)
	}
	res := resCol.(*series.Float64Series)

	if res.Value(0) != 5 {
		t.Errorf("Expected 5 at index 0, got %v", res.Value(0))
	}
	if res.Value(1) != 3 {
		t.Errorf("Expected 3 at index 1, got %v", res.Value(1))
	}
	if res.Value(2) != 3 {
		t.Errorf("Expected 3 at index 2, got %v", res.Value(2))
	}
	if res.Value(3) != 1 {
		t.Errorf("Expected 1 at index 3, got %v", res.Value(3))
	}
}

func TestCumMax(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("a", memory.DefaultAllocator, []float64{1, 4, 2, 5}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.CumMax(expr.Col("a")).Alias("cummax"),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}

	resCol, err := result.ColByName("cummax")
	if err != nil {
		t.Fatalf("ColByName failed: %v", err)
	}
	res := resCol.(*series.Float64Series)

	if res.Value(0) != 1 {
		t.Errorf("Expected 1 at index 0, got %v", res.Value(0))
	}
	if res.Value(1) != 4 {
		t.Errorf("Expected 4 at index 1, got %v", res.Value(1))
	}
	if res.Value(2) != 4 {
		t.Errorf("Expected 4 at index 2, got %v", res.Value(2))
	}
	if res.Value(3) != 5 {
		t.Errorf("Expected 5 at index 3, got %v", res.Value(3))
	}
}
