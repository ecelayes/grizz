package engine

import (
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestApplyProjection(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	df.AddSeries(series.NewInt64Series("b", memory.DefaultAllocator, []int64{4, 5, 6}, nil))

	result, err := applyProjection(df, []expr.Expr{expr.Col("a")})
	if err != nil {
		t.Fatalf("applyProjection failed: %v", err)
	}
	if result.NumCols() != 1 {
		t.Errorf("Expected 1 column, got %d", result.NumCols())
	}
}

func TestApplyProjectionFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("a", memory.DefaultAllocator, []float64{1.5, 2.5, 3.5}, nil))
	df.AddSeries(series.NewFloat64Series("b", memory.DefaultAllocator, []float64{4.5, 5.5, 6.5}, nil))

	result, err := applyProjection(df, []expr.Expr{expr.Col("a")})
	if err != nil {
		t.Fatalf("applyProjection failed: %v", err)
	}
	if result.NumCols() != 1 {
		t.Errorf("Expected 1 column, got %d", result.NumCols())
	}
	valCol, _ := result.ColByName("a")
	if valCol.(*series.Float64Series).Value(0) != 1.5 {
		t.Errorf("Expected first value 1.5, got %f", valCol.(*series.Float64Series).Value(0))
	}
}

func TestApplyProjectionBool(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewBooleanSeries("a", memory.DefaultAllocator, []bool{true, false, true}, nil))
	df.AddSeries(series.NewBooleanSeries("b", memory.DefaultAllocator, []bool{false, true, false}, nil))

	result, err := applyProjection(df, []expr.Expr{expr.Col("a")})
	if err != nil {
		t.Fatalf("applyProjection failed: %v", err)
	}
	if result.NumCols() != 1 {
		t.Errorf("Expected 1 column, got %d", result.NumCols())
	}
}

func TestApplyProjectionString(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("a", memory.DefaultAllocator, []string{"x", "y", "z"}, nil))
	df.AddSeries(series.NewStringSeries("b", memory.DefaultAllocator, []string{"p", "q", "r"}, nil))

	result, err := applyProjection(df, []expr.Expr{expr.Col("a")})
	if err != nil {
		t.Fatalf("applyProjection failed: %v", err)
	}
	if result.NumCols() != 1 {
		t.Errorf("Expected 1 column, got %d", result.NumCols())
	}
}

func TestApplyProjectionNonColumn(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30}, nil))

	_, err := applyProjection(df, []expr.Expr{expr.Lit(10)})
	if err == nil {
		t.Error("Expected error for non-column expression in projection")
	}
}

func TestApplyProjectionNonExistentColumn(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30}, nil))

	_, err := applyProjection(df, []expr.Expr{expr.Col("nonexistent")})
	if err == nil {
		t.Error("Expected error for non-existent column in projection")
	}
}
