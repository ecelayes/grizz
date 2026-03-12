package engine

import (
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestCollectLazyFrame(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3, 4, 5}, nil))

	lf := df.Lazy().Filter(expr.Col("a").Gt(expr.Lit(2)))

	result, err := Collect(lf)
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestCollectWithSelect(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	df.AddSeries(series.NewStringSeries("b", memory.DefaultAllocator, []string{"x", "y", "z"}, nil))

	lf := df.Lazy().Select(expr.Col("a"))

	result, err := Collect(lf)
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}
	if result.NumCols() != 1 {
		t.Errorf("Expected 1 column, got %d", result.NumCols())
	}
}
