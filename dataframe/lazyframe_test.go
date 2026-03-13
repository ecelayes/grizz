package dataframe

import (
	"testing"

	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestLazyFrameScan(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	lf := df.Lazy()

	if lf == nil {
		t.Error("Expected LazyFrame not nil")
	}
}

func TestLazyFrameExplain(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	lf := df.Lazy()

	explain := lf.Explain()

	if len(explain) == 0 {
		t.Error("Expected non-empty explain string")
	}
}

func TestLazyFrameFilterExplain(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	lf := df.Lazy()
	lf = lf.Filter(expr.Col("a").Gt(expr.Lit(2)))

	explain := lf.Explain()

	if len(explain) == 0 {
		t.Error("Expected non-empty explain string")
	}
}

func TestLazyFrameSelectExplain(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	df.AddSeries(series.NewInt64Series("b", memory.DefaultAllocator, []int64{4, 5, 6}, nil))

	lf := df.Lazy()
	lf = lf.Select(expr.Col("a"))

	explain := lf.Explain()

	if len(explain) == 0 {
		t.Error("Expected non-empty explain string")
	}
}
