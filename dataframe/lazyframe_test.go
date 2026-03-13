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

func TestLazyFrameHead(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3, 4, 5}, nil))

	lf := df.Lazy()
	result := lf.Head(3)

	if result == nil {
		t.Error("Expected LazyFrame not nil")
	}

	explain := result.Explain()
	if len(explain) == 0 {
		t.Error("Expected non-empty explain")
	}
}

func TestLazyFrameTail(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3, 4, 5}, nil))

	lf := df.Lazy()
	result := lf.Tail(2)

	if result == nil {
		t.Error("Expected LazyFrame not nil")
	}

	explain := result.Explain()
	if len(explain) == 0 {
		t.Error("Expected non-empty explain")
	}
}

func TestLazyFrameGroupByHead(t *testing.T) {
	df := New()
	df.AddSeries(series.NewStringSeries("group", memory.DefaultAllocator, []string{"a", "a", "b", "b"}, nil))
	df.AddSeries(series.NewInt64Series("value", memory.DefaultAllocator, []int64{1, 2, 3, 4}, nil))

	lf := df.Lazy()
	result := lf.GroupBy("group").Head(1)

	if result == nil {
		t.Error("Expected LazyGroupBy not nil")
	}

	explain := result.Explain()
	if len(explain) == 0 {
		t.Error("Expected non-empty explain")
	}
}

func TestLazyFrameGroupByTail(t *testing.T) {
	df := New()
	df.AddSeries(series.NewStringSeries("group", memory.DefaultAllocator, []string{"a", "a", "b", "b"}, nil))
	df.AddSeries(series.NewInt64Series("value", memory.DefaultAllocator, []int64{1, 2, 3, 4}, nil))

	lf := df.Lazy()
	result := lf.GroupBy("group").Tail(1)

	if result == nil {
		t.Error("Expected LazyGroupBy not nil")
	}

	explain := result.Explain()
	if len(explain) == 0 {
		t.Error("Expected non-empty explain")
	}
}
