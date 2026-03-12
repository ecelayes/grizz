package engine

import (
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestApplyMelt(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"a", "b"}, nil))
	df.AddSeries(series.NewInt64Series("x", memory.DefaultAllocator, []int64{1, 2}, nil))
	df.AddSeries(series.NewInt64Series("y", memory.DefaultAllocator, []int64{3, 4}, nil))

	result, err := applyMelt(df, []string{"id"}, []string{"x", "y"})
	if err != nil {
		t.Fatalf("applyMelt failed: %v", err)
	}
	if result.NumRows() != 4 {
		t.Errorf("Expected 4 rows, got %d", result.NumRows())
	}
	if result.NumCols() != 3 {
		t.Errorf("Expected 3 columns (id, variable, value), got %d", result.NumCols())
	}
}

func TestApplyMeltSingleValueVar(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"a", "b", "c"}, nil))
	df.AddSeries(series.NewInt64Series("val", memory.DefaultAllocator, []int64{10, 20, 30}, nil))

	result, err := applyMelt(df, []string{"id"}, []string{"val"})
	if err != nil {
		t.Fatalf("applyMelt failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplyMeltMultipleIdVars(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("key1", memory.DefaultAllocator, []string{"a", "b"}, nil))
	df.AddSeries(series.NewStringSeries("key2", memory.DefaultAllocator, []string{"x", "y"}, nil))
	df.AddSeries(series.NewInt64Series("val1", memory.DefaultAllocator, []int64{1, 2}, nil))
	df.AddSeries(series.NewInt64Series("val2", memory.DefaultAllocator, []int64{3, 4}, nil))

	result, err := applyMelt(df, []string{"key1", "key2"}, []string{"val1", "val2"})
	if err != nil {
		t.Fatalf("applyMelt failed: %v", err)
	}
	if result.NumRows() != 4 {
		t.Errorf("Expected 4 rows, got %d", result.NumRows())
	}
}
