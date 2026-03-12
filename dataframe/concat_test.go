package dataframe

import (
	"testing"

	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestDataFrameConcat(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewInt64Series("id", memory.DefaultAllocator, []int64{1, 2}, nil))
	df1.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob"}, nil))

	df2 := New()
	df2.AddSeries(series.NewInt64Series("id", memory.DefaultAllocator, []int64{3}, nil))
	df2.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Charlie"}, nil))

	concat, err := df1.Concat(df2)
	if err != nil {
		t.Errorf("Concat failed: %v", err)
	}
	if concat.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", concat.NumRows())
	}
}

func TestDataFrameUnion(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewInt64Series("id", memory.DefaultAllocator, []int64{1, 2}, nil))

	df2 := New()
	df2.AddSeries(series.NewInt64Series("id", memory.DefaultAllocator, []int64{3, 4}, nil))

	union, err := df1.Union(df2)
	if err != nil {
		t.Errorf("Union failed: %v", err)
	}
	if union.NumRows() != 4 {
		t.Errorf("Expected 4 rows, got %d", union.NumRows())
	}
}

func TestConcatFloat(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewFloat64Series("score", memory.DefaultAllocator, []float64{1.0, 2.0}, nil))
	df1.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob"}, nil))

	df2 := New()
	df2.AddSeries(series.NewFloat64Series("score", memory.DefaultAllocator, []float64{3.0}, nil))
	df2.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Charlie"}, nil))

	concat, err := df1.Concat(df2)
	if err != nil {
		t.Fatalf("Concat failed: %v", err)
	}
	if concat.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", concat.NumRows())
	}
}

func TestConcatBool(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewBooleanSeries("active", memory.DefaultAllocator, []bool{true, false}, nil))

	df2 := New()
	df2.AddSeries(series.NewBooleanSeries("active", memory.DefaultAllocator, []bool{true}, nil))

	concat, err := df1.Concat(df2)
	if err != nil {
		t.Fatalf("Concat failed: %v", err)
	}
	if concat.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", concat.NumRows())
	}
}

func TestConcatMultiple(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewInt64Series("id", memory.DefaultAllocator, []int64{1}, nil))

	df2 := New()
	df2.AddSeries(series.NewInt64Series("id", memory.DefaultAllocator, []int64{2}, nil))

	df3 := New()
	df3.AddSeries(series.NewInt64Series("id", memory.DefaultAllocator, []int64{3}, nil))

	tmp, err := df1.Concat(df2)
	if err != nil {
		t.Fatalf("First Concat failed: %v", err)
	}
	result, err := tmp.Concat(df3)
	if err != nil {
		t.Fatalf("Second Concat failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestConcatError(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewInt64Series("id", memory.DefaultAllocator, []int64{1, 2}, nil))

	df2 := New()
	df2.AddSeries(series.NewInt64Series("id", memory.DefaultAllocator, []int64{3}, nil))
	df2.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"a"}, nil))

	_, err := df1.Concat(df2)
	if err == nil {
		t.Error("Expected error for mismatched columns")
	}
}
