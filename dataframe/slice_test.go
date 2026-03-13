package dataframe

import (
	"testing"

	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestSlice(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3, 4, 5}, nil))

	result := df.Slice(1, 2)

	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}

	col, _ := result.Col(0)
	intCol := col.(*series.Int64Series)
	if intCol.Value(0) != 2 {
		t.Errorf("Expected first value 2, got %d", intCol.Value(0))
	}
}

func TestSliceFromBeginning(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3, 4, 5}, nil))

	result := df.Slice(0, 3)

	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestSliceExceedsLength(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	result := df.Slice(1, 10)

	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestSliceNegativeOffset(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3, 4, 5}, nil))

	result := df.Slice(-1, 2)

	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestSliceOffsetExceedsRows(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	result := df.Slice(10, 2)

	if result.NumRows() != 0 {
		t.Errorf("Expected 0 rows, got %d", result.NumRows())
	}
}

func TestSliceZeroLength(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	result := df.Slice(0, 0)

	if result.NumRows() != 0 {
		t.Errorf("Expected 0 rows, got %d", result.NumRows())
	}
}

func TestSliceMultipleColumns(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3, 4, 5}, nil))
	df.AddSeries(series.NewStringSeries("b", memory.DefaultAllocator, []string{"a", "b", "c", "d", "e"}, nil))

	result := df.Slice(2, 2)

	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 cols, got %d", result.NumCols())
	}
}
