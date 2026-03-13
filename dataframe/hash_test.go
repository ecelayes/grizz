package dataframe

import (
	"testing"

	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestIsUnique(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3, 2}, nil))

	result := df.IsUnique()

	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestIsUniqueAllUnique(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3, 4}, nil))

	result := df.IsUnique()

	if result.NumRows() != 4 {
		t.Errorf("Expected 4 rows, got %d", result.NumRows())
	}
}

func TestIsUniqueEmpty(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{}, nil))

	result := df.IsUnique()

	if result.NumRows() != 0 {
		t.Errorf("Expected 0 rows, got %d", result.NumRows())
	}
}

func TestIsDuplicated(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3, 2}, nil))

	result := df.IsDuplicated()

	if result.NumRows() != 1 {
		t.Errorf("Expected 1 row, got %d", result.NumRows())
	}
}

func TestIsDuplicatedNoDuplicates(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3, 4}, nil))

	result := df.IsDuplicated()

	if result.NumRows() != 0 {
		t.Errorf("Expected 0 rows, got %d", result.NumRows())
	}
}

func TestIsDuplicatedMultiple(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 2, 3, 3}, nil))

	result := df.IsDuplicated()

	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestIsUniqueWithString(t *testing.T) {
	df := New()
	df.AddSeries(series.NewStringSeries("a", memory.DefaultAllocator, []string{"x", "y", "x", "z"}, nil))

	result := df.IsUnique()

	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestIsDuplicatedWithBoolean(t *testing.T) {
	df := New()
	df.AddSeries(series.NewBooleanSeries("a", memory.DefaultAllocator, []bool{true, false, true}, nil))

	result := df.IsDuplicated()

	if result.NumRows() != 1 {
		t.Errorf("Expected 1 row, got %d", result.NumRows())
	}
}
