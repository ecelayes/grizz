package dataframe

import (
	"testing"

	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestDataFrameRename(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"a", "b", "c"}, nil))

	renamed := df.Rename(map[string]string{"age": "Age", "name": "Name"})
	cols := renamed.Columns()
	if cols[0] != "Age" || cols[1] != "Name" {
		t.Errorf("Expected [Age, Name], got %v", cols)
	}
}

func TestDataFrameUniqueValuesInt(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 1, 3, 2}, nil))

	unique := df.UniqueValues("a")
	if len(unique) != 3 {
		t.Errorf("Expected 3 unique values, got %d", len(unique))
	}
}

func TestDataFrameUniqueValuesFloat(t *testing.T) {
	df := New()
	df.AddSeries(series.NewFloat64Series("a", memory.DefaultAllocator, []float64{1.0, 2.0, 1.0, 3.0}, nil))

	unique := df.UniqueValues("a")
	if len(unique) != 3 {
		t.Errorf("Expected 3 unique values, got %d", len(unique))
	}
}

func TestRenameFloat(t *testing.T) {
	df := New()
	df.AddSeries(series.NewFloat64Series("score", memory.DefaultAllocator, []float64{1.5, 2.5, 3.5}, nil))

	renamed := df.Rename(map[string]string{"score": "Score"})
	cols := renamed.Columns()
	if cols[0] != "Score" {
		t.Errorf("Expected Score, got %s", cols[0])
	}
}

func TestRenameBool(t *testing.T) {
	df := New()
	df.AddSeries(series.NewBooleanSeries("active", memory.DefaultAllocator, []bool{true, false, true}, nil))

	renamed := df.Rename(map[string]string{"active": "Active"})
	cols := renamed.Columns()
	if cols[0] != "Active" {
		t.Errorf("Expected Active, got %s", cols[0])
	}
}

func TestRenamePartial(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	df.AddSeries(series.NewStringSeries("b", memory.DefaultAllocator, []string{"x", "y", "z"}, nil))

	renamed := df.Rename(map[string]string{"a": "A"})
	cols := renamed.Columns()
	if cols[0] != "A" || cols[1] != "b" {
		t.Errorf("Expected [A, b], got %v", cols)
	}
}

func TestUniqueValuesBoolean(t *testing.T) {
	df := New()
	df.AddSeries(series.NewBooleanSeries("flag", memory.DefaultAllocator, []bool{true, false, true, false}, nil))

	unique := df.UniqueValues("flag")
	if len(unique) != 2 {
		t.Errorf("Expected 2 unique values, got %d", len(unique))
	}
}

func TestUniqueValuesWithNulls(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 1}, []bool{true, false, true}))

	unique := df.UniqueValues("a")
	if len(unique) != 1 {
		t.Errorf("Expected 1 unique value (skipping nulls), got %d", len(unique))
	}
}

func TestDataFrameRenameMultiple(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	df.AddSeries(series.NewStringSeries("b", memory.DefaultAllocator, []string{"x", "y", "z"}, nil))

	renamed := df.Rename(map[string]string{"a": "A", "b": "B"})
	cols := renamed.Columns()
	if cols[0] != "A" || cols[1] != "B" {
		t.Errorf("Expected [A, B], got %v", cols)
	}
}
