package dataframe

import (
	"testing"

	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestDataFrameColumns(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"a", "b", "c"}, nil))
	cols := df.Columns()
	if len(cols) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(cols))
	}
	if cols[0] != "age" || cols[1] != "name" {
		t.Errorf("Expected [age, name], got %v", cols)
	}
}

func TestDataFrameDtypes(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	df.AddSeries(series.NewFloat64Series("score", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0}, nil))
	dtypes := df.Dtypes()
	if len(dtypes) != 2 {
		t.Errorf("Expected 2 dtypes, got %d", len(dtypes))
	}
}

func TestDataFrameShape(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3, 4, 5}, nil))
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"a", "b", "c", "d", "e"}, nil))
	rows, cols := df.Shape()
	if rows != 5 || cols != 2 {
		t.Errorf("Expected (5, 2), got (%d, %d)", rows, cols)
	}
}

func TestDataFrameIsEmpty(t *testing.T) {
	df := New()
	if !df.IsEmpty() {
		t.Error("Expected empty DataFrame")
	}
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1}, nil))
	if df.IsEmpty() {
		t.Error("Expected non-empty DataFrame")
	}
}

func TestDataFrameCol(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"a", "b", "c"}, nil))

	col, err := df.Col(0)
	if err != nil {
		t.Fatalf("Col failed: %v", err)
	}
	if col.Name() != "age" {
		t.Errorf("Expected age, got %s", col.Name())
	}

	_, err = df.Col(10)
	if err == nil {
		t.Error("Expected error for invalid index")
	}
}

func TestDataFrameColByName(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	col, err := df.ColByName("age")
	if err != nil {
		t.Fatalf("ColByName failed: %v", err)
	}
	if col.Name() != "age" {
		t.Errorf("Expected age, got %s", col.Name())
	}

	_, err = df.ColByName("nonexistent")
	if err == nil {
		t.Error("Expected error for nonexistent column")
	}
}

func TestDataFrameAddSeries(t *testing.T) {
	df := New()
	err := df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	if err != nil {
		t.Fatalf("AddSeries failed: %v", err)
	}

	err = df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"a", "b"}, nil))
	if err == nil {
		t.Error("Expected error for mismatched row count")
	}
}

func TestRelease(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	df.Release()
}
