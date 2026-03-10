package engine

import (
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestApplyOrderByAscending(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{30, 10, 20}, nil))
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"c", "a", "b"}, nil))

	result, err := applyOrderBy(df, "age", false)
	if err != nil {
		t.Fatalf("applyOrderBy failed: %v", err)
	}
	ageCol, _ := result.ColByName("age")
	if ageCol.(*series.Int64Series).Value(0) != 10 {
		t.Errorf("Expected first value 10, got %d", ageCol.(*series.Int64Series).Value(0))
	}
}

func TestApplyOrderByDescending(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{30, 10, 20}, nil))

	result, err := applyOrderBy(df, "age", true)
	if err != nil {
		t.Fatalf("applyOrderBy failed: %v", err)
	}
	ageCol, _ := result.ColByName("age")
	if ageCol.(*series.Int64Series).Value(0) != 30 {
		t.Errorf("Expected first value 30, got %d", ageCol.(*series.Int64Series).Value(0))
	}
}

func TestApplyOrderByString(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Charlie", "Alice", "Bob"}, nil))

	result, err := applyOrderBy(df, "name", false)
	if err != nil {
		t.Fatalf("applyOrderBy failed: %v", err)
	}
	nameCol, _ := result.ColByName("name")
	if nameCol.(*series.StringSeries).Value(0) != "Alice" {
		t.Errorf("Expected first value Alice, got %s", nameCol.(*series.StringSeries).Value(0))
	}
}

func TestApplyOrderByDescendingWithNulls(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{30, 0, 10, 0, 20}, []bool{true, false, true, false, true}))

	result, err := applyOrderBy(df, "age", true)
	if err != nil {
		t.Fatalf("applyOrderBy failed: %v", err)
	}
	ageCol, _ := result.ColByName("age")
	if ageCol.(*series.Int64Series).Value(0) != 30 {
		t.Errorf("Expected first value 30, got %d", ageCol.(*series.Int64Series).Value(0))
	}
}

func TestApplyOrderByAscendingWithNulls(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{30, 20, 10}, nil))

	result, err := applyOrderBy(df, "age", false)
	if err != nil {
		t.Fatalf("applyOrderBy failed: %v", err)
	}
	ageCol, _ := result.ColByName("age")
	if ageCol.(*series.Int64Series).Value(0) != 10 {
		t.Errorf("Expected first value 10, got %d", ageCol.(*series.Int64Series).Value(0))
	}
	if ageCol.(*series.Int64Series).Value(1) != 20 {
		t.Errorf("Expected second value 20, got %d", ageCol.(*series.Int64Series).Value(1))
	}
	if ageCol.(*series.Int64Series).Value(2) != 30 {
		t.Errorf("Expected third value 30, got %d", ageCol.(*series.Int64Series).Value(2))
	}
}

func TestApplyOrderByBooleanDescending(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewBooleanSeries("flag", memory.DefaultAllocator, []bool{true, false, true, false}, nil))

	result, err := applyOrderBy(df, "flag", true)
	if err != nil {
		t.Fatalf("applyOrderBy failed: %v", err)
	}
	flagCol, _ := result.ColByName("flag")
	if flagCol.(*series.BooleanSeries).Value(0) != true {
		t.Errorf("Expected first value true, got %v", flagCol.(*series.BooleanSeries).Value(0))
	}
}

func TestApplyOrderByFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{3.5, 1.5, 2.5}, nil))

	result, err := applyOrderBy(df, "value", false)
	if err != nil {
		t.Fatalf("applyOrderBy failed: %v", err)
	}
	valCol, _ := result.ColByName("value")
	if valCol.(*series.Float64Series).Value(0) != 1.5 {
		t.Errorf("Expected first value 1.5, got %f", valCol.(*series.Float64Series).Value(0))
	}
}

func TestApplyIndicesFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.5, 2.5, 3.5, 4.5}, nil))

	indices := []int{0, 2}
	result, err := applyIndices(df, indices)
	if err != nil {
		t.Fatalf("applyIndices failed: %v", err)
	}
	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
	valCol, _ := result.ColByName("value")
	if valCol.(*series.Float64Series).Value(0) != 1.5 || valCol.(*series.Float64Series).Value(1) != 3.5 {
		t.Errorf("Expected [1.5, 3.5], got [%f, %f]", valCol.(*series.Float64Series).Value(0), valCol.(*series.Float64Series).Value(1))
	}
}
