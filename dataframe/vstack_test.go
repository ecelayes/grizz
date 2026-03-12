package dataframe

import (
	"testing"

	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestDataFrameVStack(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	df2 := New()
	df2.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{4, 5}, nil))

	vstacked, err := df1.VStack(df2)
	if err != nil {
		t.Errorf("VStack failed: %v", err)
	}
	if vstacked.NumRows() != 5 {
		t.Errorf("Expected 5 rows, got %d", vstacked.NumRows())
	}
}

func TestDataFrameVStackFloat(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewFloat64Series("score", memory.DefaultAllocator, []float64{1.0, 2.0}, nil))

	df2 := New()
	df2.AddSeries(series.NewFloat64Series("score", memory.DefaultAllocator, []float64{3.0}, nil))

	vstacked, err := df1.VStack(df2)
	if err != nil {
		t.Fatalf("VStack failed: %v", err)
	}
	if vstacked.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", vstacked.NumRows())
	}
}

func TestDataFrameVStackString(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob"}, nil))

	df2 := New()
	df2.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Charlie"}, nil))

	vstacked, err := df1.VStack(df2)
	if err != nil {
		t.Fatalf("VStack failed: %v", err)
	}
	if vstacked.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", vstacked.NumRows())
	}
}

func TestVStackWithNulls(t *testing.T) {
	valid1 := []bool{true, false, true}
	df1 := New()
	df1.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 0, 3}, valid1))

	valid2 := []bool{true, true, false}
	df2 := New()
	df2.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{4, 5, 0}, valid2))

	vstacked, err := df1.VStack(df2)
	if err != nil {
		t.Fatalf("VStack with nulls failed: %v", err)
	}
	if vstacked.NumRows() != 6 {
		t.Errorf("Expected 6 rows, got %d", vstacked.NumRows())
	}

	col, _ := vstacked.Col(0)
	intCol := col.(*series.Int64Series)
	if !intCol.IsNull(1) {
		t.Error("Expected index 1 to be null")
	}
	if !intCol.IsNull(5) {
		t.Error("Expected index 5 to be null")
	}
}

func TestVStackWithNullsFloat(t *testing.T) {
	valid1 := []bool{true, true, false}
	df1 := New()
	df1.AddSeries(series.NewFloat64Series("score", memory.DefaultAllocator, []float64{1.5, 2.5, 0.0}, valid1))

	valid2 := []bool{false, true, true}
	df2 := New()
	df2.AddSeries(series.NewFloat64Series("score", memory.DefaultAllocator, []float64{0.0, 3.5, 4.5}, valid2))

	vstacked, err := df1.VStack(df2)
	if err != nil {
		t.Fatalf("VStack with nulls failed: %v", err)
	}
	if vstacked.NumRows() != 6 {
		t.Errorf("Expected 6 rows, got %d", vstacked.NumRows())
	}

	col, _ := vstacked.Col(0)
	floatCol := col.(*series.Float64Series)
	if !floatCol.IsNull(2) {
		t.Error("Expected index 2 to be null")
	}
	if !floatCol.IsNull(3) {
		t.Error("Expected index 3 to be null")
	}
}

func TestVStackWithNullsBool(t *testing.T) {
	valid1 := []bool{true, false, true}
	df1 := New()
	df1.AddSeries(series.NewBooleanSeries("active", memory.DefaultAllocator, []bool{true, false, true}, valid1))

	valid2 := []bool{true, true, false}
	df2 := New()
	df2.AddSeries(series.NewBooleanSeries("active", memory.DefaultAllocator, []bool{false, true, false}, valid2))

	vstacked, err := df1.VStack(df2)
	if err != nil {
		t.Fatalf("VStack with nulls failed: %v", err)
	}
	if vstacked.NumRows() != 6 {
		t.Errorf("Expected 6 rows, got %d", vstacked.NumRows())
	}
}

func TestVStackWithNullsString(t *testing.T) {
	valid1 := []bool{true, false, true}
	df1 := New()
	df1.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "", "Bob"}, valid1))

	valid2 := []bool{false, true, true}
	df2 := New()
	df2.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"", "Charlie", "David"}, valid2))

	vstacked, err := df1.VStack(df2)
	if err != nil {
		t.Fatalf("VStack with nulls failed: %v", err)
	}
	if vstacked.NumRows() != 6 {
		t.Errorf("Expected 6 rows, got %d", vstacked.NumRows())
	}
}

func TestVStackError(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	df2 := New()
	df2.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{4, 5}, nil))
	df2.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"a", "b"}, nil))

	_, err := df1.VStack(df2)
	if err == nil {
		t.Error("Expected error for mismatched columns")
	}
}
