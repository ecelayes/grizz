package dataframe

import (
	"testing"

	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestDataFramePivot(t *testing.T) {
	df := New()
	df.AddSeries(series.NewStringSeries("fruit", memory.DefaultAllocator, []string{"apple", "banana", "apple"}, nil))
	df.AddSeries(series.NewStringSeries("color", memory.DefaultAllocator, []string{"red", "yellow", "green"}, nil))
	df.AddSeries(series.NewInt64Series("count", memory.DefaultAllocator, []int64{10, 5, 8}, nil))

	pivoted, err := df.Pivot("fruit", "color", "count")
	if err != nil {
		t.Fatalf("Pivot failed: %v", err)
	}
	t.Logf("Pivot result: %d cols, %d rows", pivoted.NumCols(), pivoted.NumRows())
	for i := 0; i < pivoted.NumCols(); i++ {
		col, _ := pivoted.Col(i)
		t.Logf("Column %d: %s", i, col.Name())
	}
}

func TestDataFrameMelt(t *testing.T) {
	df := New()
	df.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"a", "b"}, nil))
	df.AddSeries(series.NewInt64Series("x", memory.DefaultAllocator, []int64{1, 2}, nil))
	df.AddSeries(series.NewInt64Series("y", memory.DefaultAllocator, []int64{3, 4}, nil))

	melted, err := df.Melt([]string{"id"}, []string{"x", "y"})
	if err != nil {
		t.Errorf("Melt failed: %v", err)
	}
	if melted.NumRows() != 4 {
		t.Errorf("Expected 4 rows, got %d", melted.NumRows())
	}
}

func TestPivotWithIntIndex(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("id", memory.DefaultAllocator, []int64{1, 2, 1}, nil))
	df.AddSeries(series.NewStringSeries("type", memory.DefaultAllocator, []string{"a", "b", "a"}, nil))
	df.AddSeries(series.NewInt64Series("val", memory.DefaultAllocator, []int64{10, 20, 15}, nil))

	pivoted, err := df.Pivot("id", "type", "val")
	if err != nil {
		t.Fatalf("Pivot with Int index failed: %v", err)
	}
	if pivoted.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", pivoted.NumRows())
	}
}

func TestPivotWithFloatIndex(t *testing.T) {
	df := New()
	df.AddSeries(series.NewFloat64Series("id", memory.DefaultAllocator, []float64{1.5, 2.5, 1.5}, nil))
	df.AddSeries(series.NewStringSeries("type", memory.DefaultAllocator, []string{"a", "b", "a"}, nil))
	df.AddSeries(series.NewFloat64Series("val", memory.DefaultAllocator, []float64{10.0, 20.0, 15.0}, nil))

	pivoted, err := df.Pivot("id", "type", "val")
	if err != nil {
		t.Fatalf("Pivot with Float index failed: %v", err)
	}
	if pivoted.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", pivoted.NumRows())
	}
}

func TestPivotWithIntValue(t *testing.T) {
	df := New()
	df.AddSeries(series.NewStringSeries("fruit", memory.DefaultAllocator, []string{"apple", "banana", "apple"}, nil))
	df.AddSeries(series.NewStringSeries("color", memory.DefaultAllocator, []string{"red", "yellow", "green"}, nil))
	df.AddSeries(series.NewInt64Series("count", memory.DefaultAllocator, []int64{10, 5, 8}, nil))

	pivoted, err := df.Pivot("fruit", "color", "count")
	if err != nil {
		t.Fatalf("Pivot with Int value failed: %v", err)
	}
	_ = pivoted
}

func TestPivotWithFloatValue(t *testing.T) {
	df := New()
	df.AddSeries(series.NewStringSeries("fruit", memory.DefaultAllocator, []string{"apple", "banana", "apple"}, nil))
	df.AddSeries(series.NewStringSeries("color", memory.DefaultAllocator, []string{"red", "yellow", "green"}, nil))
	df.AddSeries(series.NewFloat64Series("price", memory.DefaultAllocator, []float64{1.5, 2.5, 1.8}, nil))

	pivoted, err := df.Pivot("fruit", "color", "price")
	if err != nil {
		t.Fatalf("Pivot with Float value failed: %v", err)
	}
	_ = pivoted
}

func TestMeltWithBoolean(t *testing.T) {
	df := New()
	df.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"a", "b"}, nil))
	df.AddSeries(series.NewBooleanSeries("flag1", memory.DefaultAllocator, []bool{true, false}, nil))
	df.AddSeries(series.NewBooleanSeries("flag2", memory.DefaultAllocator, []bool{false, true}, nil))

	melted, err := df.Melt([]string{"id"}, []string{"flag1", "flag2"})
	if err != nil {
		t.Fatalf("Melt with Boolean failed: %v", err)
	}
	if melted.NumRows() != 4 {
		t.Errorf("Expected 4 rows, got %d", melted.NumRows())
	}
}

func TestMeltWithInt(t *testing.T) {
	df := New()
	df.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"a", "b"}, nil))
	df.AddSeries(series.NewInt64Series("x", memory.DefaultAllocator, []int64{1, 2}, nil))
	df.AddSeries(series.NewInt64Series("y", memory.DefaultAllocator, []int64{3, 4}, nil))

	melted, err := df.Melt([]string{"id"}, []string{"x", "y"})
	if err != nil {
		t.Fatalf("Melt with Int failed: %v", err)
	}
	if melted.NumRows() != 4 {
		t.Errorf("Expected 4 rows, got %d", melted.NumRows())
	}
}

func TestMeltWithFloat(t *testing.T) {
	df := New()
	df.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"a", "b"}, nil))
	df.AddSeries(series.NewFloat64Series("x", memory.DefaultAllocator, []float64{1.5, 2.5}, nil))
	df.AddSeries(series.NewFloat64Series("y", memory.DefaultAllocator, []float64{3.5, 4.5}, nil))

	melted, err := df.Melt([]string{"id"}, []string{"x", "y"})
	if err != nil {
		t.Fatalf("Melt with Float failed: %v", err)
	}
	if melted.NumRows() != 4 {
		t.Errorf("Expected 4 rows, got %d", melted.NumRows())
	}
}

func TestMeltWithNulls(t *testing.T) {
	df := New()
	df.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"a", "b"}, nil))
	df.AddSeries(series.NewInt64Series("x", memory.DefaultAllocator, []int64{1, 2}, []bool{true, false}))

	melted, err := df.Melt([]string{"id"}, []string{"x"})
	if err != nil {
		t.Fatalf("Melt with nulls failed: %v", err)
	}
	_ = melted
}

func TestPivotWithBooleanValue(t *testing.T) {
	df := New()
	df.AddSeries(series.NewStringSeries("fruit", memory.DefaultAllocator, []string{"apple", "banana", "apple"}, nil))
	df.AddSeries(series.NewStringSeries("color", memory.DefaultAllocator, []string{"red", "yellow", "green"}, nil))
	df.AddSeries(series.NewBooleanSeries("inStock", memory.DefaultAllocator, []bool{true, false, true}, nil))

	pivoted, err := df.Pivot("fruit", "color", "inStock")
	if err != nil {
		t.Fatalf("Pivot with Boolean value failed: %v", err)
	}
	_ = pivoted
}

func TestMeltWithNullIdVars(t *testing.T) {
	df := New()
	df.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"a", "b", "c"}, []bool{false, true, false}))
	df.AddSeries(series.NewInt64Series("x", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	melted, err := df.Melt([]string{"id"}, []string{"x"})
	if err != nil {
		t.Fatalf("Melt with null idVars failed: %v", err)
	}
	_ = melted
}
