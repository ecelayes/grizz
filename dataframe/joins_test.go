package dataframe

import (
	"testing"

	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestDataFrameJoin(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2", "3"}, nil))
	df1.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob", "Charlie"}, nil))

	df2 := New()
	df2.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2", "4"}, nil))
	df2.AddSeries(series.NewStringSeries("dept", memory.DefaultAllocator, []string{"Sales", "Engineering", "HR"}, nil))

	joined, err := df1.Join(df2, "id", Inner)
	if err != nil {
		t.Errorf("Join failed: %v", err)
	}
	if joined.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", joined.NumRows())
	}
}

func TestDataFrameJoinInt(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewInt64Series("id", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	df1.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob", "Charlie"}, nil))

	df2 := New()
	df2.AddSeries(series.NewInt64Series("id", memory.DefaultAllocator, []int64{1, 2, 4}, nil))
	df2.AddSeries(series.NewStringSeries("dept", memory.DefaultAllocator, []string{"Sales", "Engineering", "HR"}, nil))

	joined, err := df1.Join(df2, "id", Inner)
	if err != nil {
		t.Errorf("Join with Int64 key failed: %v", err)
	}
	if joined.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", joined.NumRows())
	}
}

func TestDataFrameJoinLeft(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2", "3"}, nil))
	df1.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob", "Charlie"}, nil))

	df2 := New()
	df2.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2", "4"}, nil))
	df2.AddSeries(series.NewStringSeries("dept", memory.DefaultAllocator, []string{"Sales", "Engineering", "HR"}, nil))

	joined, err := df1.Join(df2, "id", Left)
	if err != nil {
		t.Errorf("Left Join failed: %v", err)
	}
	if joined.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", joined.NumRows())
	}
}

func TestDataFrameJoinOuter(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2"}, nil))

	df2 := New()
	df2.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"2", "3"}, nil))

	joined, err := df1.Join(df2, "id", Outer)
	if err != nil {
		t.Errorf("Outer Join failed: %v", err)
	}
	if joined.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", joined.NumRows())
	}
}

func TestDataFrameJoinRight(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2"}, nil))
	df1.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob"}, nil))

	df2 := New()
	df2.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2", "3"}, nil))
	df2.AddSeries(series.NewStringSeries("dept", memory.DefaultAllocator, []string{"Sales", "Engineering", "HR"}, nil))

	joined, err := df1.Join(df2, "id", Right)
	if err != nil {
		t.Errorf("Right Join failed: %v", err)
	}
	if joined.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", joined.NumRows())
	}
}

func TestDataFrameJoinCross(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2"}, nil))

	df2 := New()
	df2.AddSeries(series.NewStringSeries("dept", memory.DefaultAllocator, []string{"A", "B", "C"}, nil))

	joined, err := df1.Join(df2, "", Cross)
	if err != nil {
		t.Errorf("Cross Join failed: %v", err)
	}
	if joined.NumRows() != 6 {
		t.Errorf("Expected 6 rows, got %d", joined.NumRows())
	}
}

func TestDataFrameJoinFloat(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewFloat64Series("id", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0}, nil))
	df1.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob", "Charlie"}, nil))

	df2 := New()
	df2.AddSeries(series.NewFloat64Series("id", memory.DefaultAllocator, []float64{1.0, 2.0, 4.0}, nil))
	df2.AddSeries(series.NewStringSeries("dept", memory.DefaultAllocator, []string{"Sales", "Engineering", "HR"}, nil))

	joined, err := df1.Join(df2, "id", Inner)
	if err != nil {
		t.Errorf("Join with Float64 key failed: %v", err)
	}
	if joined.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", joined.NumRows())
	}
}

func TestJoinInnerWithFloat(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewFloat64Series("id", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0}, nil))
	df1.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob", "Charlie"}, nil))

	df2 := New()
	df2.AddSeries(series.NewFloat64Series("id", memory.DefaultAllocator, []float64{1.0, 2.0, 4.0}, nil))
	df2.AddSeries(series.NewFloat64Series("score", memory.DefaultAllocator, []float64{85.5, 90.0, 78.0}, nil))

	joined, err := df1.Join(df2, "id", Inner)
	if err != nil {
		t.Fatalf("Join failed: %v", err)
	}
	if joined.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", joined.NumRows())
	}
}

func TestJoinInnerWithBool(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"a", "b", "c"}, nil))
	df1.AddSeries(series.NewBooleanSeries("active", memory.DefaultAllocator, []bool{true, false, true}, nil))

	df2 := New()
	df2.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"a", "b", "d"}, nil))
	df2.AddSeries(series.NewInt64Series("count", memory.DefaultAllocator, []int64{10, 20, 30}, nil))

	joined, err := df1.Join(df2, "id", Inner)
	if err != nil {
		t.Fatalf("Join failed: %v", err)
	}
	if joined.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", joined.NumRows())
	}
}

func TestJoinInnerWithInt(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewInt64Series("id", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	df1.AddSeries(series.NewBooleanSeries("flag", memory.DefaultAllocator, []bool{true, false, true}, nil))

	df2 := New()
	df2.AddSeries(series.NewInt64Series("id", memory.DefaultAllocator, []int64{1, 2, 4}, nil))
	df2.AddSeries(series.NewStringSeries("dept", memory.DefaultAllocator, []string{"Sales", "Eng", "HR"}, nil))

	joined, err := df1.Join(df2, "id", Inner)
	if err != nil {
		t.Fatalf("Join failed: %v", err)
	}
	if joined.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", joined.NumRows())
	}
}

func TestJoinInnerMultipleMatches(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"a"}, nil))
	df1.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice"}, nil))

	df2 := New()
	df2.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"a", "a", "b"}, nil))
	df2.AddSeries(series.NewStringSeries("dept", memory.DefaultAllocator, []string{"Sales", "Eng", "HR"}, nil))

	joined, err := df1.Join(df2, "id", Inner)
	if err != nil {
		t.Fatalf("Join failed: %v", err)
	}
	if joined.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", joined.NumRows())
	}
}

func TestJoinLeftWithNulls(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"a", "b", "c"}, nil))
	df1.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob", "Charlie"}, nil))

	df2 := New()
	df2.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"a", "d"}, nil))
	df2.AddSeries(series.NewStringSeries("dept", memory.DefaultAllocator, []string{"Sales", "HR"}, nil))

	joined, err := df1.Join(df2, "id", Left)
	if err != nil {
		t.Fatalf("Left Join failed: %v", err)
	}
	if joined.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", joined.NumRows())
	}
}

func TestJoinRightWithNulls(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"a", "d"}, nil))
	df1.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob"}, nil))

	df2 := New()
	df2.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"a", "b", "c"}, nil))
	df2.AddSeries(series.NewStringSeries("dept", memory.DefaultAllocator, []string{"Sales", "Eng", "HR"}, nil))

	joined, err := df1.Join(df2, "id", Right)
	if err != nil {
		t.Fatalf("Right Join failed: %v", err)
	}
	if joined.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", joined.NumRows())
	}
}

func TestJoinOuterWithNulls(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"a", "b"}, nil))

	df2 := New()
	df2.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"b", "c", "d"}, nil))

	joined, err := df1.Join(df2, "id", Outer)
	if err != nil {
		t.Fatalf("Outer Join failed: %v", err)
	}
	if joined.NumRows() != 4 {
		t.Errorf("Expected 4 rows, got %d", joined.NumRows())
	}
}

func TestJoinLeftWithFloatColumn(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2", "3"}, nil))
	df1.AddSeries(series.NewFloat64Series("val", memory.DefaultAllocator, []float64{1.5, 2.5, 3.5}, nil))

	df2 := New()
	df2.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "4"}, nil))
	df2.AddSeries(series.NewFloat64Series("score", memory.DefaultAllocator, []float64{85.5, 90.0}, nil))

	joined, err := df1.Join(df2, "id", Left)
	if err != nil {
		t.Fatalf("Left Join failed: %v", err)
	}
	if joined.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", joined.NumRows())
	}
}

func TestJoinLeftWithBoolColumn(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"a", "b", "c"}, nil))
	df1.AddSeries(series.NewBooleanSeries("flag", memory.DefaultAllocator, []bool{true, false, true}, nil))

	df2 := New()
	df2.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"a", "d"}, nil))
	df2.AddSeries(series.NewBooleanSeries("active", memory.DefaultAllocator, []bool{true, false}, nil))

	joined, err := df1.Join(df2, "id", Left)
	if err != nil {
		t.Fatalf("Left Join failed: %v", err)
	}
	if joined.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", joined.NumRows())
	}
}

func TestJoinRightWithFloatColumn(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "4"}, nil))
	df1.AddSeries(series.NewFloat64Series("val", memory.DefaultAllocator, []float64{1.5, 2.5}, nil))

	df2 := New()
	df2.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2", "3"}, nil))
	df2.AddSeries(series.NewFloat64Series("score", memory.DefaultAllocator, []float64{85.5, 90.0, 78.0}, nil))

	joined, err := df1.Join(df2, "id", Right)
	if err != nil {
		t.Fatalf("Right Join failed: %v", err)
	}
	if joined.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", joined.NumRows())
	}
}

func TestJoinRightWithIntColumn(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "4"}, nil))
	df1.AddSeries(series.NewInt64Series("count", memory.DefaultAllocator, []int64{10, 20}, nil))

	df2 := New()
	df2.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2", "3"}, nil))
	df2.AddSeries(series.NewInt64Series("score", memory.DefaultAllocator, []int64{85, 90, 78}, nil))

	joined, err := df1.Join(df2, "id", Right)
	if err != nil {
		t.Fatalf("Right Join failed: %v", err)
	}
	if joined.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", joined.NumRows())
	}
}

func TestJoinOuterWithFloatColumn(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"a", "b"}, nil))
	df1.AddSeries(series.NewFloat64Series("val", memory.DefaultAllocator, []float64{1.5, 2.5}, nil))

	df2 := New()
	df2.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"b", "c", "d"}, nil))
	df2.AddSeries(series.NewBooleanSeries("flag", memory.DefaultAllocator, []bool{true, false, true}, nil))

	joined, err := df1.Join(df2, "id", Outer)
	if err != nil {
		t.Fatalf("Outer Join failed: %v", err)
	}
	if joined.NumRows() != 4 {
		t.Errorf("Expected 4 rows, got %d", joined.NumRows())
	}
}

func TestJoinWithMixedTypes(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2"}, nil))
	df1.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob"}, nil))

	df2 := New()
	df2.AddSeries(series.NewInt64Series("id", memory.DefaultAllocator, []int64{1, 2}, nil))
	df2.AddSeries(series.NewStringSeries("dept", memory.DefaultAllocator, []string{"Sales", "Engineering"}, nil))

	_, err := df1.Join(df2, "id", Inner)
	if err == nil {
		t.Error("Expected error for mismatched join key types")
	}
}

func TestJoinCross(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewStringSeries("a", memory.DefaultAllocator, []string{"1", "2"}, nil))

	df2 := New()
	df2.AddSeries(series.NewStringSeries("b", memory.DefaultAllocator, []string{"x", "y", "z"}, nil))

	joined, err := df1.Join(df2, "a", Cross)
	if err != nil {
		t.Fatalf("Cross Join failed: %v", err)
	}
	if joined.NumRows() != 6 {
		t.Errorf("Expected 6 rows, got %d", joined.NumRows())
	}
}

func TestJoinOuterWithStringKey(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"a", "b", "c"}, nil))
	df1.AddSeries(series.NewInt64Series("val1", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	df2 := New()
	df2.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"b", "c", "d"}, nil))
	df2.AddSeries(series.NewInt64Series("val2", memory.DefaultAllocator, []int64{20, 30, 40}, nil))

	joined, err := df1.Join(df2, "id", Outer)
	if err != nil {
		t.Fatalf("Outer Join with String failed: %v", err)
	}
	if joined.NumRows() != 4 {
		t.Errorf("Expected 4 rows, got %d", joined.NumRows())
	}
}

func TestJoinRightWithStringKey(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"a", "b"}, nil))
	df1.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob"}, nil))

	df2 := New()
	df2.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"a", "b", "c"}, nil))
	df2.AddSeries(series.NewStringSeries("dept", memory.DefaultAllocator, []string{"Sales", "Engineering", "HR"}, nil))

	joined, err := df1.Join(df2, "id", Right)
	if err != nil {
		t.Fatalf("Right Join with String failed: %v", err)
	}
	if joined.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", joined.NumRows())
	}
}

func TestJoinInnerWithNullsInKey(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"a", "b", "c"}, []bool{false, true, false}))
	df1.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob", "Charlie"}, nil))

	df2 := New()
	df2.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"a", "b", "d"}, nil))
	df2.AddSeries(series.NewStringSeries("dept", memory.DefaultAllocator, []string{"Sales", "Engineering", "HR"}, nil))

	joined, err := df1.Join(df2, "id", Inner)
	if err != nil {
		t.Fatalf("Inner Join with nulls failed: %v", err)
	}
	if joined.NumRows() != 1 {
		t.Errorf("Expected 1 row, got %d", joined.NumRows())
	}
}
