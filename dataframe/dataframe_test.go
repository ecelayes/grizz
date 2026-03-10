package dataframe

import (
	"testing"

	"github.com/ecelayes/grizz/expr"
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
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{4, 5}, nil))

	if df.NumCols() != 1 {
		t.Errorf("Expected 1 column, got %d", df.NumCols())
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

func TestRelease(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"a", "b", "c"}, nil))

	df.Release()
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

func TestShow(t *testing.T) {
	df := New()
	df.Show()

	df2 := New()
	df2.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	df2.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"alice", "bob", "charlie"}, nil))
	df2.Show()

	df3 := New()
	df3.AddSeries(series.NewFloat64Series("score", memory.DefaultAllocator, []float64{1.5, 2.5}, nil))
	df3.Show()

	df4 := New()
	df4.AddSeries(series.NewBooleanSeries("flag", memory.DefaultAllocator, []bool{true, false}, nil))
	df4.Show()

	df5 := New()
	df5.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, []bool{false, true, false}))
	df5.Show()
}

func TestLazyFrame(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3, 4, 5}, nil))
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"a", "b", "c", "d", "e"}, nil))

	lf := df.Lazy()
	if lf == nil {
		t.Error("Lazy() returned nil")
	}

	explainStr := lf.Explain()
	if explainStr == "" {
		t.Error("Explain() returned empty string")
	}

	plan := lf.Plan()
	if plan == nil {
		t.Error("Plan() returned nil")
	}

	_, err := lf.Collect()
	if err == nil {
		t.Error("Collect() should return error")
	}
}

func TestLazyFrameFilter(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3, 4, 5}, nil))

	lf := df.Lazy()
	filtered := lf.Filter(expr.Col("age").Gt(expr.Lit(2)))

	if filtered == nil {
		t.Error("Filter() returned nil")
	}

	explainStr := filtered.Explain()
	if explainStr == "" {
		t.Error("Explain() returned empty string")
	}
}

func TestLazyFrameSelect(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"a", "b", "c"}, nil))

	lf := df.Lazy()
	selected := lf.Select(expr.Col("age"))

	if selected == nil {
		t.Error("Select() returned nil")
	}

	explainStr := selected.Explain()
	if explainStr == "" {
		t.Error("Explain() returned empty string")
	}
}

func TestLazyFrameGroupBy(t *testing.T) {
	df := New()
	df.AddSeries(series.NewStringSeries("dept", memory.DefaultAllocator, []string{"Sales", "Sales", "Engineering"}, nil))
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{25, 30, 35}, nil))

	lf := df.Lazy()
	groupBy := lf.GroupBy("dept")

	if groupBy == nil {
		t.Error("GroupBy() returned nil")
	}

	agg := groupBy.Agg(expr.Sum("age"))
	if agg == nil {
		t.Error("Agg() returned nil")
	}

	explainStr := agg.Explain()
	if explainStr == "" {
		t.Error("Explain() returned empty string")
	}
}

func TestLazyFrameOrderBy(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{3, 1, 2}, nil))

	lf := df.Lazy()
	ordered := lf.OrderBy("age", false)

	if ordered == nil {
		t.Error("OrderBy() returned nil")
	}

	explainStr := ordered.Explain()
	if explainStr == "" {
		t.Error("Explain() returned empty string")
	}

	orderedDesc := lf.OrderBy("age", true)
	explainDesc := orderedDesc.Explain()
	if explainDesc == "" {
		t.Error("Explain() for descending returned empty string")
	}
}

func TestLazyFrameLimit(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3, 4, 5}, nil))

	lf := df.Lazy()
	limited := lf.Limit(3)

	if limited == nil {
		t.Error("Limit() returned nil")
	}

	explainStr := limited.Explain()
	if explainStr == "" {
		t.Error("Explain() returned empty string")
	}
}

func TestLazyFrameHead(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3, 4, 5}, nil))

	lf := df.Lazy()
	head := lf.Head(2)

	if head == nil {
		t.Error("Head() returned nil")
	}

	explainStr := head.Explain()
	if explainStr == "" {
		t.Error("Explain() returned empty string")
	}
}

func TestLazyFrameTail(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3, 4, 5}, nil))

	lf := df.Lazy()
	tail := lf.Tail(2)

	if tail == nil {
		t.Error("Tail() returned nil")
	}

	explainStr := tail.Explain()
	if explainStr == "" {
		t.Error("Explain() returned empty string")
	}
}

func TestLazyFrameSample(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3, 4, 5}, nil))

	lf := df.Lazy()
	sampled := lf.Sample(2, false)

	if sampled == nil {
		t.Error("Sample() returned nil")
	}

	explainStr := sampled.Explain()
	if explainStr == "" {
		t.Error("Explain() returned empty string")
	}
}

func TestLazyFrameSampleFrac(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3, 4, 5}, nil))

	lf := df.Lazy()
	sampled := lf.SampleFrac(0.5, false)

	if sampled == nil {
		t.Error("SampleFrac() returned nil")
	}

	explainStr := sampled.Explain()
	if explainStr == "" {
		t.Error("Explain() returned empty string")
	}
}

func TestLazyFrameJoin(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2", "3"}, nil))

	df2 := New()
	df2.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2", "4"}, nil))

	lf1 := df1.Lazy()
	lf2 := df2.Lazy()

	joined := lf1.Join(lf2, "id", Inner)
	if joined == nil {
		t.Error("Join() returned nil")
	}

	explainStr := joined.Explain()
	if explainStr == "" {
		t.Error("Explain() returned empty string")
	}
}

func TestLazyFrameJoinOn(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2", "3"}, nil))

	df2 := New()
	df2.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2", "4"}, nil))

	lf1 := df1.Lazy()
	lf2 := df2.Lazy()

	joined := lf1.JoinOn(lf2, []string{"id"}, Left)
	if joined == nil {
		t.Error("JoinOn() returned nil")
	}

	explainStr := joined.Explain()
	if explainStr == "" {
		t.Error("Explain() returned empty string")
	}
}

func TestLazyFrameWithColumns(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	lf := df.Lazy()
	withCols := lf.WithColumns(expr.Col("age").Add(expr.Lit(1)))

	if withCols == nil {
		t.Error("WithColumns() returned nil")
	}

	explainStr := withCols.Explain()
	if explainStr == "" {
		t.Error("Explain() returned empty string")
	}
}

func TestLazyFrameDropNulls(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	lf := df.Lazy()
	dropped := lf.DropNulls()

	if dropped == nil {
		t.Error("DropNulls() returned nil")
	}

	explainStr := dropped.Explain()
	if explainStr == "" {
		t.Error("Explain() returned empty string")
	}
}

func TestLazyFrameDistinct(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 2, 3}, nil))

	lf := df.Lazy()
	distinct := lf.Distinct()

	if distinct == nil {
		t.Error("Distinct() returned nil")
	}

	explainStr := distinct.Explain()
	if explainStr == "" {
		t.Error("Explain() returned empty string")
	}
}

func TestLazyFrameUnique(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 2, 3}, nil))

	lf := df.Lazy()
	unique := lf.Unique()

	if unique == nil {
		t.Error("Unique() returned nil")
	}

	explainStr := unique.Explain()
	if explainStr == "" {
		t.Error("Explain() returned empty string")
	}
}

func TestLazyFrameWithWindow(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	lf := df.Lazy()

	windowExpr := expr.RowNumber()
	withWindow := lf.WithWindow(windowExpr, nil, nil)

	if withWindow == nil {
		t.Error("WithWindow() returned nil")
	}

	explainStr := withWindow.Explain()
	if explainStr == "" {
		t.Error("Explain() returned empty string")
	}
}

func TestScanPlanExplain(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	plan := ScanPlan{DataFrame: df}
	explainStr := plan.Explain(0)

	if explainStr == "" {
		t.Error("Explain() returned empty string")
	}
}

func TestFilterPlanExplain(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	lf := df.Lazy()
	filterPlan := FilterPlan{
		Input:     lf.Plan(),
		Condition: expr.Col("age").Gt(expr.Lit(2)),
	}
	explainStr := filterPlan.Explain(0)

	if explainStr == "" {
		t.Error("Explain() returned empty string")
	}
}

func TestSelectPlanExplain(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	lf := df.Lazy()
	selectPlan := SelectPlan{
		Input:   lf.Plan(),
		Columns: []expr.Expr{expr.Col("age")},
	}
	explainStr := selectPlan.Explain(0)

	if explainStr == "" {
		t.Error("Explain() returned empty string")
	}
}

func TestGroupByPlanExplain(t *testing.T) {
	df := New()
	df.AddSeries(series.NewStringSeries("dept", memory.DefaultAllocator, []string{"Sales", "Sales"}, nil))

	lf := df.Lazy()
	groupByPlan := GroupByPlan{
		Input: lf.Plan(),
		Keys:  []string{"dept"},
		Aggs:  []expr.Aggregation{expr.Sum("dept")},
	}
	explainStr := groupByPlan.Explain(0)

	if explainStr == "" {
		t.Error("Explain() returned empty string")
	}
}

func TestOrderByPlanExplain(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	lf := df.Lazy()
	orderByPlan := OrderByPlan{
		Input:      lf.Plan(),
		Column:     "age",
		Descending: false,
	}
	explainStr := orderByPlan.Explain(0)

	if explainStr == "" {
		t.Error("Explain() returned empty string")
	}

	orderByPlanDesc := OrderByPlan{
		Input:      lf.Plan(),
		Column:     "age",
		Descending: true,
	}
	explainStrDesc := orderByPlanDesc.Explain(0)
	if explainStrDesc == "" {
		t.Error("Explain() for descending returned empty string")
	}
}

func TestLimitPlanExplain(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	lf := df.Lazy()
	limitPlan := LimitPlan{
		Input: lf.Plan(),
		Limit: 10,
	}
	explainStr := limitPlan.Explain(0)

	if explainStr == "" {
		t.Error("Explain() returned empty string")
	}
}

func TestTailPlanExplain(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	lf := df.Lazy()
	tailPlan := TailPlan{
		Input: lf.Plan(),
		N:     2,
	}
	explainStr := tailPlan.Explain(0)

	if explainStr == "" {
		t.Error("Explain() returned empty string")
	}
}

func TestSamplePlanExplain(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	lf := df.Lazy()
	samplePlan := SamplePlan{
		Input:   lf.Plan(),
		N:       2,
		Replace: true,
	}
	explainStr := samplePlan.Explain(0)

	if explainStr == "" {
		t.Error("Explain() returned empty string")
	}

	samplePlanFrac := SamplePlan{
		Input:   lf.Plan(),
		Frac:    0.5,
		Replace: false,
	}
	explainStrFrac := samplePlanFrac.Explain(0)
	if explainStrFrac == "" {
		t.Error("Explain() with frac returned empty string")
	}
}

func TestJoinPlanExplain(t *testing.T) {
	df1 := New()
	df1.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2"}, nil))

	df2 := New()
	df2.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "3"}, nil))

	lf1 := df1.Lazy()
	lf2 := df2.Lazy()

	joinPlan := JoinPlan{
		Left:  lf1.Plan(),
		Right: lf2.Plan(),
		On:    "id",
		How:   Inner,
	}
	explainStr := joinPlan.Explain(0)

	if explainStr == "" {
		t.Error("Explain() returned empty string")
	}

	joinPlanOnCols := JoinPlan{
		Left:   lf1.Plan(),
		Right:  lf2.Plan(),
		OnCols: []string{"id"},
		How:    Left,
	}
	explainStrOnCols := joinPlanOnCols.Explain(0)
	if explainStrOnCols == "" {
		t.Error("Explain() with OnCols returned empty string")
	}
}

func TestWithColumnsPlanExplain(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	lf := df.Lazy()
	withColsPlan := WithColumnsPlan{
		Input:   lf.Plan(),
		Columns: []expr.Expr{expr.Col("age").Add(expr.Lit(1))},
	}
	explainStr := withColsPlan.Explain(0)

	if explainStr == "" {
		t.Error("Explain() returned empty string")
	}
}

func TestDropNullsPlanExplain(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	lf := df.Lazy()
	dropNullsPlan := DropNullsPlan{
		Input: lf.Plan(),
	}
	explainStr := dropNullsPlan.Explain(0)

	if explainStr == "" {
		t.Error("Explain() returned empty string")
	}
}

func TestDistinctPlanExplain(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	lf := df.Lazy()
	distinctPlan := DistinctPlan{
		Input: lf.Plan(),
	}
	explainStr := distinctPlan.Explain(0)

	if explainStr == "" {
		t.Error("Explain() returned empty string")
	}
}

func TestWindowPlanExplain(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	lf := df.Lazy()
	windowExpr := expr.RowNumber()
	windowPlan := WindowPlan{
		Input:   lf.Plan(),
		Func:    windowExpr,
		PartBy:  []string{"dept"},
		OrderBy: []string{"age"},
	}
	explainStr := windowPlan.Explain(0)

	if explainStr == "" {
		t.Error("Explain() returned empty string")
	}
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
