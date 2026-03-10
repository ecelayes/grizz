package engine

import (
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestApplyJoinInner(t *testing.T) {
	left := dataframe.New()
	left.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2", "3"}, nil))
	left.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob", "Charlie"}, nil))

	right := dataframe.New()
	right.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2", "4"}, nil))
	right.AddSeries(series.NewStringSeries("dept", memory.DefaultAllocator, []string{"Sales", "Engineering", "HR"}, nil))

	result, err := applyJoin(left, right, "id", dataframe.Inner)
	if err != nil {
		t.Fatalf("applyJoin failed: %v", err)
	}
	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestApplyJoinLeft(t *testing.T) {
	left := dataframe.New()
	left.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2", "3"}, nil))
	left.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob", "Charlie"}, nil))

	right := dataframe.New()
	right.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2"}, nil))
	right.AddSeries(series.NewStringSeries("dept", memory.DefaultAllocator, []string{"Sales", "Engineering"}, nil))

	result, err := applyJoin(left, right, "id", dataframe.Left)
	if err != nil {
		t.Fatalf("applyJoin failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplyJoinOuter(t *testing.T) {
	left := dataframe.New()
	left.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2"}, nil))

	right := dataframe.New()
	right.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"2", "3"}, nil))

	result, err := applyJoin(left, right, "id", dataframe.Outer)
	if err != nil {
		t.Fatalf("applyJoin failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplyJoinCross(t *testing.T) {
	left := dataframe.New()
	left.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2"}, nil))

	right := dataframe.New()
	right.AddSeries(series.NewStringSeries("dept", memory.DefaultAllocator, []string{"A", "B", "C"}, nil))

	result, err := applyJoin(left, right, "", dataframe.Cross)
	if err != nil {
		t.Fatalf("applyJoin failed: %v", err)
	}
	if result.NumRows() != 6 {
		t.Errorf("Expected 6 rows, got %d", result.NumRows())
	}
}

func TestApplyJoinRight(t *testing.T) {
	left := dataframe.New()
	left.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2"}, nil))
	left.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob"}, nil))

	right := dataframe.New()
	right.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2", "3"}, nil))
	right.AddSeries(series.NewStringSeries("dept", memory.DefaultAllocator, []string{"Sales", "Engineering", "HR"}, nil))

	result, err := applyJoin(left, right, "id", dataframe.Right)
	if err != nil {
		t.Fatalf("applyJoin failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestCopySeriesByIndicesFloat(t *testing.T) {
	col := series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.5, 2.5, 3.5, 4.5}, nil)
	indices := []int{0, 2}

	result := copySeriesByIndices(col, indices, memory.DefaultAllocator)
	if result.Len() != 2 {
		t.Errorf("Expected 2 elements, got %d", result.Len())
	}
	floatCol := result.(*series.Float64Series)
	if floatCol.Value(0) != 1.5 || floatCol.Value(1) != 3.5 {
		t.Errorf("Expected [1.5, 3.5], got [%f, %f]", floatCol.Value(0), floatCol.Value(1))
	}
}

func TestCopySeriesByIndicesBool(t *testing.T) {
	col := series.NewBooleanSeries("flag", memory.DefaultAllocator, []bool{true, false, true, false}, nil)
	indices := []int{0, 2}

	result := copySeriesByIndices(col, indices, memory.DefaultAllocator)
	if result.Len() != 2 {
		t.Errorf("Expected 2 elements, got %d", result.Len())
	}
	boolCol := result.(*series.BooleanSeries)
	if boolCol.Value(0) != true || boolCol.Value(1) != true {
		t.Errorf("Expected [true, true], got [%v, %v]", boolCol.Value(0), boolCol.Value(1))
	}
}

func TestCopySeriesByIndicesWithNulls(t *testing.T) {
	col := series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.5, 2.5, 3.5, 4.5}, nil)
	indices := []int{0, 2, -1, 1}
	valid := []bool{true, true, false, true}

	result := copySeriesByIndicesWithNulls(col, indices, valid, memory.DefaultAllocator)
	if result.Len() != 4 {
		t.Errorf("Expected 4 elements, got %d", result.Len())
	}
	floatCol := result.(*series.Float64Series)
	if floatCol.Value(0) != 1.5 || floatCol.Value(1) != 3.5 || floatCol.Value(2) != 0 || floatCol.Value(3) != 2.5 {
		t.Errorf("Expected [1.5, 3.5, 0, 2.5], got [%f, %f, %f, %f]", floatCol.Value(0), floatCol.Value(1), floatCol.Value(2), floatCol.Value(3))
	}
}

func TestCopySeriesByIndicesWithNullsString(t *testing.T) {
	col := series.NewStringSeries("name", memory.DefaultAllocator, []string{"a", "b", "c", "d"}, nil)
	indices := []int{0, -1, 2}
	valid := []bool{true, false, true}

	result := copySeriesByIndicesWithNulls(col, indices, valid, memory.DefaultAllocator)
	if result.Len() != 3 {
		t.Errorf("Expected 3 elements, got %d", result.Len())
	}
	strCol := result.(*series.StringSeries)
	if strCol.Value(0) != "a" || strCol.Value(1) != "" || strCol.Value(2) != "c" {
		t.Errorf("Expected [a, , c], got [%s, %s, %s]", strCol.Value(0), strCol.Value(1), strCol.Value(2))
	}
}

func TestCopySeriesByIndicesWithNullsInt(t *testing.T) {
	col := series.NewInt64Series("value", memory.DefaultAllocator, []int64{1, 2, 3, 4}, nil)
	indices := []int{0, 2, -1}
	valid := []bool{true, true, false}

	result := copySeriesByIndicesWithNulls(col, indices, valid, memory.DefaultAllocator)
	if result.Len() != 3 {
		t.Errorf("Expected 3 elements, got %d", result.Len())
	}
	intCol := result.(*series.Int64Series)
	if intCol.Value(0) != 1 || intCol.Value(1) != 3 || intCol.Value(2) != 0 {
		t.Errorf("Expected [1, 3, 0], got [%d, %d, %d]", intCol.Value(0), intCol.Value(1), intCol.Value(2))
	}
}

func TestCopySeriesByIndicesWithNullsBool(t *testing.T) {
	col := series.NewBooleanSeries("flag", memory.DefaultAllocator, []bool{true, false, true, false}, nil)
	indices := []int{0, -1, 2}
	valid := []bool{true, false, true}

	result := copySeriesByIndicesWithNulls(col, indices, valid, memory.DefaultAllocator)
	if result.Len() != 3 {
		t.Errorf("Expected 3 elements, got %d", result.Len())
	}
	boolCol := result.(*series.BooleanSeries)
	if boolCol.Value(0) != true || boolCol.Value(1) != false || boolCol.Value(2) != true {
		t.Errorf("Expected [true, false, true], got [%v, %v, %v]", boolCol.Value(0), boolCol.Value(1), boolCol.Value(2))
	}
}

func TestApplyJoinInnerWithFloat(t *testing.T) {
	left := dataframe.New()
	left.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2", "3"}, nil))
	left.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{10.5, 20.5, 30.5}, nil))
	left.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob", "Charlie"}, nil))

	right := dataframe.New()
	right.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2", "4"}, nil))
	right.AddSeries(series.NewFloat64Series("score", memory.DefaultAllocator, []float64{100.0, 200.0, 300.0}, nil))
	right.AddSeries(series.NewStringSeries("dept", memory.DefaultAllocator, []string{"Sales", "Engineering", "HR"}, nil))

	result, err := applyJoin(left, right, "id", dataframe.Inner)
	if err != nil {
		t.Fatalf("applyJoin failed: %v", err)
	}
	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestApplyJoinInnerWithBool(t *testing.T) {
	left := dataframe.New()
	left.AddSeries(series.NewStringSeries("active", memory.DefaultAllocator, []string{"yes", "no", "yes"}, nil))
	left.AddSeries(series.NewBooleanSeries("flag", memory.DefaultAllocator, []bool{true, false, true}, nil))
	left.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob", "Charlie"}, nil))

	right := dataframe.New()
	right.AddSeries(series.NewStringSeries("active", memory.DefaultAllocator, []string{"yes", "no"}, nil))
	right.AddSeries(series.NewBooleanSeries("status", memory.DefaultAllocator, []bool{true, false}, nil))
	right.AddSeries(series.NewStringSeries("dept", memory.DefaultAllocator, []string{"Sales", "Engineering"}, nil))

	result, err := applyJoin(left, right, "active", dataframe.Inner)
	if err != nil {
		t.Fatalf("applyJoin failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplyJoinLeftWithNulls(t *testing.T) {
	left := dataframe.New()
	left.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2", "3"}, nil))
	left.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob", "Charlie"}, nil))

	right := dataframe.New()
	right.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "4", "5"}, []bool{true, true, false}))
	right.AddSeries(series.NewStringSeries("dept", memory.DefaultAllocator, []string{"Sales", "HR", ""}, []bool{true, true, false}))

	result, err := applyJoin(left, right, "id", dataframe.Left)
	if err != nil {
		t.Fatalf("applyJoin failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplyJoinRightWithNulls(t *testing.T) {
	left := dataframe.New()
	left.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "3"}, nil))
	left.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Charlie"}, nil))

	right := dataframe.New()
	right.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2", "3"}, nil))
	right.AddSeries(series.NewStringSeries("dept", memory.DefaultAllocator, []string{"Sales", "Engineering", "HR"}, nil))

	result, err := applyJoin(left, right, "id", dataframe.Right)
	if err != nil {
		t.Fatalf("applyJoin failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplyJoinOuterWithNulls(t *testing.T) {
	left := dataframe.New()
	left.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "3"}, nil))
	left.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Charlie"}, nil))

	right := dataframe.New()
	right.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2", "3"}, nil))
	right.AddSeries(series.NewStringSeries("dept", memory.DefaultAllocator, []string{"Sales", "Engineering", "HR"}, nil))

	result, err := applyJoin(left, right, "id", dataframe.Outer)
	if err != nil {
		t.Fatalf("applyJoin failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplyJoinLeftEmptyResult(t *testing.T) {
	left := dataframe.New()
	left.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2"}, nil))
	left.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob"}, nil))

	right := dataframe.New()
	right.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"3", "4"}, nil))
	right.AddSeries(series.NewStringSeries("dept", memory.DefaultAllocator, []string{"HR", "Finance"}, nil))

	result, err := applyJoin(left, right, "id", dataframe.Left)
	if err != nil {
		t.Fatalf("applyJoin failed: %v", err)
	}
	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}
