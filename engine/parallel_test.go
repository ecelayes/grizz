package engine

import (
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestExecuteParallelScanPlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	plan := dataframe.ScanPlan{DataFrame: df}
	result, err := ExecuteParallel(plan)
	if err != nil {
		t.Fatalf("ExecuteParallel failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestExecuteParallelFilterPlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	plan := dataframe.FilterPlan{
		Input:     dataframe.ScanPlan{DataFrame: df},
		Condition: expr.Col("a").Gt(expr.Lit(1)),
	}
	result, err := ExecuteParallel(plan)
	if err != nil {
		t.Fatalf("ExecuteParallel failed: %v", err)
	}
	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestExecuteParallelSelectPlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2}, nil))
	df.AddSeries(series.NewStringSeries("b", memory.DefaultAllocator, []string{"x", "y"}, nil))

	plan := dataframe.SelectPlan{
		Input:   dataframe.ScanPlan{DataFrame: df},
		Columns: []expr.Expr{expr.Col("a")},
	}
	result, err := ExecuteParallel(plan)
	if err != nil {
		t.Fatalf("ExecuteParallel failed: %v", err)
	}
	if result.NumCols() != 1 {
		t.Errorf("Expected 1 column, got %d", result.NumCols())
	}
}

func TestExecuteParallelWithColumnsPlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	plan := dataframe.WithColumnsPlan{
		Input:   dataframe.ScanPlan{DataFrame: df},
		Columns: []expr.Expr{expr.FillNull(expr.Col("a"), expr.Lit(0))},
	}
	result, err := ExecuteParallel(plan)
	if err != nil {
		t.Fatalf("ExecuteParallel failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplyMaskParallel(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3, 4, 5}, nil))

	mask := []bool{true, false, true, false, true}
	result, err := applyMaskParallel(df, mask, 2)
	if err != nil {
		t.Fatalf("applyMaskParallel failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestFilterColumnParallel(t *testing.T) {
	col := series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3, 4, 5}, nil)
	mask := []bool{true, false, true, false, true}

	result := filterColumnParallel(col, mask)
	resultInt, ok := result.(*series.Int64Series)
	if !ok {
		t.Fatalf("Expected Int64Series")
	}
	if resultInt.Len() != 3 {
		t.Errorf("Expected 3 values, got %d", resultInt.Len())
	}
}

func TestFilterColumnParallelString(t *testing.T) {
	col := series.NewStringSeries("name", memory.DefaultAllocator, []string{"a", "b", "c", "d", "e"}, nil)
	mask := []bool{true, false, true, false, true}

	result := filterColumnParallel(col, mask)
	resultStr, ok := result.(*series.StringSeries)
	if !ok {
		t.Fatalf("Expected StringSeries")
	}
	if resultStr.Len() != 3 {
		t.Errorf("Expected 3 values, got %d", resultStr.Len())
	}
}

func TestFilterColumnParallelFloat(t *testing.T) {
	col := series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0, 4.0, 5.0}, nil)
	mask := []bool{true, false, true, false, true}

	result := filterColumnParallel(col, mask)
	resultFloat, ok := result.(*series.Float64Series)
	if !ok {
		t.Fatalf("Expected Float64Series")
	}
	if resultFloat.Len() != 3 {
		t.Errorf("Expected 3 values, got %d", resultFloat.Len())
	}
}

func TestFilterColumnParallelBool(t *testing.T) {
	col := series.NewBooleanSeries("flag", memory.DefaultAllocator, []bool{true, false, true, false, true}, nil)
	mask := []bool{true, false, true, false, true}

	result := filterColumnParallel(col, mask)
	resultBool, ok := result.(*series.BooleanSeries)
	if !ok {
		t.Fatalf("Expected BooleanSeries")
	}
	if resultBool.Len() != 3 {
		t.Errorf("Expected 3 values, got %d", resultBool.Len())
	}
}

func TestFilterColumnParallelWithNulls(t *testing.T) {
	col := series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3, 4, 5}, []bool{true, false, true, true, false})
	mask := []bool{true, true, false, false, true}

	result := filterColumnParallel(col, mask)
	resultInt, ok := result.(*series.Int64Series)
	if !ok {
		t.Fatalf("Expected Int64Series")
	}
	if resultInt.Len() != 3 {
		t.Errorf("Expected 3 values, got %d", resultInt.Len())
	}
}

func TestExecuteParallelJoinPlan(t *testing.T) {
	df1 := dataframe.New()
	df1.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2"}, nil))
	df1.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob"}, nil))

	df2 := dataframe.New()
	df2.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "3"}, nil))
	df2.AddSeries(series.NewStringSeries("dept", memory.DefaultAllocator, []string{"Sales", "HR"}, nil))

	plan := dataframe.JoinPlan{
		Left:  dataframe.ScanPlan{DataFrame: df1},
		Right: dataframe.ScanPlan{DataFrame: df2},
		On:    "id",
		How:   dataframe.Inner,
	}
	result, err := ExecuteParallel(plan)
	if err != nil {
		t.Fatalf("ExecuteParallel failed: %v", err)
	}
	if result.NumRows() != 1 {
		t.Errorf("Expected 1 row, got %d", result.NumRows())
	}
}

func TestExecuteParallelDistinctPlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 10, 20, 30}, nil))

	plan := dataframe.DistinctPlan{
		Input: dataframe.ScanPlan{DataFrame: df},
	}
	result, err := ExecuteParallel(plan)
	if err != nil {
		t.Fatalf("ExecuteParallel failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestExecuteParallelDropNullsPlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 0, 30}, []bool{true, false, true}))

	plan := dataframe.DropNullsPlan{
		Input: dataframe.ScanPlan{DataFrame: df},
	}
	result, err := ExecuteParallel(plan)
	if err != nil {
		t.Fatalf("ExecuteParallel failed: %v", err)
	}
	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestExecuteParallelWindowPlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("id", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	plan := dataframe.WindowPlan{
		Input: dataframe.ScanPlan{DataFrame: df},
		Func:  expr.WindowExpr{Func: expr.FuncRowNumber},
	}
	result, err := ExecuteParallel(plan)
	if err != nil {
		t.Fatalf("ExecuteParallel failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestExecuteParallelTailPlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30, 40, 50}, nil))

	plan := dataframe.TailPlan{
		Input: dataframe.ScanPlan{DataFrame: df},
		N:     2,
	}
	result, err := ExecuteParallel(plan)
	if err != nil {
		t.Fatalf("ExecuteParallel failed: %v", err)
	}
	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestExecuteParallelSamplePlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30, 40, 50}, nil))

	plan := dataframe.SamplePlan{
		Input:   dataframe.ScanPlan{DataFrame: df},
		N:       3,
		Replace: false,
	}
	result, err := ExecuteParallel(plan)
	if err != nil {
		t.Fatalf("ExecuteParallel failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestExecuteParallelOrderByPlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{30, 10, 20}, nil))
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"c", "a", "b"}, nil))

	plan := dataframe.OrderByPlan{
		Input:      dataframe.ScanPlan{DataFrame: df},
		Column:     "age",
		Descending: false,
	}
	result, err := ExecuteParallel(plan)
	if err != nil {
		t.Fatalf("ExecuteParallel failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}
