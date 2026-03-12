package engine

import (
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestExecuteScanPlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	plan := dataframe.ScanPlan{DataFrame: df}
	result, err := Execute(plan)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestExecuteFilterPlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30}, nil))

	plan := dataframe.FilterPlan{
		Input:     dataframe.ScanPlan{DataFrame: df},
		Condition: expr.Col("age").Gt(expr.Lit(15)),
	}
	result, err := Execute(plan)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestExecuteSelectPlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20}, nil))
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"a", "b"}, nil))

	plan := dataframe.SelectPlan{
		Input:   dataframe.ScanPlan{DataFrame: df},
		Columns: []expr.Expr{expr.Col("age")},
	}
	result, err := Execute(plan)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if result.NumCols() != 1 {
		t.Errorf("Expected 1 column, got %d", result.NumCols())
	}
}

func TestExecuteLimitPlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30, 40, 50}, nil))

	plan := dataframe.LimitPlan{
		Input: dataframe.ScanPlan{DataFrame: df},
		Limit: 3,
	}
	result, err := Execute(plan)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestExecuteTailPlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30, 40, 50}, nil))

	plan := dataframe.TailPlan{
		Input: dataframe.ScanPlan{DataFrame: df},
		N:     2,
	}
	result, err := Execute(plan)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestExecuteSamplePlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30, 40, 50}, nil))

	plan := dataframe.SamplePlan{
		Input:   dataframe.ScanPlan{DataFrame: df},
		N:       2,
		Frac:    0,
		Replace: false,
	}
	result, err := Execute(plan)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestExecuteDistinctPlanWithNulls(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 10, 20, 30}, nil))

	plan := dataframe.DistinctPlan{
		Input: dataframe.ScanPlan{DataFrame: df},
	}
	result, err := Execute(plan)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestExecuteDropNullsPlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 0, 30}, []bool{true, false, true}))

	plan := dataframe.DropNullsPlan{
		Input: dataframe.ScanPlan{DataFrame: df},
	}
	result, err := Execute(plan)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestExecuteGroupByPlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("dept", memory.DefaultAllocator, []string{"A", "A", "B", "B"}, nil))
	df.AddSeries(series.NewInt64Series("salary", memory.DefaultAllocator, []int64{100, 200, 150, 250}, nil))

	plan := dataframe.GroupByPlan{
		Input: dataframe.ScanPlan{DataFrame: df},
		Keys:  []string{"dept"},
		Aggs:  []expr.Aggregation{expr.Sum("salary")},
	}
	result, err := Execute(plan)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestExecuteOrderByPlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{30, 10, 20}, nil))

	plan := dataframe.OrderByPlan{
		Input:      dataframe.ScanPlan{DataFrame: df},
		Column:     "age",
		Descending: false,
	}
	result, err := Execute(plan)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	ageCol, _ := result.ColByName("age")
	if ageCol.(*series.Int64Series).Value(0) != 10 {
		t.Errorf("Expected first value 10, got %d", ageCol.(*series.Int64Series).Value(0))
	}
}

func TestExecuteJoinPlan(t *testing.T) {
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
	result, err := Execute(plan)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if result.NumRows() != 1 {
		t.Errorf("Expected 1 row, got %d", result.NumRows())
	}
}

func TestExecuteWindowPlanRowNumber(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("id", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"a", "b", "c"}, nil))

	plan := dataframe.WindowPlan{
		Input:   dataframe.ScanPlan{DataFrame: df},
		Func:    expr.RowNumber(),
		PartBy:  []string{},
		OrderBy: []string{},
	}
	result, err := Execute(plan)
	if err != nil {
		t.Fatalf("Execute WindowPlan failed: %v", err)
	}
	if result.NumCols() != 3 {
		t.Errorf("Expected 3 columns, got %d", result.NumCols())
	}
}

func TestExecuteWindowPlanRank(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("id", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	plan := dataframe.WindowPlan{
		Input:   dataframe.ScanPlan{DataFrame: df},
		Func:    expr.Rank(),
		PartBy:  []string{},
		OrderBy: []string{},
	}
	result, err := Execute(plan)
	if err != nil {
		t.Fatalf("Execute WindowPlan failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestExecuteWindowPlanLead(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("id", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	plan := dataframe.WindowPlan{
		Input:   dataframe.ScanPlan{DataFrame: df},
		Func:    expr.Lead(expr.Col("id"), 1),
		PartBy:  []string{},
		OrderBy: []string{},
	}
	result, err := Execute(plan)
	if err != nil {
		t.Fatalf("Execute WindowPlan failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestExecuteWindowPlanLag(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("id", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	plan := dataframe.WindowPlan{
		Input:   dataframe.ScanPlan{DataFrame: df},
		Func:    expr.Lag(expr.Col("id"), 1),
		PartBy:  []string{},
		OrderBy: []string{},
	}
	result, err := Execute(plan)
	if err != nil {
		t.Fatalf("Execute WindowPlan failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestExecuteDistinctPlanMultiCol(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("id", memory.DefaultAllocator, []int64{1, 2, 1, 3, 2}, nil))
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"a", "b", "a", "c", "b"}, nil))

	plan := dataframe.DistinctPlan{
		Input: dataframe.ScanPlan{DataFrame: df},
	}
	result, err := Execute(plan)
	if err != nil {
		t.Fatalf("Execute DistinctPlan failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestExecuteWithColumnsPlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30}, nil))

	plan := dataframe.WithColumnsPlan{
		Input: dataframe.ScanPlan{DataFrame: df},
		Columns: []expr.Expr{
			expr.FillNull(expr.Col("age"), expr.Lit(0)),
		},
	}
	result, err := Execute(plan)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

type testUnknownPlan struct{}

func (testUnknownPlan) Explain(indent int) string {
	return "test"
}

func TestExecuteUnknownPlan(t *testing.T) {
	plan := testUnknownPlan{}
	_, err := Execute(plan)
	if err == nil {
		t.Errorf("Expected error for unknown plan type")
	}
	if err.Error() != "unknown logical plan node" {
		t.Errorf("Expected 'unknown logical plan node' error, got: %v", err)
	}
}
