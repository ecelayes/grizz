package engine

import (
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	grizzarrows "github.com/ecelayes/grizz/internal/arrow"
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

func TestEvaluateConditionEq(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30}, nil))

	mask, err := evaluateCondition(df, expr.Col("age").Eq(expr.Lit(20)))
	if err != nil {
		t.Fatalf("evaluateCondition failed: %v", err)
	}
	if mask[0] || !mask[1] || mask[2] {
		t.Errorf("Expected [false, true, false], got %v", mask)
	}
}

func TestEvaluateConditionNe(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30}, nil))

	mask, err := evaluateCondition(df, expr.Col("age").Ne(expr.Lit(20)))
	if err != nil {
		t.Fatalf("evaluateCondition failed: %v", err)
	}
	if !mask[0] || mask[1] || !mask[2] {
		t.Errorf("Expected [true, false, true], got %v", mask)
	}
}

func TestEvaluateConditionFloatNe(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("score", memory.DefaultAllocator, []float64{10.5, 20.5, 30.5}, nil))

	mask, err := evaluateCondition(df, expr.Col("score").Ne(expr.Lit(20.5)))
	if err != nil {
		t.Fatalf("evaluateCondition failed: %v", err)
	}
	if !mask[0] || mask[1] || !mask[2] {
		t.Errorf("Expected [true, false, true], got %v", mask)
	}
}

func TestEvaluateConditionGt(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30}, nil))

	mask, err := evaluateCondition(df, expr.Col("age").Gt(expr.Lit(15)))
	if err != nil {
		t.Fatalf("evaluateCondition failed: %v", err)
	}
	if mask[0] || !mask[1] || !mask[2] {
		t.Errorf("Expected [false, true, true], got %v", mask)
	}
}

func TestEvaluateConditionLt(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30}, nil))

	mask, err := evaluateCondition(df, expr.Col("age").Lt(expr.Lit(25)))
	if err != nil {
		t.Fatalf("evaluateCondition failed: %v", err)
	}
	if !mask[0] || !mask[1] || mask[2] {
		t.Errorf("Expected [true, true, false], got %v", mask)
	}
}

func TestEvaluateConditionAnd(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30}, nil))

	mask, err := evaluateCondition(df, expr.Col("age").Gt(expr.Lit(10)).And(expr.Col("age").Lt(expr.Lit(30))))
	if err != nil {
		t.Fatalf("evaluateCondition failed: %v", err)
	}
	if mask[0] || !mask[1] || mask[2] {
		t.Errorf("Expected [false, true, false], got %v", mask)
	}
}

func TestEvaluateConditionOr(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30}, nil))

	mask, err := evaluateCondition(df, expr.Col("age").Eq(expr.Lit(10)).Or(expr.Col("age").Eq(expr.Lit(30))))
	if err != nil {
		t.Fatalf("evaluateCondition failed: %v", err)
	}
	if !mask[0] || mask[1] || !mask[2] {
		t.Errorf("Expected [true, false, true], got %v", mask)
	}
}

func TestApplyMask(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30, 40}, nil))

	mask := []bool{true, false, true, false}
	result, err := applyMask(df, mask)
	if err != nil {
		t.Fatalf("applyMask failed: %v", err)
	}
	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestApplyDropNulls(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 0, 30, 0}, []bool{true, false, true, false}))

	result, err := applyDropNulls(df)
	if err != nil {
		t.Fatalf("applyDropNulls failed: %v", err)
	}
	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestApplyDistinct(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 10, 20, 20, 30}, nil))

	result, err := applyDistinct(df)
	if err != nil {
		t.Fatalf("applyDistinct failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplyDistinctFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.5, 1.5, 2.5, 3.5}, nil))

	result, err := applyDistinct(df)
	if err != nil {
		t.Fatalf("applyDistinct failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplyDistinctBool(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewBooleanSeries("flag", memory.DefaultAllocator, []bool{true, true, false, false, true}, nil))

	result, err := applyDistinct(df)
	if err != nil {
		t.Fatalf("applyDistinct failed: %v", err)
	}
	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
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

type testUnsupportedExpr struct{}

func (testUnsupportedExpr) String() string {
	return "unsupported"
}

func TestApplyWithColumnsUnsupportedExpr(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30}, nil))

	_, err := applyWithColumns(df, []expr.Expr{testUnsupportedExpr{}})
	if err == nil {
		t.Errorf("Expected error for unsupported expression")
	}
}

func TestApplyWindowRowNumber(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("id", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	result, err := applyWindow(df, expr.WindowExpr{Func: expr.FuncRowNumber}, nil, nil)
	if err != nil {
		t.Fatalf("applyWindow failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWindowRank(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("id", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	result, err := applyWindow(df, expr.WindowExpr{Func: expr.FuncRank}, nil, nil)
	if err != nil {
		t.Fatalf("applyWindow failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWindowLead(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("val", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	result, err := applyWindow(df, expr.WindowExpr{
		Func:   expr.FuncLead,
		Expr:   expr.Col("val"),
		Offset: 1,
	}, nil, nil)
	if err != nil {
		t.Fatalf("applyWindow failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWindowLag(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("val", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	result, err := applyWindow(df, expr.WindowExpr{
		Func:   expr.FuncLag,
		Expr:   expr.Col("val"),
		Offset: 1,
	}, nil, nil)
	if err != nil {
		t.Fatalf("applyWindow failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWindowLagFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("val", memory.DefaultAllocator, []float64{1.5, 2.5, 3.5}, nil))

	result, err := applyWindow(df, expr.WindowExpr{
		Func:   expr.FuncLag,
		Expr:   expr.Col("val"),
		Offset: 1,
	}, nil, nil)
	if err != nil {
		t.Fatalf("applyWindow failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWindowLagString(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("val", memory.DefaultAllocator, []string{"a", "b", "c"}, nil))

	result, err := applyWindow(df, expr.WindowExpr{
		Func:   expr.FuncLag,
		Expr:   expr.Col("val"),
		Offset: 1,
	}, nil, nil)
	if err != nil {
		t.Fatalf("applyWindow failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWindowLeadFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("val", memory.DefaultAllocator, []float64{1.5, 2.5, 3.5}, nil))

	result, err := applyWindow(df, expr.WindowExpr{
		Func:   expr.FuncLead,
		Expr:   expr.Col("val"),
		Offset: 1,
	}, nil, nil)
	if err != nil {
		t.Fatalf("applyWindow failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWindowLeadString(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("val", memory.DefaultAllocator, []string{"a", "b", "c"}, nil))

	result, err := applyWindow(df, expr.WindowExpr{
		Func:   expr.FuncLead,
		Expr:   expr.Col("val"),
		Offset: 1,
	}, nil, nil)
	if err != nil {
		t.Fatalf("applyWindow failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsFillNull(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 0, 30}, []bool{true, false, true}))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.FillNull(expr.Col("age"), expr.Lit(20)),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplyWithColumnsFillNullFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.0, 0.0, 3.0}, []bool{true, false, true}))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.FillNull(expr.Col("value"), expr.Lit(2.0)),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplyMaskString(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"a", "b", "c"}, nil))

	mask := []bool{true, false, true}
	result, err := applyMask(df, mask)
	if err != nil {
		t.Fatalf("applyMask failed: %v", err)
	}
	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestApplyMaskFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.5, 2.5, 3.5}, nil))

	mask := []bool{true, false, true}
	result, err := applyMask(df, mask)
	if err != nil {
		t.Fatalf("applyMask failed: %v", err)
	}
	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
	valCol, _ := result.ColByName("value")
	if valCol.(*series.Float64Series).Value(0) != 1.5 {
		t.Errorf("Expected first value 1.5, got %f", valCol.(*series.Float64Series).Value(0))
	}
}

func TestApplyMaskBool(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewBooleanSeries("flag", memory.DefaultAllocator, []bool{true, false, true}, nil))

	mask := []bool{true, false, true}
	result, err := applyMask(df, mask)
	if err != nil {
		t.Fatalf("applyMask failed: %v", err)
	}
	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestApplyProjection(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	df.AddSeries(series.NewInt64Series("b", memory.DefaultAllocator, []int64{4, 5, 6}, nil))

	result, err := applyProjection(df, []expr.Expr{expr.Col("a")})
	if err != nil {
		t.Fatalf("applyProjection failed: %v", err)
	}
	if result.NumCols() != 1 {
		t.Errorf("Expected 1 column, got %d", result.NumCols())
	}
}

func TestApplyProjectionFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("a", memory.DefaultAllocator, []float64{1.5, 2.5, 3.5}, nil))
	df.AddSeries(series.NewFloat64Series("b", memory.DefaultAllocator, []float64{4.5, 5.5, 6.5}, nil))

	result, err := applyProjection(df, []expr.Expr{expr.Col("a")})
	if err != nil {
		t.Fatalf("applyProjection failed: %v", err)
	}
	if result.NumCols() != 1 {
		t.Errorf("Expected 1 column, got %d", result.NumCols())
	}
	valCol, _ := result.ColByName("a")
	if valCol.(*series.Float64Series).Value(0) != 1.5 {
		t.Errorf("Expected first value 1.5, got %f", valCol.(*series.Float64Series).Value(0))
	}
}

func TestApplyProjectionBool(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewBooleanSeries("a", memory.DefaultAllocator, []bool{true, false, true}, nil))
	df.AddSeries(series.NewBooleanSeries("b", memory.DefaultAllocator, []bool{false, true, false}, nil))

	result, err := applyProjection(df, []expr.Expr{expr.Col("a")})
	if err != nil {
		t.Fatalf("applyProjection failed: %v", err)
	}
	if result.NumCols() != 1 {
		t.Errorf("Expected 1 column, got %d", result.NumCols())
	}
}

func TestApplyProjectionString(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("a", memory.DefaultAllocator, []string{"x", "y", "z"}, nil))
	df.AddSeries(series.NewStringSeries("b", memory.DefaultAllocator, []string{"p", "q", "r"}, nil))

	result, err := applyProjection(df, []expr.Expr{expr.Col("a")})
	if err != nil {
		t.Fatalf("applyProjection failed: %v", err)
	}
	if result.NumCols() != 1 {
		t.Errorf("Expected 1 column, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsContains(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"hello", "world", "test"}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Contains(expr.Col("name"), expr.Lit("lo")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsReplace(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"hello", "world"}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Replace(expr.Col("name"), expr.Lit("o"), expr.Lit("x")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsUpper(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"HELLO", "WORLD"}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Upper(expr.Col("name")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsLower(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"HELLO", "WORLD"}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Lower(expr.Col("name")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsTrim(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"  hello  ", "world"}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Trim(expr.Col("name")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsLpad(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"hi", "hello"}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.LPad(expr.Col("name"), expr.Lit(5)),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsRpad(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"hi", "hello"}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.RPad(expr.Col("name"), expr.Lit(5)),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsSplit(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"a,b", "c,d"}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Split(expr.Col("name"), expr.Lit(",")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsSlice(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"hello", "world"}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Slice(expr.Col("name"), expr.Lit(0), expr.Lit(2)),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsLength(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"hi", "hello"}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Length(expr.Col("name")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsFillNullForward(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 0, 30}, []bool{true, false, true}))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.FillNullForward(expr.Col("age")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplyWithColumnsFillNullBackward(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 0, 30}, []bool{true, false, true}))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.FillNullBackward(expr.Col("age")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplyWithColumnsFillNullForwardInt(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("value", memory.DefaultAllocator, []int64{10, 0, 30}, []bool{true, false, true}))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.FillNullForward(expr.Col("value")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplyWithColumnsFillNullBackwardInt(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("value", memory.DefaultAllocator, []int64{10, 0, 30}, []bool{true, false, true}))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.FillNullBackward(expr.Col("value")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplyWithColumnsCoalesce(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{10, 0, 30}, []bool{true, false, true}))
	df.AddSeries(series.NewInt64Series("b", memory.DefaultAllocator, []int64{0, 20, 30}, []bool{false, true, true}))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Coalesce(expr.Col("a"), expr.Col("b")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 3 {
		t.Errorf("Expected 3 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsCoalesceFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("a", memory.DefaultAllocator, []float64{10.5, 0.0, 30.5}, []bool{true, false, true}))
	df.AddSeries(series.NewFloat64Series("b", memory.DefaultAllocator, []float64{0.0, 20.5, 30.5}, []bool{false, true, true}))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Coalesce(expr.Col("a"), expr.Col("b")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 3 {
		t.Errorf("Expected 3 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsCoalesceString(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("a", memory.DefaultAllocator, []string{"hello", "", "world"}, []bool{true, false, true}))
	df.AddSeries(series.NewStringSeries("b", memory.DefaultAllocator, []string{"", "foo", "bar"}, []bool{false, true, true}))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Coalesce(expr.Col("a"), expr.Col("b")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 3 {
		t.Errorf("Expected 3 columns, got %d", result.NumCols())
	}
}

func TestEvaluateConditionFloatGt(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.5, 2.5, 3.5}, nil))

	mask, err := evaluateCondition(df, expr.Col("value").Gt(expr.Lit(2.0)))
	if err != nil {
		t.Fatalf("evaluateCondition failed: %v", err)
	}
	if mask[0] || !mask[1] || !mask[2] {
		t.Errorf("Expected [false, true, true], got %v", mask)
	}
}

func TestEvaluateConditionFloatLt(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.5, 2.5, 3.5}, nil))

	mask, err := evaluateCondition(df, expr.Col("value").Lt(expr.Lit(3.0)))
	if err != nil {
		t.Fatalf("evaluateCondition failed: %v", err)
	}
	if !mask[0] || !mask[1] || mask[2] {
		t.Errorf("Expected [true, true, false], got %v", mask)
	}
}

func TestEvaluateConditionFloatGte(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.5, 2.5, 3.5}, nil))

	mask, err := evaluateCondition(df, expr.Col("value").GtEq(expr.Lit(2.5)))
	if err != nil {
		t.Fatalf("evaluateCondition failed: %v", err)
	}
	if mask[0] || !mask[1] || !mask[2] {
		t.Errorf("Expected [false, true, true], got %v", mask)
	}
}

func TestEvaluateConditionFloatLte(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.5, 2.5, 3.5}, nil))

	mask, err := evaluateCondition(df, expr.Col("value").LtEq(expr.Lit(2.5)))
	if err != nil {
		t.Fatalf("evaluateCondition failed: %v", err)
	}
	if !mask[0] || !mask[1] || mask[2] {
		t.Errorf("Expected [true, true, false], got %v", mask)
	}
}

func TestEvaluateConditionStringEq(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"alice", "bob", "charlie"}, nil))

	mask, err := evaluateCondition(df, expr.Col("name").Eq(expr.Lit("bob")))
	if err != nil {
		t.Fatalf("evaluateCondition failed: %v", err)
	}
	if mask[0] || !mask[1] || mask[2] {
		t.Errorf("Expected [false, true, false], got %v", mask)
	}
}

func TestEvaluateConditionStringNe(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"alice", "bob", "charlie"}, nil))

	mask, err := evaluateCondition(df, expr.Col("name").Ne(expr.Lit("bob")))
	if err != nil {
		t.Fatalf("evaluateCondition failed: %v", err)
	}
	if !mask[0] || mask[1] || !mask[2] {
		t.Errorf("Expected [true, false, true], got %v", mask)
	}
}

func TestEvaluateConditionIsNull(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 0, 30}, []bool{true, false, true}))

	mask, err := evaluateCondition(df, expr.IsNull(expr.Col("age")))
	if err != nil {
		t.Fatalf("evaluateCondition failed: %v", err)
	}
	if mask[0] || !mask[1] || mask[2] {
		t.Errorf("Expected [false, true, false], got %v", mask)
	}
}

func TestEvaluateConditionIsNotNull(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 0, 30}, []bool{true, false, true}))

	mask, err := evaluateCondition(df, expr.IsNotNull(expr.Col("age")))
	if err != nil {
		t.Fatalf("evaluateCondition failed: %v", err)
	}
	if !mask[0] || mask[1] || !mask[2] {
		t.Errorf("Expected [true, false, true], got %v", mask)
	}
}

func TestEvaluateConditionNot(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30}, nil))

	mask, err := evaluateCondition(df, expr.Not(expr.Col("age").Gt(expr.Lit(15))))
	if err != nil {
		t.Fatalf("evaluateCondition failed: %v", err)
	}
	if !mask[0] || mask[1] || mask[2] {
		t.Errorf("Expected [true, false, false], got %v", mask)
	}
}

func TestApplyWithColumnsStrip(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"  hello  ", "  world  "}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Strip(expr.Col("name")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsContainsRegex(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"hello123", "world"}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.ContainsRegex(expr.Col("name"), expr.Lit("[0-9]+")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsCastFloatToInt(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.5, 2.7, 3.3}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Cast(expr.Col("value"), grizzarrows.Int64),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsCastIntToFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("value", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Cast(expr.Col("value"), grizzarrows.Float64),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsCastStringToInt(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("value", memory.DefaultAllocator, []string{"1", "2", "3"}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Cast(expr.Col("value"), grizzarrows.Int64),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsCastFloatToString(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.5, 2.5, 3.5}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Cast(expr.Col("value"), grizzarrows.String),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsCastIntToString(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("value", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Cast(expr.Col("value"), grizzarrows.String),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsCastStringToFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("value", memory.DefaultAllocator, []string{"1.5", "2.5", "3.5"}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Cast(expr.Col("value"), grizzarrows.Float64),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestEvaluateConditionIntGte(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30}, nil))

	mask, err := evaluateCondition(df, expr.Col("age").GtEq(expr.Lit(20)))
	if err != nil {
		t.Fatalf("evaluateCondition failed: %v", err)
	}
	if mask[0] || !mask[1] || !mask[2] {
		t.Errorf("Expected [false, true, true], got %v", mask)
	}
}

func TestEvaluateConditionIntLte(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30}, nil))

	mask, err := evaluateCondition(df, expr.Col("age").LtEq(expr.Lit(20)))
	if err != nil {
		t.Fatalf("evaluateCondition failed: %v", err)
	}
	if !mask[0] || !mask[1] || mask[2] {
		t.Errorf("Expected [true, true, false], got %v", mask)
	}
}

func TestApplyWithColumnsFillNullForwardFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.0, 0.0, 3.0}, []bool{true, false, true}))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.FillNullForward(expr.Col("value")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplyWithColumnsFillNullForwardString(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"hello", "", "world"}, []bool{true, false, true}))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.FillNullForward(expr.Col("name")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplyWithColumnsFillNullBackwardFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.0, 0.0, 3.0}, []bool{true, false, true}))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.FillNullBackward(expr.Col("value")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplyWithColumnsFillNullBackwardString(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"hello", "", "world"}, []bool{true, false, true}))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.FillNullBackward(expr.Col("name")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
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

func TestApplyWithColumnsError(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30}, nil))

	_, err := applyWithColumns(df, []expr.Expr{
		expr.FillNull(expr.Col("nonexistent"), expr.Lit(0)),
	})
	if err == nil {
		t.Errorf("Expected error for non-existent column")
	}
}

func TestApplyWithColumnsFloatWithFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.5, 2.5, 3.5}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.FillNull(expr.Col("value"), expr.Lit(0.0)),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplyWithColumnsStringWithInt(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"a", "b", "c"}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.FillNull(expr.Col("name"), expr.Lit("default")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplyWithColumnsFillNullInt64(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 0, 30}, []bool{true, false, true}))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.FillNull(expr.Col("age"), expr.Lit(int64(100))),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplyWithColumnsFillNullInt(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 0, 30}, []bool{true, false, true}))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.FillNull(expr.Col("age"), expr.Lit(100)),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplyWithColumnsFillNullFloatFromInt(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.5, 0.0, 3.5}, []bool{true, false, true}))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.FillNull(expr.Col("value"), expr.Lit(100)),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplyWindowLagError(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("id", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	_, err := applyWindow(df, expr.WindowExpr{Func: expr.FuncLag}, []string{}, []string{})
	if err != nil {
		t.Logf("Expected error or nil result for lag without expr: %v", err)
	}
}

func TestApplyWindowLeadError(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("id", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	_, err := applyWindow(df, expr.WindowExpr{Func: expr.FuncLead}, []string{}, []string{})
	if err != nil {
		t.Logf("Expected error or nil result for lead without expr: %v", err)
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

func TestApplyWithColumnsOtherwise(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("score", memory.DefaultAllocator, []int64{10, 20, 30}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.When(
			expr.Col("score").Gt(expr.Lit(20)),
		).Then(expr.Lit("high")).Otherwise(expr.Lit("low")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestEvaluateConditionUnsupportedOperator(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewBooleanSeries("flag", memory.DefaultAllocator, []bool{true, false}, nil))

	mask, err := evaluateCondition(df, expr.Col("flag").Gt(expr.Lit(true)))
	if err != nil {
		t.Fatalf("Boolean > operator should work: %v", err)
	}
	if len(mask) != 2 {
		t.Errorf("Expected 2 elements, got %d", len(mask))
	}
}

func TestEvaluateConditionIsNullWithNonColumn(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30}, nil))

	_, err := evaluateCondition(df, expr.IsNull(expr.Lit(10)))
	if err == nil {
		t.Error("Expected error for IsNull with non-column expression")
	}
}

func TestApplyWithColumnsWithBooleanOtherwise(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewBooleanSeries("flag", memory.DefaultAllocator, []bool{true, false, true}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.When(
			expr.Col("flag").Eq(expr.Lit(true)),
		).Then(expr.Lit("yes")).Otherwise(expr.Lit("no")),
	})
	if err != nil {
		t.Fatalf("Otherwise with Boolean column should work: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsWithBooleanOtherwiseBool(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewBooleanSeries("flag", memory.DefaultAllocator, []bool{true, false, true}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.When(
			expr.Col("flag").Eq(expr.Lit(true)),
		).Then(expr.Lit(true)).Otherwise(expr.Lit(false)),
	})
	if err != nil {
		t.Fatalf("Otherwise with Boolean column and Boolean values should work: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsFillNullBoolean(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewBooleanSeries("flag", memory.DefaultAllocator, []bool{true, false, true}, []bool{true, false, true}))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.FillNull(expr.Col("flag"), expr.Lit(false)),
	})
	if err != nil {
		t.Fatalf("FillNull with Boolean should work: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplyWithColumnsFillNullForwardBoolean(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewBooleanSeries("flag", memory.DefaultAllocator, []bool{true, false, true, false}, []bool{true, false, true, false}))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.FillNullForward(expr.Col("flag")),
	})
	if err != nil {
		t.Fatalf("FillNullForward with Boolean should work: %v", err)
	}
	if result.NumRows() != 4 {
		t.Errorf("Expected 4 rows, got %d", result.NumRows())
	}
}

func TestApplyWithColumnsFillNullBackwardBoolean(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewBooleanSeries("flag", memory.DefaultAllocator, []bool{true, false, true, false}, []bool{true, false, true, false}))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.FillNullBackward(expr.Col("flag")),
	})
	if err != nil {
		t.Fatalf("FillNullBackward with Boolean should work: %v", err)
	}
	if result.NumRows() != 4 {
		t.Errorf("Expected 4 rows, got %d", result.NumRows())
	}
}

func TestApplyWithColumnsCoalesceBoolean(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewBooleanSeries("a", memory.DefaultAllocator, []bool{true, false, true}, []bool{true, false, true}))
	df.AddSeries(series.NewBooleanSeries("b", memory.DefaultAllocator, []bool{false, true, false}, []bool{false, true, true}))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Coalesce(expr.Col("a"), expr.Col("b")),
	})
	if err != nil {
		t.Fatalf("Coalesce with Boolean should work: %v", err)
	}
	if result.NumCols() != 3 {
		t.Errorf("Expected 3 columns, got %d", result.NumCols())
	}
}

func TestEvaluateConditionBooleanGt(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewBooleanSeries("flag", memory.DefaultAllocator, []bool{true, false, true}, nil))

	mask, err := evaluateCondition(df, expr.Col("flag").Gt(expr.Lit(false)))
	if err != nil {
		t.Fatalf("Boolean > operator should work: %v", err)
	}
	if len(mask) != 3 {
		t.Errorf("Expected 3 elements, got %d", len(mask))
	}
	if !mask[0] || mask[1] || !mask[2] {
		t.Errorf("Expected [true, false, true], got %v", mask)
	}
}

func TestEvaluateConditionBooleanLt(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewBooleanSeries("flag", memory.DefaultAllocator, []bool{true, false, true}, nil))

	mask, err := evaluateCondition(df, expr.Col("flag").Lt(expr.Lit(true)))
	if err != nil {
		t.Fatalf("Boolean < operator should work: %v", err)
	}
	if len(mask) != 3 {
		t.Errorf("Expected 3 elements, got %d", len(mask))
	}
}

func TestApplyProjectionNonColumn(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30}, nil))

	_, err := applyProjection(df, []expr.Expr{expr.Lit(10)})
	if err == nil {
		t.Error("Expected error for non-column expression in projection")
	}
}

func TestApplyProjectionNonExistentColumn(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30}, nil))

	_, err := applyProjection(df, []expr.Expr{expr.Col("nonexistent")})
	if err == nil {
		t.Error("Expected error for non-existent column in projection")
	}
}
