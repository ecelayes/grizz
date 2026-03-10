package engine

import (
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestOptimizerNew(t *testing.T) {
	opt := NewOptimizer()
	if opt == nil {
		t.Error("Expected optimizer, got nil")
	}
}

func TestOptimizeNil(t *testing.T) {
	result := Optimize(nil)
	if result != nil {
		t.Error("Expected nil, got plan")
	}
}

func TestOptimizeScanPlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	plan := dataframe.ScanPlan{DataFrame: df}
	result := Optimize(plan)
	if result == nil {
		t.Error("Expected optimized plan, got nil")
	}
}

func TestOptimizeFilterPlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	plan := dataframe.FilterPlan{
		Input:     dataframe.ScanPlan{DataFrame: df},
		Condition: expr.Col("a").Gt(expr.Lit(1)),
	}
	result := Optimize(plan)
	if result == nil {
		t.Error("Expected optimized plan, got nil")
	}
}

func TestOptimizeSelectPlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	plan := dataframe.SelectPlan{
		Input:   dataframe.ScanPlan{DataFrame: df},
		Columns: []expr.Expr{expr.Col("a")},
	}
	result := Optimize(plan)
	if result == nil {
		t.Error("Expected optimized plan, got nil")
	}
}

func TestOptimizeWithColumnsPlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	plan := dataframe.WithColumnsPlan{
		Input:   dataframe.ScanPlan{DataFrame: df},
		Columns: []expr.Expr{expr.Col("a")},
	}
	result := Optimize(plan)
	if result == nil {
		t.Error("Expected optimized plan, got nil")
	}
}

func TestOptimizeJoinPlan(t *testing.T) {
	left := dataframe.New()
	left.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2"}, nil))

	right := dataframe.New()
	right.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "3"}, nil))

	plan := dataframe.JoinPlan{
		Left:  dataframe.ScanPlan{DataFrame: left},
		Right: dataframe.ScanPlan{DataFrame: right},
		On:    "id",
		How:   dataframe.Inner,
	}
	result := Optimize(plan)
	if result == nil {
		t.Error("Expected optimized plan, got nil")
	}
}

func TestOptimizeGroupByPlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("group", memory.DefaultAllocator, []string{"a", "a", "b"}, nil))
	df.AddSeries(series.NewInt64Series("value", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	plan := dataframe.GroupByPlan{
		Input: dataframe.ScanPlan{DataFrame: df},
		Keys:  []string{"group"},
		Aggs:  []expr.Aggregation{{Expr: expr.Column{Name: "value"}, Func: expr.SumAgg}},
	}
	result := Optimize(plan)
	if result == nil {
		t.Error("Expected optimized plan, got nil")
	}
}

func TestOptimizeOrderByPlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{3, 1, 2}, nil))

	plan := dataframe.OrderByPlan{
		Input: dataframe.ScanPlan{DataFrame: df},
	}
	result := Optimize(plan)
	if result == nil {
		t.Error("Expected optimized plan, got nil")
	}
}

func TestOptimizeLimitPlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	plan := dataframe.LimitPlan{
		Input: dataframe.ScanPlan{DataFrame: df},
		Limit: 2,
	}
	result := Optimize(plan)
	if result == nil {
		t.Error("Expected optimized plan, got nil")
	}
}

func TestOptimizeDistinctPlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 1}, nil))

	plan := dataframe.DistinctPlan{
		Input: dataframe.ScanPlan{DataFrame: df},
	}
	result := Optimize(plan)
	if result == nil {
		t.Error("Expected optimized plan, got nil")
	}
}

func TestPushDownFilter(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	filterPlan := dataframe.FilterPlan{
		Input:     dataframe.ScanPlan{DataFrame: df},
		Condition: expr.Col("a").Gt(expr.Lit(1)),
	}

	opt := NewOptimizer()
	result := opt.pushDownFilter(filterPlan)
	if result.Input == nil {
		t.Error("Expected pushed down filter")
	}
}

func TestCanPushDown(t *testing.T) {
	opt := NewOptimizer()

	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	if !opt.canPushDown(dataframe.ScanPlan{DataFrame: df}) {
		t.Error("Expected canPushDown to return true for ScanPlan")
	}

	if !opt.canPushDown(dataframe.FilterPlan{Input: dataframe.ScanPlan{DataFrame: df}}) {
		t.Error("Expected canPushDown to return true for FilterPlan")
	}

	if opt.canPushDown(dataframe.SelectPlan{Input: dataframe.ScanPlan{DataFrame: df}}) {
		t.Error("Expected canPushDown to return false for SelectPlan")
	}
}

func TestPushDownFilterToSelectPlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	df.AddSeries(series.NewInt64Series("b", memory.DefaultAllocator, []int64{4, 5, 6}, nil))

	selectPlan := dataframe.SelectPlan{
		Input:   dataframe.ScanPlan{DataFrame: df},
		Columns: []expr.Expr{expr.Col("a")},
	}

	filterPlan := dataframe.FilterPlan{
		Input:     selectPlan,
		Condition: expr.Col("a").Gt(expr.Lit(1)),
	}

	opt := NewOptimizer()
	result := opt.pushDownFilter(filterPlan)
	if result.Input == nil {
		t.Error("Expected pushed down filter")
	}
}

func TestPushDownFilterToWithColumnsPlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	withColumnsPlan := dataframe.WithColumnsPlan{
		Input:   dataframe.ScanPlan{DataFrame: df},
		Columns: []expr.Expr{expr.Col("a")},
	}

	filterPlan := dataframe.FilterPlan{
		Input:     withColumnsPlan,
		Condition: expr.Col("a").Gt(expr.Lit(1)),
	}

	opt := NewOptimizer()
	result := opt.pushDownFilter(filterPlan)
	if result.Input == nil {
		t.Error("Expected pushed down filter")
	}
}

func TestPushDownFilterToLimitPlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	limitPlan := dataframe.LimitPlan{
		Input: dataframe.ScanPlan{DataFrame: df},
		Limit: 2,
	}

	filterPlan := dataframe.FilterPlan{
		Input:     limitPlan,
		Condition: expr.Col("a").Gt(expr.Lit(1)),
	}

	opt := NewOptimizer()
	result := opt.pushDownFilter(filterPlan)
	if result.Input == nil {
		t.Error("Expected pushed down filter")
	}
}

func TestPushDownFilterToJoinLeftOnly(t *testing.T) {
	left := dataframe.New()
	left.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2"}, nil))

	right := dataframe.New()
	right.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "3"}, nil))

	leftFiltered := dataframe.FilterPlan{
		Input:     dataframe.ScanPlan{DataFrame: left},
		Condition: expr.Col("id").Eq(expr.Lit("1")),
	}

	joinPlan := dataframe.JoinPlan{
		Left:  leftFiltered,
		Right: dataframe.ScanPlan{DataFrame: right},
		On:    "id",
		How:   dataframe.Inner,
	}

	opt := NewOptimizer()
	result := opt.pushDownFilterToJoin(joinPlan)
	if result.Left == nil {
		t.Error("Expected left to be set")
	}
}

func TestPushDownFilterToJoinRightOnly(t *testing.T) {
	left := dataframe.New()
	left.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2"}, nil))

	right := dataframe.New()
	right.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "3"}, nil))

	rightFiltered := dataframe.FilterPlan{
		Input:     dataframe.ScanPlan{DataFrame: right},
		Condition: expr.Col("id").Eq(expr.Lit("1")),
	}

	joinPlan := dataframe.JoinPlan{
		Left:  dataframe.ScanPlan{DataFrame: left},
		Right: rightFiltered,
		On:    "id",
		How:   dataframe.Inner,
	}

	opt := NewOptimizer()
	result := opt.pushDownFilterToJoin(joinPlan)
	if result.Right == nil {
		t.Error("Expected right to be set")
	}
}

func TestPushDownFilterToJoinBoth(t *testing.T) {
	left := dataframe.New()
	left.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2"}, nil))

	right := dataframe.New()
	right.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "3"}, nil))

	leftFiltered := dataframe.FilterPlan{
		Input:     dataframe.ScanPlan{DataFrame: left},
		Condition: expr.Col("id").Eq(expr.Lit("1")),
	}

	rightFiltered := dataframe.FilterPlan{
		Input:     dataframe.ScanPlan{DataFrame: right},
		Condition: expr.Col("id").Eq(expr.Lit("1")),
	}

	joinPlan := dataframe.JoinPlan{
		Left:  leftFiltered,
		Right: rightFiltered,
		On:    "id",
		How:   dataframe.Inner,
	}

	opt := NewOptimizer()
	result := opt.pushDownFilterToJoin(joinPlan)
	if result.Left == nil || result.Right == nil {
		t.Error("Expected both left and right to be set")
	}
}

func TestOptimizeFilterWithInnerFilter(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	innerFilter := dataframe.FilterPlan{
		Input:     dataframe.ScanPlan{DataFrame: df},
		Condition: expr.Col("a").Gt(expr.Lit(0)),
	}

	outerFilter := dataframe.FilterPlan{
		Input:     innerFilter,
		Condition: expr.Col("a").Lt(expr.Lit(3)),
	}

	opt := NewOptimizer()
	result := opt.optimizeFilter(outerFilter)
	if result.Input == nil {
		t.Error("Expected optimized filter")
	}
}

func TestOptimizeJoinWithSelect(t *testing.T) {
	left := dataframe.New()
	left.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "2"}, nil))
	left.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"a", "b"}, nil))

	right := dataframe.New()
	right.AddSeries(series.NewStringSeries("id", memory.DefaultAllocator, []string{"1", "3"}, nil))

	leftWithSelect := dataframe.SelectPlan{
		Input:   dataframe.ScanPlan{DataFrame: left},
		Columns: []expr.Expr{expr.Col("id")},
	}

	joinPlan := dataframe.JoinPlan{
		Left:  leftWithSelect,
		Right: dataframe.ScanPlan{DataFrame: right},
		On:    "id",
		How:   dataframe.Inner,
	}

	opt := NewOptimizer()
	result := opt.optimizeJoin(joinPlan)
	if result.Left == nil {
		t.Error("Expected optimized join")
	}
}

func TestOptimizeLimitWithInnerLimit(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	innerLimit := dataframe.LimitPlan{
		Input: dataframe.ScanPlan{DataFrame: df},
		Limit: 5,
	}

	outerLimit := dataframe.LimitPlan{
		Input: innerLimit,
		Limit: 2,
	}

	opt := NewOptimizer()
	result := opt.optimizeLimit(outerLimit)
	if result.Limit != 2 {
		t.Error("Expected limit to be 2")
	}
}

func TestOptimizeSelectWithInnerSelect(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	df.AddSeries(series.NewInt64Series("b", memory.DefaultAllocator, []int64{4, 5, 6}, nil))

	innerSelect := dataframe.SelectPlan{
		Input:   dataframe.ScanPlan{DataFrame: df},
		Columns: []expr.Expr{expr.Col("a"), expr.Col("b")},
	}

	outerSelect := dataframe.SelectPlan{
		Input:   innerSelect,
		Columns: []expr.Expr{expr.Col("a")},
	}

	opt := NewOptimizer()
	result := opt.optimizeSelect(outerSelect)
	if result.Input == nil {
		t.Error("Expected optimized select")
	}
}

func TestOptimizeWithTailPlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	plan := dataframe.TailPlan{
		Input: dataframe.ScanPlan{DataFrame: df},
		N:     2,
	}
	result := Optimize(plan)
	if result == nil {
		t.Error("Expected optimized plan, got nil")
	}
}

func TestOptimizeWithSamplePlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	plan := dataframe.SamplePlan{
		Input:   dataframe.ScanPlan{DataFrame: df},
		N:       2,
		Replace: false,
	}
	result := Optimize(plan)
	if result == nil {
		t.Error("Expected optimized plan, got nil")
	}
}

func TestOptimizeWithWindowPlan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	plan := dataframe.WindowPlan{
		Input: dataframe.ScanPlan{DataFrame: df},
		Func:  expr.WindowExpr{Func: expr.FuncRowNumber},
	}
	result := Optimize(plan)
	if result == nil {
		t.Error("Expected optimized plan, got nil")
	}
}
