package engine

import (
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

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

func TestEvaluateConditionIsInInt(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30, 40}, nil))

	mask, err := evaluateCondition(df, expr.Col("age").IsIn([]any{20, 30}))
	if err != nil {
		t.Fatalf("evaluateCondition failed: %v", err)
	}
	if mask[0] || !mask[1] || !mask[2] || mask[3] {
		t.Errorf("Expected [false, true, true, false], got %v", mask)
	}
}

func TestEvaluateConditionIsInString(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"alice", "bob", "charlie", "dave"}, nil))

	mask, err := evaluateCondition(df, expr.Col("name").IsIn([]any{"alice", "charlie"}))
	if err != nil {
		t.Fatalf("evaluateCondition failed: %v", err)
	}
	if !mask[0] || mask[1] || !mask[2] || mask[3] {
		t.Errorf("Expected [true, false, true, false], got %v", mask)
	}
}

func TestEvaluateConditionIsInWithNull(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30}, []bool{true, false, true}))

	mask, err := evaluateCondition(df, expr.Col("age").IsIn([]any{20, 30}))
	if err != nil {
		t.Fatalf("evaluateCondition failed: %v", err)
	}
	if mask[0] || mask[1] || !mask[2] {
		t.Errorf("Expected [false, false, true], got %v", mask)
	}
}
