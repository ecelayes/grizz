package engine

import (
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

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
