package engine

import (
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

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
