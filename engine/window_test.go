package engine

import (
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

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
