package engine

import (
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestApplyWithColumnsArithmeticColumnInt(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	df.AddSeries(series.NewInt64Series("b", memory.DefaultAllocator, []int64{10, 20, 30}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Col("a").Add(expr.Col("b")).Alias("sum"),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 3 {
		t.Errorf("Expected 3 columns, got %d", result.NumCols())
	}
	sumCol, _ := result.ColByName("sum")
	if sumCol.(*series.Int64Series).Value(0) != 11 {
		t.Errorf("Expected first sum 11, got %d", sumCol.(*series.Int64Series).Value(0))
	}
}

func TestApplyWithColumnsArithmeticColumnFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("a", memory.DefaultAllocator, []float64{1.5, 2.5, 3.5}, nil))
	df.AddSeries(series.NewFloat64Series("b", memory.DefaultAllocator, []float64{10.0, 20.0, 30.0}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Col("a").Add(expr.Col("b")).Alias("sum"),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	sumCol, _ := result.ColByName("sum")
	if sumCol.(*series.Float64Series).Value(0) != 11.5 {
		t.Errorf("Expected first sum 11.5, got %f", sumCol.(*series.Float64Series).Value(0))
	}
}

func TestApplyWithColumnsSubColumn(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{10, 20, 30}, nil))
	df.AddSeries(series.NewInt64Series("b", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Col("a").Sub(expr.Col("b")).Alias("diff"),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	diffCol, _ := result.ColByName("diff")
	if diffCol.(*series.Int64Series).Value(0) != 9 {
		t.Errorf("Expected first diff 9, got %d", diffCol.(*series.Int64Series).Value(0))
	}
}

func TestApplyWithColumnsMulColumn(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{2, 3, 4}, nil))
	df.AddSeries(series.NewInt64Series("b", memory.DefaultAllocator, []int64{5, 6, 7}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Col("a").Mul(expr.Col("b")).Alias("product"),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	prodCol, _ := result.ColByName("product")
	if prodCol.(*series.Int64Series).Value(0) != 10 {
		t.Errorf("Expected first product 10, got %d", prodCol.(*series.Int64Series).Value(0))
	}
}

func TestApplyWithColumnsDivColumn(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("a", memory.DefaultAllocator, []float64{10, 20, 30}, nil))
	df.AddSeries(series.NewFloat64Series("b", memory.DefaultAllocator, []float64{2, 4, 5}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Col("a").Div(expr.Col("b")).Alias("quotient"),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	quotCol, _ := result.ColByName("quotient")
	if quotCol.(*series.Float64Series).Value(0) != 5.0 {
		t.Errorf("Expected first quotient 5.0, got %f", quotCol.(*series.Float64Series).Value(0))
	}
}
