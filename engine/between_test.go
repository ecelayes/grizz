package engine

import (
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestEvaluateConditionBetweenInt(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 5, 10, 15, 20}, nil))

	mask, err := evaluateCondition(df, expr.Col("a").Between(5, 15))
	if err != nil {
		t.Fatalf("evaluateCondition failed: %v", err)
	}
	if mask[0] != false {
		t.Errorf("Expected first value false, got %v", mask[0])
	}
	if mask[1] != true {
		t.Errorf("Expected second value true, got %v", mask[1])
	}
	if mask[2] != true {
		t.Errorf("Expected third value true, got %v", mask[2])
	}
	if mask[3] != true {
		t.Errorf("Expected fourth value true, got %v", mask[3])
	}
	if mask[4] != false {
		t.Errorf("Expected fifth value false, got %v", mask[4])
	}
}

func TestEvaluateConditionBetweenFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("a", memory.DefaultAllocator, []float64{1.5, 5.5, 10.5, 15.5, 20.5}, nil))

	mask, err := evaluateCondition(df, expr.Col("a").Between(5.0, 15.0))
	if err != nil {
		t.Fatalf("evaluateCondition failed: %v", err)
	}
	if mask[0] != false {
		t.Errorf("Expected first value false, got %v", mask[0])
	}
	if mask[1] != true {
		t.Errorf("Expected second value true, got %v", mask[1])
	}
	if mask[2] != true {
		t.Errorf("Expected third value true, got %v", mask[2])
	}
}

func TestFilterBetweenInt(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 5, 10, 15, 20}, nil))

	plan := dataframe.FilterPlan{
		Input:     dataframe.ScanPlan{DataFrame: df},
		Condition: expr.Col("a").Between(5, 15),
	}
	result, err := Execute(plan)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestFilterBetweenFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("a", memory.DefaultAllocator, []float64{1.5, 5.5, 10.5, 15.5, 20.5}, nil))

	plan := dataframe.FilterPlan{
		Input:     dataframe.ScanPlan{DataFrame: df},
		Condition: expr.Col("a").Between(5.0, 15.0),
	}
	result, err := Execute(plan)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows (5.5 and 10.5), got %d", result.NumRows())
	}
}

func TestWithColumnsBetween(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 5, 10, 15, 20}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Col("a").Between(5, 15).Alias("in_range"),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
	inRangeCol, _ := result.ColByName("in_range")
	inRange := inRangeCol.(*series.BooleanSeries)
	if inRange.Value(0) != false {
		t.Errorf("Expected first value false, got %v", inRange.Value(0))
	}
	if inRange.Value(1) != true {
		t.Errorf("Expected second value true, got %v", inRange.Value(1))
	}
	if inRange.Value(2) != true {
		t.Errorf("Expected third value true, got %v", inRange.Value(2))
	}
	if inRange.Value(4) != false {
		t.Errorf("Expected fifth value false, got %v", inRange.Value(4))
	}
}
