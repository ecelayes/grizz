package engine

import (
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestRollingSum(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("test", memory.DefaultAllocator, []int64{1, 2, 3, 4}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.RollingSum(expr.Col("test"), 2, 2).Alias("rolling_sum"),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}

	resCol, err := result.ColByName("rolling_sum")
	if err != nil {
		t.Fatalf("ColByName failed: %v", err)
	}
	res := resCol.(*series.Float64Series)

	if !res.IsNull(0) {
		t.Errorf("Expected null at index 0, got %v", res.Value(0))
	}
	if res.IsNull(1) || res.Value(1) != 3 {
		t.Errorf("Expected 3 at index 1, got %v", res.Value(1))
	}
	if res.IsNull(2) || res.Value(2) != 5 {
		t.Errorf("Expected 5 at index 2, got %v", res.Value(2))
	}
	if res.IsNull(3) || res.Value(3) != 7 {
		t.Errorf("Expected 7 at index 3, got %v", res.Value(3))
	}

	result.Release()
}

func TestRollingMean(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("test", memory.DefaultAllocator, []int64{1, 2, 3, 4}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.RollingMean(expr.Col("test"), 2, 2).Alias("rolling_mean"),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}

	resCol, _ := result.ColByName("rolling_mean")
	res := resCol.(*series.Float64Series)

	if !res.IsNull(0) {
		t.Errorf("Expected null at index 0, got %v", res.Value(0))
	}
	if res.IsNull(1) || res.Value(1) != 1.5 {
		t.Errorf("Expected 1.5 at index 1, got %v", res.Value(1))
	}
	if res.IsNull(2) || res.Value(2) != 2.5 {
		t.Errorf("Expected 2.5 at index 2, got %v", res.Value(2))
	}
	if res.IsNull(3) || res.Value(3) != 3.5 {
		t.Errorf("Expected 3.5 at index 3, got %v", res.Value(3))
	}

	result.Release()
}

func TestRollingMin(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("test", memory.DefaultAllocator, []int64{4, 2, 3, 1}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.RollingMin(expr.Col("test"), 2, 2).Alias("rolling_min"),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}

	resCol, _ := result.ColByName("rolling_min")
	res := resCol.(*series.Float64Series)

	if !res.IsNull(0) {
		t.Errorf("Expected null at index 0, got %v", res.Value(0))
	}
	if res.IsNull(1) || res.Value(1) != 2 {
		t.Errorf("Expected 2 at index 1, got %v", res.Value(1))
	}
	if res.IsNull(2) || res.Value(2) != 2 {
		t.Errorf("Expected 2 at index 2, got %v", res.Value(2))
	}
	if res.IsNull(3) || res.Value(3) != 1 {
		t.Errorf("Expected 1 at index 3, got %v", res.Value(3))
	}

	result.Release()
}

func TestRollingMax(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("test", memory.DefaultAllocator, []int64{1, 3, 2, 4}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.RollingMax(expr.Col("test"), 2, 2).Alias("rolling_max"),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}

	resCol, _ := result.ColByName("rolling_max")
	res := resCol.(*series.Float64Series)

	if !res.IsNull(0) {
		t.Errorf("Expected null at index 0, got %v", res.Value(0))
	}
	if res.IsNull(1) || res.Value(1) != 3 {
		t.Errorf("Expected 3 at index 1, got %v", res.Value(1))
	}
	if res.IsNull(2) || res.Value(2) != 3 {
		t.Errorf("Expected 3 at index 2, got %v", res.Value(2))
	}
	if res.IsNull(3) || res.Value(3) != 4 {
		t.Errorf("Expected 4 at index 3, got %v", res.Value(3))
	}

	result.Release()
}

func TestRollingWithNulls(t *testing.T) {
	df := dataframe.New()
	valid := []bool{true, false, true, true}
	df.AddSeries(series.NewInt64Series("test", memory.DefaultAllocator, []int64{1, 0, 3, 4}, valid))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.RollingSum(expr.Col("test"), 2, 2).Alias("rolling_sum"),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}

	resCol, _ := result.ColByName("rolling_sum")
	res := resCol.(*series.Float64Series)

	if !res.IsNull(0) {
		t.Errorf("Expected null at index 0, got %v", res.Value(0))
	}
	if !res.IsNull(1) {
		t.Errorf("Expected null at index 1, got %v", res.Value(1))
	}
	if !res.IsNull(2) {
		t.Errorf("Expected null at index 2, got %v", res.Value(2))
	}
	if res.IsNull(3) || res.Value(3) != 7 {
		t.Errorf("Expected 7 at index 3, got %v", res.Value(3))
	}

	result.Release()
}
