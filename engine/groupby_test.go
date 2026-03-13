package engine

import (
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestApplyGroupBySumFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("group", memory.DefaultAllocator, []string{"a", "a", "b", "b"}, nil))
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0, 4.0}, nil))

	result, err := applyGroupBy(df, []string{"group"}, []expr.Aggregation{
		{Expr: expr.Column{Name: "value"}, Func: expr.SumAgg},
	})
	if err != nil {
		t.Fatalf("applyGroupBy failed: %v", err)
	}

	sumVal, _ := result.ColByName("Sum_value")
	v0 := sumVal.(*series.Float64Series).Value(0)
	v1 := sumVal.(*series.Float64Series).Value(1)
	if (v0 == 3.0 && v1 == 7.0) || (v0 == 7.0 && v1 == 3.0) {
		return
	}
	t.Errorf("Expected sums 3.0 and 7.0, got %f and %f", v0, v1)
}

func TestApplyGroupByMeanFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("group", memory.DefaultAllocator, []string{"a", "a", "b"}, nil))
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.0, 3.0, 5.0}, nil))

	result, err := applyGroupBy(df, []string{"group"}, []expr.Aggregation{
		{Expr: expr.Column{Name: "value"}, Func: expr.MeanAgg},
	})
	if err != nil {
		t.Fatalf("applyGroupBy failed: %v", err)
	}

	meanVal, _ := result.ColByName("Mean_value")
	v0 := meanVal.(*series.Float64Series).Value(0)
	v1 := meanVal.(*series.Float64Series).Value(1)
	if (v0 == 2.0 && v1 == 5.0) || (v0 == 5.0 && v1 == 2.0) {
		return
	}
	t.Errorf("Expected means 2.0 and 5.0, got %f and %f", v0, v1)
}

func TestApplyGroupByMinMaxFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("group", memory.DefaultAllocator, []string{"a", "a", "b"}, nil))
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{5.0, 1.0, 3.0}, nil))

	result, err := applyGroupBy(df, []string{"group"}, []expr.Aggregation{
		{Expr: expr.Column{Name: "value"}, Func: expr.MinAgg},
		{Expr: expr.Column{Name: "value"}, Func: expr.MaxAgg},
	})
	if err != nil {
		t.Fatalf("applyGroupBy failed: %v", err)
	}

	minVal, _ := result.ColByName("Min_value")
	v0 := minVal.(*series.Float64Series).Value(0)
	v1 := minVal.(*series.Float64Series).Value(1)
	if (v0 == 1.0 && v1 == 3.0) || (v0 == 3.0 && v1 == 1.0) {
	} else {
		t.Errorf("Expected mins 1.0 and 3.0, got %f and %f", v0, v1)
	}

	maxVal, _ := result.ColByName("Max_value")
	mv0 := maxVal.(*series.Float64Series).Value(0)
	mv1 := maxVal.(*series.Float64Series).Value(1)
	if (mv0 == 5.0 && mv1 == 3.0) || (mv0 == 3.0 && mv1 == 5.0) {
	} else {
		t.Errorf("Expected maxs 5.0 and 3.0, got %f and %f", mv0, mv1)
	}
}

func TestApplyGroupByCountFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("group", memory.DefaultAllocator, []string{"a", "a", "b"}, nil))
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0}, nil))

	result, err := applyGroupBy(df, []string{"group"}, []expr.Aggregation{
		{Expr: expr.Column{Name: "value"}, Func: expr.CountAgg},
	})
	if err != nil {
		t.Fatalf("applyGroupBy failed: %v", err)
	}

	countVal, _ := result.ColByName("Count_value")
	v0 := countVal.(*series.Float64Series).Value(0)
	v1 := countVal.(*series.Float64Series).Value(1)
	if (v0 == 2.0 && v1 == 1.0) || (v0 == 1.0 && v1 == 2.0) {
		return
	}
	t.Errorf("Expected counts 2.0 and 1.0, got %f and %f", v0, v1)
}

func TestApplyGroupByStdFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("group", memory.DefaultAllocator, []string{"a", "a"}, nil))
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.0, 3.0}, nil))

	result, err := applyGroupBy(df, []string{"group"}, []expr.Aggregation{
		{Expr: expr.Column{Name: "value"}, Func: expr.StdAgg},
	})
	if err != nil {
		t.Fatalf("applyGroupBy failed: %v", err)
	}

	stdVal, _ := result.ColByName("Std_value")
	if stdVal.(*series.Float64Series).Value(0) < 0.9 || stdVal.(*series.Float64Series).Value(0) > 1.1 {
		t.Errorf("Expected std ~1.0, got %f", stdVal.(*series.Float64Series).Value(0))
	}
}

func TestApplyGroupByVarianceFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("group", memory.DefaultAllocator, []string{"a", "a"}, nil))
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.0, 3.0}, nil))

	result, err := applyGroupBy(df, []string{"group"}, []expr.Aggregation{
		{Expr: expr.Column{Name: "value"}, Func: expr.VarAgg},
	})
	if err != nil {
		t.Fatalf("applyGroupBy failed: %v", err)
	}

	varVal, _ := result.ColByName("Var_value")
	if varVal.(*series.Float64Series).Value(0) != 1.0 {
		t.Errorf("Expected variance 1.0, got %f", varVal.(*series.Float64Series).Value(0))
	}
}

func TestApplyGroupByMedianFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("group", memory.DefaultAllocator, []string{"a", "a", "a"}, nil))
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0}, nil))

	result, err := applyGroupBy(df, []string{"group"}, []expr.Aggregation{
		{Expr: expr.Column{Name: "value"}, Func: expr.MedianAgg},
	})
	if err != nil {
		t.Fatalf("applyGroupBy failed: %v", err)
	}

	medianVal, _ := result.ColByName("Median_value")
	if medianVal.(*series.Float64Series).Value(0) != 2.0 {
		t.Errorf("Expected median 2.0, got %f", medianVal.(*series.Float64Series).Value(0))
	}
}

func TestApplyGroupByQuantileFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("group", memory.DefaultAllocator, []string{"a", "a", "a", "a"}, nil))
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0, 4.0}, nil))

	result, err := applyGroupBy(df, []string{"group"}, []expr.Aggregation{
		{Expr: expr.Column{Name: "value"}, Func: expr.QuantileAgg, Param: 0.25},
	})
	if err != nil {
		t.Fatalf("applyGroupBy failed: %v", err)
	}

	quantileVal, _ := result.ColByName("Quantile_value")
	if quantileVal.(*series.Float64Series).Value(0) != 1.75 {
		t.Errorf("Expected quantile 0.25 to be 1.75, got %f", quantileVal.(*series.Float64Series).Value(0))
	}
}

func TestApplyGroupByNUniqueFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("group", memory.DefaultAllocator, []string{"a", "a", "a"}, nil))
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.0, 1.0, 2.0}, nil))

	result, err := applyGroupBy(df, []string{"group"}, []expr.Aggregation{
		{Expr: expr.Column{Name: "value"}, Func: expr.NUniqueAgg},
	})
	if err != nil {
		t.Fatalf("applyGroupBy failed: %v", err)
	}

	nuniqueVal, _ := result.ColByName("NUnique_value")
	if nuniqueVal.(*series.Float64Series).Value(0) != 2.0 {
		t.Errorf("Expected nunique 2.0, got %f", nuniqueVal.(*series.Float64Series).Value(0))
	}
}

func TestApplyGroupByFirstLastFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("group", memory.DefaultAllocator, []string{"a", "a"}, nil))
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.0, 2.0}, nil))

	result, err := applyGroupBy(df, []string{"group"}, []expr.Aggregation{
		{Expr: expr.Column{Name: "value"}, Func: expr.FirstAgg},
		{Expr: expr.Column{Name: "value"}, Func: expr.LastAgg},
	})
	if err != nil {
		t.Fatalf("applyGroupBy failed: %v", err)
	}

	firstVal, _ := result.ColByName("First_value")
	if firstVal.(*series.Float64Series).Value(0) != 1.0 {
		t.Errorf("Expected first 1.0, got %f", firstVal.(*series.Float64Series).Value(0))
	}

	lastVal, _ := result.ColByName("Last_value")
	if lastVal.(*series.Float64Series).Value(0) != 2.0 {
		t.Errorf("Expected last 2.0, got %f", lastVal.(*series.Float64Series).Value(0))
	}
}

func TestApplyGroupByArgMinArgMaxFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("group", memory.DefaultAllocator, []string{"a", "a", "a"}, nil))
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{5.0, 1.0, 3.0}, nil))

	result, err := applyGroupBy(df, []string{"group"}, []expr.Aggregation{
		{Expr: expr.Column{Name: "value"}, Func: expr.ArgMinAgg},
		{Expr: expr.Column{Name: "value"}, Func: expr.ArgMaxAgg},
	})
	if err != nil {
		t.Fatalf("applyGroupBy failed: %v", err)
	}

	argminVal, _ := result.ColByName("ArgMin_value")
	if argminVal.(*series.Float64Series).Value(0) != 1.0 {
		t.Errorf("Expected argmin 1.0, got %f", argminVal.(*series.Float64Series).Value(0))
	}

	argmaxVal, _ := result.ColByName("ArgMax_value")
	if argmaxVal.(*series.Float64Series).Value(0) != 0.0 {
		t.Errorf("Expected argmax 0.0, got %f", argmaxVal.(*series.Float64Series).Value(0))
	}
}

func TestApplyGroupBySumInt(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("group", memory.DefaultAllocator, []string{"a", "a", "b", "b"}, nil))
	df.AddSeries(series.NewInt64Series("value", memory.DefaultAllocator, []int64{1, 2, 3, 4}, nil))

	result, err := applyGroupBy(df, []string{"group"}, []expr.Aggregation{
		{Expr: expr.Column{Name: "value"}, Func: expr.SumAgg},
	})
	if err != nil {
		t.Fatalf("applyGroupBy failed: %v", err)
	}

	sumVal, _ := result.ColByName("Sum_value")
	v0 := sumVal.(*series.Int64Series).Value(0)
	v1 := sumVal.(*series.Int64Series).Value(1)
	if (v0 == 3 && v1 == 7) || (v0 == 7 && v1 == 3) {
		return
	}
	t.Errorf("Expected sums 3 and 7, got %d and %d", v0, v1)
}

func TestApplyGroupByMeanInt(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("group", memory.DefaultAllocator, []string{"a", "a", "b"}, nil))
	df.AddSeries(series.NewInt64Series("value", memory.DefaultAllocator, []int64{1, 3, 5}, nil))

	result, err := applyGroupBy(df, []string{"group"}, []expr.Aggregation{
		{Expr: expr.Column{Name: "value"}, Func: expr.MeanAgg},
	})
	if err != nil {
		t.Fatalf("applyGroupBy failed: %v", err)
	}

	meanVal, _ := result.ColByName("Mean_value")
	v0 := meanVal.(*series.Float64Series).Value(0)
	v1 := meanVal.(*series.Float64Series).Value(1)
	if (v0 == 2.0 && v1 == 5.0) || (v0 == 5.0 && v1 == 2.0) {
		return
	}
	t.Errorf("Expected means 2.0 and 5.0, got %f and %f", v0, v1)
}

func TestApplyGroupByStdInt(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("group", memory.DefaultAllocator, []string{"a", "a"}, nil))
	df.AddSeries(series.NewInt64Series("value", memory.DefaultAllocator, []int64{1, 3}, nil))

	result, err := applyGroupBy(df, []string{"group"}, []expr.Aggregation{
		{Expr: expr.Column{Name: "value"}, Func: expr.StdAgg},
	})
	if err != nil {
		t.Fatalf("applyGroupBy failed: %v", err)
	}

	stdVal, _ := result.ColByName("Std_value")
	if stdVal.(*series.Float64Series).Value(0) < 0.9 || stdVal.(*series.Float64Series).Value(0) > 1.1 {
		t.Errorf("Expected std ~1.0, got %f", stdVal.(*series.Float64Series).Value(0))
	}
}

func TestApplyGroupByVarianceInt(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("group", memory.DefaultAllocator, []string{"a", "a"}, nil))
	df.AddSeries(series.NewInt64Series("value", memory.DefaultAllocator, []int64{1, 3}, nil))

	result, err := applyGroupBy(df, []string{"group"}, []expr.Aggregation{
		{Expr: expr.Column{Name: "value"}, Func: expr.VarAgg},
	})
	if err != nil {
		t.Fatalf("applyGroupBy failed: %v", err)
	}

	varVal, _ := result.ColByName("Var_value")
	if varVal.(*series.Float64Series).Value(0) != 1.0 {
		t.Errorf("Expected variance 1.0, got %f", varVal.(*series.Float64Series).Value(0))
	}
}

func TestApplyGroupByMedianInt(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("group", memory.DefaultAllocator, []string{"a", "a", "a"}, nil))
	df.AddSeries(series.NewInt64Series("value", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	result, err := applyGroupBy(df, []string{"group"}, []expr.Aggregation{
		{Expr: expr.Column{Name: "value"}, Func: expr.MedianAgg},
	})
	if err != nil {
		t.Fatalf("applyGroupBy failed: %v", err)
	}

	medianVal, _ := result.ColByName("Median_value")
	if medianVal.(*series.Float64Series).Value(0) != 2.0 {
		t.Errorf("Expected median 2.0, got %f", medianVal.(*series.Float64Series).Value(0))
	}
}

func TestApplyGroupByQuantileInt(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("group", memory.DefaultAllocator, []string{"a", "a", "a", "a"}, nil))
	df.AddSeries(series.NewInt64Series("value", memory.DefaultAllocator, []int64{1, 2, 3, 4}, nil))

	result, err := applyGroupBy(df, []string{"group"}, []expr.Aggregation{
		{Expr: expr.Column{Name: "value"}, Func: expr.QuantileAgg, Param: 0.5},
	})
	if err != nil {
		t.Fatalf("applyGroupBy failed: %v", err)
	}

	quantileVal, _ := result.ColByName("Quantile_value")
	if quantileVal.(*series.Float64Series).Value(0) != 2.5 {
		t.Errorf("Expected quantile 0.5 to be 2.5, got %f", quantileVal.(*series.Float64Series).Value(0))
	}
}

func TestApplyGroupByEmptyIndices(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("group", memory.DefaultAllocator, []string{"a"}, nil))
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.0}, nil))

	result, err := applyGroupBy(df, []string{"group"}, []expr.Aggregation{
		{Expr: expr.Column{Name: "value"}, Func: expr.SumAgg},
	})
	if err != nil {
		t.Fatalf("applyGroupBy failed: %v", err)
	}

	sumVal, _ := result.ColByName("Sum_value")
	if sumVal.(*series.Float64Series).Value(0) != 1.0 {
		t.Errorf("Expected sum 1.0, got %f", sumVal.(*series.Float64Series).Value(0))
	}
}

func TestApplyGroupByMultipleKeysError(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("group", memory.DefaultAllocator, []string{"a", "a"}, nil))
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.0, 2.0}, nil))

	_, err := applyGroupBy(df, []string{"group", "other"}, []expr.Aggregation{
		{Expr: expr.Column{Name: "value"}, Func: expr.SumAgg},
	})
	if err == nil {
		t.Error("Expected error for multiple keys")
	}
}

func TestApplyGroupByInvalidKeyError(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("group", memory.DefaultAllocator, []string{"a", "a"}, nil))
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.0, 2.0}, nil))

	_, err := applyGroupBy(df, []string{"nonexistent"}, []expr.Aggregation{
		{Expr: expr.Column{Name: "value"}, Func: expr.SumAgg},
	})
	if err == nil {
		t.Error("Expected error for invalid key")
	}
}

func TestApplyGroupByNonStringKeyError(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("group", memory.DefaultAllocator, []int64{1, 1}, nil))
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.0, 2.0}, nil))

	_, err := applyGroupBy(df, []string{"group"}, []expr.Aggregation{
		{Expr: expr.Column{Name: "value"}, Func: expr.SumAgg},
	})
	if err == nil {
		t.Error("Expected error for non-string key")
	}
}

func TestCalculateAggFloatEmptyIndices(t *testing.T) {
	result := calculateAggFloat(nil, []int{}, expr.SumAgg, 0)
	if result != 0 {
		t.Errorf("Expected 0 for empty indices, got %f", result)
	}
}

func TestCalculateAggIntEmptyIndices(t *testing.T) {
	result := calculateAggInt(nil, []int{}, expr.SumAgg)
	if result != 0 {
		t.Errorf("Expected 0 for empty indices, got %d", result)
	}
}

func TestCalculateAggIntToFloatEmptyIndices(t *testing.T) {
	result := calculateAggIntToFloat(nil, []int{}, expr.MeanAgg, 0)
	if result != 0 {
		t.Errorf("Expected 0 for empty indices, got %f", result)
	}
}

func TestPopVarianceFloat(t *testing.T) {
	col := series.NewFloat64Series("test", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0, 4.0, 5.0}, nil)
	result := popVarianceFloat(col, []int{0, 1, 2, 3, 4})
	if result != 2.0 {
		t.Errorf("Expected variance 2.0, got %f", result)
	}
}

func TestPopVarianceFloatEmpty(t *testing.T) {
	col := series.NewFloat64Series("test", memory.DefaultAllocator, []float64{1.0}, nil)
	result := popVarianceFloat(col, []int{})
	if result != 0 {
		t.Errorf("Expected 0 for empty indices, got %f", result)
	}
}

func TestPopVarianceInt(t *testing.T) {
	col := series.NewInt64Series("test", memory.DefaultAllocator, []int64{1, 2, 3, 4, 5}, nil)
	result := popVarianceInt(col, []int{0, 1, 2, 3, 4})
	if result != 2.0 {
		t.Errorf("Expected variance 2.0, got %f", result)
	}
}

func TestPopVarianceIntEmpty(t *testing.T) {
	col := series.NewInt64Series("test", memory.DefaultAllocator, []int64{1}, nil)
	result := popVarianceInt(col, []int{})
	if result != 0 {
		t.Errorf("Expected 0 for empty indices, got %f", result)
	}
}

func TestQuantileFloat(t *testing.T) {
	col := series.NewFloat64Series("test", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0, 4.0}, nil)
	result := quantileFloat(col, []int{0, 1, 2, 3}, 0.5)
	if result != 2.5 {
		t.Errorf("Expected 2.5, got %f", result)
	}
}

func TestQuantileFloatEmpty(t *testing.T) {
	col := series.NewFloat64Series("test", memory.DefaultAllocator, []float64{1.0}, nil)
	result := quantileFloat(col, []int{}, 0.5)
	if result != 0 {
		t.Errorf("Expected 0 for empty indices, got %f", result)
	}
}

func TestQuantileFloatEdge(t *testing.T) {
	col := series.NewFloat64Series("test", memory.DefaultAllocator, []float64{1.0}, nil)
	result := quantileFloat(col, []int{0}, 0.5)
	if result != 1.0 {
		t.Errorf("Expected 1.0, got %f", result)
	}
}

func TestQuantileInt(t *testing.T) {
	col := series.NewInt64Series("test", memory.DefaultAllocator, []int64{1, 2, 3, 4}, nil)
	result := quantileInt(col, []int{0, 1, 2, 3}, 0.5)
	if result != 2.5 {
		t.Errorf("Expected 2.5, got %f", result)
	}
}

func TestQuantileIntEmpty(t *testing.T) {
	col := series.NewInt64Series("test", memory.DefaultAllocator, []int64{1}, nil)
	result := quantileInt(col, []int{}, 0.5)
	if result != 0 {
		t.Errorf("Expected 0 for empty indices, got %f", result)
	}
}

func TestNUniqueFloat(t *testing.T) {
	col := series.NewFloat64Series("test", memory.DefaultAllocator, []float64{1.0, 1.0, 2.0, 2.0, 3.0}, nil)
	result := nuniqueFloat(col, []int{0, 1, 2, 3, 4})
	if result != 3.0 {
		t.Errorf("Expected 3.0, got %f", result)
	}
}

func TestNUniqueFloatEmpty(t *testing.T) {
	col := series.NewFloat64Series("test", memory.DefaultAllocator, []float64{1.0}, nil)
	result := nuniqueFloat(col, []int{})
	if result != 0 {
		t.Errorf("Expected 0 for empty indices, got %f", result)
	}
}

func TestFirstFloat(t *testing.T) {
	col := series.NewFloat64Series("test", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0}, nil)
	result := firstFloat(col, []int{0, 1, 2})
	if result != 1.0 {
		t.Errorf("Expected 1.0, got %f", result)
	}
}

func TestFirstFloatEmpty(t *testing.T) {
	col := series.NewFloat64Series("test", memory.DefaultAllocator, []float64{1.0}, nil)
	result := firstFloat(col, []int{})
	if result != 0 {
		t.Errorf("Expected 0 for empty indices, got %f", result)
	}
}

func TestLastFloat(t *testing.T) {
	col := series.NewFloat64Series("test", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0}, nil)
	result := lastFloat(col, []int{0, 1, 2})
	if result != 3.0 {
		t.Errorf("Expected 3.0, got %f", result)
	}
}

func TestLastFloatEmpty(t *testing.T) {
	col := series.NewFloat64Series("test", memory.DefaultAllocator, []float64{1.0}, nil)
	result := lastFloat(col, []int{})
	if result != 0 {
		t.Errorf("Expected 0 for empty indices, got %f", result)
	}
}

func TestArgMinFloat(t *testing.T) {
	col := series.NewFloat64Series("test", memory.DefaultAllocator, []float64{5.0, 1.0, 3.0}, nil)
	result := argminFloat(col, []int{0, 1, 2})
	if result != 1.0 {
		t.Errorf("Expected 1.0, got %f", result)
	}
}

func TestArgMinFloatEmpty(t *testing.T) {
	col := series.NewFloat64Series("test", memory.DefaultAllocator, []float64{1.0}, nil)
	result := argminFloat(col, []int{})
	if result != 0 {
		t.Errorf("Expected 0 for empty indices, got %f", result)
	}
}

func TestArgMaxFloat(t *testing.T) {
	col := series.NewFloat64Series("test", memory.DefaultAllocator, []float64{1.0, 5.0, 3.0}, nil)
	result := argmaxFloat(col, []int{0, 1, 2})
	if result != 1.0 {
		t.Errorf("Expected 1.0, got %f", result)
	}
}

func TestArgMaxFloatEmpty(t *testing.T) {
	col := series.NewFloat64Series("test", memory.DefaultAllocator, []float64{1.0}, nil)
	result := argmaxFloat(col, []int{})
	if result != 0 {
		t.Errorf("Expected 0 for empty indices, got %f", result)
	}
}

func TestSqrt(t *testing.T) {
	result := sqrt(4.0)
	if result < 1.99 || result > 2.01 {
		t.Errorf("Expected ~2.0, got %f", result)
	}
}

func TestSqrtZero(t *testing.T) {
	result := sqrt(0.0)
	if result > 0.001 {
		t.Errorf("Expected ~0, got %f", result)
	}
}

func TestCalculateAggIntMinMax(t *testing.T) {
	col := series.NewInt64Series("test", memory.DefaultAllocator, []int64{5, 1, 3}, nil)

	minResult := calculateAggInt(col, []int{0, 1, 2}, expr.MinAgg)
	if minResult != 1 {
		t.Errorf("Expected min 1, got %d", minResult)
	}

	maxResult := calculateAggInt(col, []int{0, 1, 2}, expr.MaxAgg)
	if maxResult != 5 {
		t.Errorf("Expected max 5, got %d", maxResult)
	}
}

func TestCalculateAggIntCount(t *testing.T) {
	col := series.NewInt64Series("test", memory.DefaultAllocator, []int64{1, 2, 3}, nil)
	result := calculateAggInt(col, []int{0, 1, 2}, expr.CountAgg)
	if result != 3 {
		t.Errorf("Expected count 3, got %d", result)
	}
}

func TestCalculateAggIntSum(t *testing.T) {
	col := series.NewInt64Series("test", memory.DefaultAllocator, []int64{1, 2, 3}, nil)
	result := calculateAggInt(col, []int{0, 1, 2}, expr.SumAgg)
	if result != 6 {
		t.Errorf("Expected sum 6, got %d", result)
	}
}

func TestCalculateAggIntDefault(t *testing.T) {
	col := series.NewInt64Series("test", memory.DefaultAllocator, []int64{1, 2, 3}, nil)
	result := calculateAggInt(col, []int{0, 1, 2}, expr.MeanAgg)
	if result != 0 {
		t.Errorf("Expected 0 for default case, got %d", result)
	}
}

func TestCalculateAggIntToFloatDefault(t *testing.T) {
	col := series.NewInt64Series("test", memory.DefaultAllocator, []int64{1, 2, 3}, nil)
	result := calculateAggIntToFloat(col, []int{0, 1, 2}, expr.SumAgg, 0)
	if result != 0 {
		t.Errorf("Expected 0 for default case, got %f", result)
	}
}

func TestCalculateAggFloatDefault(t *testing.T) {
	col := series.NewFloat64Series("test", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0}, nil)
	result := calculateAggFloat(col, []int{0, 1, 2}, expr.AggFunc("unknown"), 0)
	if result != 0 {
		t.Errorf("Expected 0 for default case, got %f", result)
	}
}

func TestQuantileIntEdge(t *testing.T) {
	col := series.NewInt64Series("test", memory.DefaultAllocator, []int64{1}, nil)
	result := quantileInt(col, []int{0}, 0.5)
	if result != 1.0 {
		t.Errorf("Expected 1.0, got %f", result)
	}
}

func TestApplyGroupByMultipleAggregations(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("group", memory.DefaultAllocator, []string{"a", "a", "b"}, nil))
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0}, nil))

	result, err := applyGroupBy(df, []string{"group"}, []expr.Aggregation{
		{Expr: expr.Column{Name: "value"}, Func: expr.SumAgg},
		{Expr: expr.Column{Name: "value"}, Func: expr.MeanAgg},
	})
	if err != nil {
		t.Fatalf("applyGroupBy failed: %v", err)
	}

	if result.NumCols() != 3 {
		t.Errorf("Expected 3 columns, got %d", result.NumCols())
	}
}

func TestApplyGroupByHead(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("group", memory.DefaultAllocator, []string{"a", "a", "a", "b", "b", "b"}, nil))
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0}, nil))

	result, err := applyGroupByHead(df, []string{"group"}, 2)
	if err != nil {
		t.Fatalf("applyGroupByHead failed: %v", err)
	}

	if result.NumRows() != 4 {
		t.Errorf("Expected 4 rows (2 per group), got %d", result.NumRows())
	}

	groupCol, _ := result.ColByName("group")
	groups := groupCol.(*series.StringSeries)
	if groups.Value(0) != "a" || groups.Value(1) != "a" {
		t.Errorf("Expected first 2 rows to be group 'a', got %s, %s", groups.Value(0), groups.Value(1))
	}
}

func TestApplyGroupByTail(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("group", memory.DefaultAllocator, []string{"a", "a", "a", "b", "b", "b"}, nil))
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0}, nil))

	result, err := applyGroupByTail(df, []string{"group"}, 2)
	if err != nil {
		t.Fatalf("applyGroupByTail failed: %v", err)
	}

	if result.NumRows() != 4 {
		t.Errorf("Expected 4 rows (2 per group), got %d", result.NumRows())
	}

	groupCol, _ := result.ColByName("group")
	groups := groupCol.(*series.StringSeries)
	if groups.Value(2) != "b" || groups.Value(3) != "b" {
		t.Errorf("Expected last 2 rows to be group 'b', got %s, %s", groups.Value(2), groups.Value(3))
	}
}

func TestApplyGroupByGroups(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("group", memory.DefaultAllocator, []string{"a", "a", "a", "b", "b"}, nil))
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0, 4.0, 5.0}, nil))

	result, err := applyGroupByGroups(df, []string{"group"})
	if err != nil {
		t.Fatalf("applyGroupByGroups failed: %v", err)
	}

	if result.NumRows() != 2 {
		t.Errorf("Expected 2 groups, got %d", result.NumRows())
	}

	groupCol, _ := result.ColByName("group")
	groups := groupCol.(*series.StringSeries)
	if groups.Value(0) != "a" || groups.Value(1) != "b" {
		t.Errorf("Expected groups 'a' and 'b', got %s, %s", groups.Value(0), groups.Value(1))
	}

	countCol, _ := result.ColByName("__row_count")
	counts := countCol.(*series.Int64Series)
	if counts.Value(0) != 3 || counts.Value(1) != 2 {
		t.Errorf("Expected counts 3 and 2, got %d and %d", counts.Value(0), counts.Value(1))
	}
}

func TestApplyGroupByHeadLessThanGroupSize(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("group", memory.DefaultAllocator, []string{"a", "a", "b"}, nil))
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0}, nil))

	result, err := applyGroupByHead(df, []string{"group"}, 5)
	if err != nil {
		t.Fatalf("applyGroupByHead failed: %v", err)
	}

	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows (all rows), got %d", result.NumRows())
	}
}
