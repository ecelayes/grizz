package dataframe

import (
	"fmt"

	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func (df *DataFrame) Describe() *DataFrame {
	result := New()

	stats := []string{"count", "mean", "std", "min", "25%", "50%", "75%", "max"}
	result.AddSeries(series.NewStringSeries("statistic", memory.DefaultAllocator, stats, nil))

	for i := 0; i < df.NumCols(); i++ {
		col, _ := df.Col(i)
		statValues := calculateStats(col)
		result.AddSeries(statValues)
	}

	return result
}

func calculateStats(col series.Series) series.Series {
	switch c := col.(type) {
	case *series.Int64Series:
		return int64Stats(c)
	case *series.Float64Series:
		return float64Stats(c)
	case *series.StringSeries:
		return stringStats(c)
	case *series.BooleanSeries:
		return boolStats(c)
	default:
		return series.NewFloat64Series(col.Name(), memory.DefaultAllocator, []float64{0, 0, 0, 0, 0, 0, 0, 0}, nil)
	}
}

func int64Stats(col *series.Int64Series) series.Series {
	count := float64(col.Count())
	mean := col.Mean()
	std := col.Std()
	min := float64(col.Min())
	q25 := float64(col.Quantile(0.25))
	q50 := float64(col.Quantile(0.50))
	q75 := float64(col.Quantile(0.75))
	max := float64(col.Max())

	return series.NewFloat64Series(col.Name(), memory.DefaultAllocator,
		[]float64{count, mean, std, min, q25, q50, q75, max}, nil)
}

func float64Stats(col *series.Float64Series) series.Series {
	count := float64(col.Count())
	mean := col.Mean()
	std := col.Std()
	min := col.Min()
	q25 := col.Quantile(0.25)
	q50 := col.Quantile(0.50)
	q75 := col.Quantile(0.75)
	max := col.Max()

	return series.NewFloat64Series(col.Name(), memory.DefaultAllocator,
		[]float64{count, mean, std, min, q25, q50, q75, max}, nil)
}

func stringStats(col *series.StringSeries) series.Series {
	count := 0
	for i := 0; i < col.Len(); i++ {
		if !col.IsNull(i) {
			count++
		}
	}
	return series.NewFloat64Series(col.Name(), memory.DefaultAllocator,
		[]float64{float64(count), 0, 0, 0, 0, 0, 0, 0}, nil)
}

func boolStats(col *series.BooleanSeries) series.Series {
	trueCount := 0
	validCount := 0
	for i := 0; i < col.Len(); i++ {
		if !col.IsNull(i) {
			validCount++
			if col.Value(i) {
				trueCount++
			}
		}
	}
	return series.NewFloat64Series(col.Name(), memory.DefaultAllocator,
		[]float64{float64(validCount), float64(trueCount) / float64(validCount), 0, 0, 0, 0, 0, float64(trueCount)}, nil)
}

func (df *DataFrame) Schema() map[string]string {
	schema := make(map[string]string)
	for i := 0; i < df.NumCols(); i++ {
		col, _ := df.Col(i)
		schema[col.Name()] = col.Type().Name()
	}
	return schema
}

func (df *DataFrame) Info() string {
	return fmt.Sprintf("DataFrame: %d rows x %d columns\nColumns: %v\nTypes: %v",
		df.NumRows(), df.NumCols(), df.Columns(), df.Dtypes())
}
