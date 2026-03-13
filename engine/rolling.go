package engine

import (
	"math"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func applyRollingSum(df *dataframe.DataFrame, re expr.RollingSumExpr, alloc memory.Allocator) (series.Series, error) {
	col, err := getColumnFromExpr(df, re.Expr)
	if err != nil {
		return nil, err
	}

	result, err := rollingSum(col, re.WindowSize, re.MinPeriods, alloc)
	if err != nil {
		return nil, err
	}
	result.SetName(col.Name())
	return result, nil
}

func applyRollingMean(df *dataframe.DataFrame, re expr.RollingMeanExpr, alloc memory.Allocator) (series.Series, error) {
	col, err := getColumnFromExpr(df, re.Expr)
	if err != nil {
		return nil, err
	}

	result, err := rollingMean(col, re.WindowSize, re.MinPeriods, alloc)
	if err != nil {
		return nil, err
	}
	result.SetName(col.Name())
	return result, nil
}

func applyRollingMin(df *dataframe.DataFrame, re expr.RollingMinExpr, alloc memory.Allocator) (series.Series, error) {
	col, err := getColumnFromExpr(df, re.Expr)
	if err != nil {
		return nil, err
	}

	result, err := rollingMin(col, re.WindowSize, re.MinPeriods, alloc)
	if err != nil {
		return nil, err
	}
	result.SetName(col.Name())
	return result, nil
}

func applyRollingMax(df *dataframe.DataFrame, re expr.RollingMaxExpr, alloc memory.Allocator) (series.Series, error) {
	col, err := getColumnFromExpr(df, re.Expr)
	if err != nil {
		return nil, err
	}

	result, err := rollingMax(col, re.WindowSize, re.MinPeriods, alloc)
	if err != nil {
		return nil, err
	}
	result.SetName(col.Name())
	return result, nil
}

func getColumnFromExpr(df *dataframe.DataFrame, e expr.Expr) (series.Series, error) {
	switch c := e.(type) {
	case expr.Column:
		return df.ColByName(c.Name)
	default:
		return nil, nil
	}
}

func rollingSum(col series.Series, windowSize int, minPeriods int, alloc memory.Allocator) (series.Series, error) {
	n := col.Len()
	result := make([]float64, n)
	valid := make([]bool, n)

	for i := 0; i < n; i++ {
		start := i - windowSize + 1
		if start < 0 {
			start = 0
		}

		var sum float64
		count := 0
		for j := start; j <= i; j++ {
			if !col.IsNull(j) {
				sum += toFloat64(col, j)
				count++
			}
		}

		if count >= minPeriods {
			result[i] = sum
			valid[i] = true
		}
	}

	return series.NewFloat64Series(col.Name(), alloc, result, valid), nil
}

func rollingMean(col series.Series, windowSize int, minPeriods int, alloc memory.Allocator) (series.Series, error) {
	n := col.Len()
	result := make([]float64, n)
	valid := make([]bool, n)

	for i := 0; i < n; i++ {
		start := i - windowSize + 1
		if start < 0 {
			start = 0
		}

		var sum float64
		count := 0
		for j := start; j <= i; j++ {
			if !col.IsNull(j) {
				sum += toFloat64(col, j)
				count++
			}
		}

		if count >= minPeriods {
			result[i] = sum / float64(count)
			valid[i] = true
		}
	}

	return series.NewFloat64Series(col.Name(), alloc, result, valid), nil
}

func rollingMin(col series.Series, windowSize int, minPeriods int, alloc memory.Allocator) (series.Series, error) {
	n := col.Len()
	result := make([]float64, n)
	valid := make([]bool, n)

	for i := 0; i < n; i++ {
		start := i - windowSize + 1
		if start < 0 {
			start = 0
		}

		minVal := math.Inf(1)
		count := 0
		for j := start; j <= i; j++ {
			if !col.IsNull(j) {
				val := toFloat64(col, j)
				if val < minVal {
					minVal = val
				}
				count++
			}
		}

		if count >= minPeriods {
			result[i] = minVal
			valid[i] = true
		}
	}

	return series.NewFloat64Series(col.Name(), alloc, result, valid), nil
}

func rollingMax(col series.Series, windowSize int, minPeriods int, alloc memory.Allocator) (series.Series, error) {
	n := col.Len()
	result := make([]float64, n)
	valid := make([]bool, n)

	for i := 0; i < n; i++ {
		start := i - windowSize + 1
		if start < 0 {
			start = 0
		}

		maxVal := math.Inf(-1)
		count := 0
		for j := start; j <= i; j++ {
			if !col.IsNull(j) {
				val := toFloat64(col, j)
				if val > maxVal {
					maxVal = val
				}
				count++
			}
		}

		if count >= minPeriods {
			result[i] = maxVal
			valid[i] = true
		}
	}

	return series.NewFloat64Series(col.Name(), alloc, result, valid), nil
}

func toFloat64(col series.Series, i int) float64 {
	switch c := col.(type) {
	case *series.Int64Series:
		return float64(c.Value(i))
	case *series.Float64Series:
		return c.Value(i)
	case *series.UInt64Series:
		return float64(c.Value(i))
	case *series.UInt32Series:
		return float64(c.Value(i))
	case *series.UInt16Series:
		return float64(c.Value(i))
	case *series.UInt8Series:
		return float64(c.Value(i))
	case *series.Int16Series:
		return float64(c.Value(i))
	case *series.Int8Series:
		return float64(c.Value(i))
	default:
		return 0
	}
}
