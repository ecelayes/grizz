package engine

import (
	"math"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func applyCumSum(df *dataframe.DataFrame, e expr.CumSumExpr, alloc memory.Allocator) (series.Series, error) {
	col, err := getColumnFromExpr(df, e.Expr)
	if err != nil {
		return nil, err
	}

	n := col.Len()
	result := make([]float64, n)
	valid := make([]bool, n)

	var sum float64
	for i := 0; i < n; i++ {
		if !col.IsNull(i) {
			sum += toFloat64(col, i)
			result[i] = sum
			valid[i] = true
		}
	}

	return series.NewFloat64Series(col.Name(), alloc, result, valid), nil
}

func applyCumProd(df *dataframe.DataFrame, e expr.CumProdExpr, alloc memory.Allocator) (series.Series, error) {
	col, err := getColumnFromExpr(df, e.Expr)
	if err != nil {
		return nil, err
	}

	n := col.Len()
	result := make([]float64, n)
	valid := make([]bool, n)

	var prod float64 = 1
	for i := 0; i < n; i++ {
		if !col.IsNull(i) {
			prod *= toFloat64(col, i)
			result[i] = prod
			valid[i] = true
		}
	}

	return series.NewFloat64Series(col.Name(), alloc, result, valid), nil
}

func applyCumMin(df *dataframe.DataFrame, e expr.CumMinExpr, alloc memory.Allocator) (series.Series, error) {
	col, err := getColumnFromExpr(df, e.Expr)
	if err != nil {
		return nil, err
	}

	n := col.Len()
	result := make([]float64, n)
	valid := make([]bool, n)

	minVal := math.Inf(1)
	for i := 0; i < n; i++ {
		if !col.IsNull(i) {
			val := toFloat64(col, i)
			if val < minVal {
				minVal = val
			}
			result[i] = minVal
			valid[i] = true
		}
	}

	return series.NewFloat64Series(col.Name(), alloc, result, valid), nil
}

func applyCumMax(df *dataframe.DataFrame, e expr.CumMaxExpr, alloc memory.Allocator) (series.Series, error) {
	col, err := getColumnFromExpr(df, e.Expr)
	if err != nil {
		return nil, err
	}

	n := col.Len()
	result := make([]float64, n)
	valid := make([]bool, n)

	maxVal := math.Inf(-1)
	for i := 0; i < n; i++ {
		if !col.IsNull(i) {
			val := toFloat64(col, i)
			if val > maxVal {
				maxVal = val
			}
			result[i] = maxVal
			valid[i] = true
		}
	}

	return series.NewFloat64Series(col.Name(), alloc, result, valid), nil
}
