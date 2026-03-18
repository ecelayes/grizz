package engine

import (
	"math"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func applyEwmMean(df *dataframe.DataFrame, e expr.EwmMeanExpr, alloc memory.Allocator) (series.Series, error) {
	colExpr, ok := e.Expr.(expr.Column)
	if !ok {
		return nil, nil
	}

	col, err := df.ColByName(colExpr.Name)
	if err != nil {
		return nil, err
	}

	alpha := e.Alpha
	if alpha <= 0 || alpha > 1 {
		alpha = 0.5
	}
	minPeriods := e.MinPeriods
	if minPeriods < 1 {
		minPeriods = 1
	}

	switch c := col.(type) {
	case *series.Int64Series:
		return applyEwmMeanInt64(c, alpha, minPeriods, colExpr.Name, alloc)
	case *series.Float64Series:
		return applyEwmMeanFloat64(c, alpha, minPeriods, colExpr.Name, alloc)
	default:
		return nil, nil
	}
}

func applyEwmMeanInt64(s *series.Int64Series, alpha float64, minPeriods int, name string, alloc memory.Allocator) (series.Series, error) {
	length := s.Len()
	result := make([]float64, length)
	valid := make([]bool, length)

	count := 0
	var ewma float64

	for i := 0; i < length; i++ {
		if s.IsNull(i) {
			valid[i] = false
			result[i] = 0
			continue
		}

		count++
		if count == 1 {
			ewma = float64(s.Value(i))
		} else {
			ewma = alpha*float64(s.Value(i)) + (1-alpha)*ewma
		}

		if count >= minPeriods {
			valid[i] = true
			result[i] = ewma
		} else {
			valid[i] = false
			result[i] = 0
		}
	}

	return series.NewFloat64Series(name, alloc, result, valid), nil
}

func applyEwmMeanFloat64(s *series.Float64Series, alpha float64, minPeriods int, name string, alloc memory.Allocator) (series.Series, error) {
	length := s.Len()
	result := make([]float64, length)
	valid := make([]bool, length)

	count := 0
	var ewma float64

	for i := 0; i < length; i++ {
		if s.IsNull(i) {
			valid[i] = false
			result[i] = 0
			continue
		}

		count++
		if count == 1 {
			ewma = s.Value(i)
		} else {
			ewma = alpha*s.Value(i) + (1-alpha)*ewma
		}

		if count >= minPeriods {
			valid[i] = true
			result[i] = ewma
		} else {
			valid[i] = false
			result[i] = math.NaN()
		}
	}

	return series.NewFloat64Series(name, alloc, result, valid), nil
}
