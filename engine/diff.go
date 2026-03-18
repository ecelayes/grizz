package engine

import (
	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func applyDiff(df *dataframe.DataFrame, e expr.DiffExpr, alloc memory.Allocator) (series.Series, error) {
	colExpr, ok := e.Expr.(expr.Column)
	if !ok {
		return nil, nil
	}

	col, err := df.ColByName(colExpr.Name)
	if err != nil {
		return nil, err
	}

	periods := e.Periods
	if periods < 1 {
		periods = 1
	}

	switch c := col.(type) {
	case *series.Int64Series:
		return applyDiffInt64(c, periods, e.Expr.String(), alloc)
	case *series.Float64Series:
		return applyDiffFloat64(c, periods, e.Expr.String(), alloc)
	default:
		return nil, nil
	}
}

func applyDiffInt64(s *series.Int64Series, periods int, name string, alloc memory.Allocator) (series.Series, error) {
	length := s.Len()
	result := make([]int64, length)
	valid := make([]bool, length)

	for i := 0; i < length; i++ {
		if i < periods {
			valid[i] = false
			result[i] = 0
		} else {
			prevIdx := i - periods
			if !s.IsNull(i) && !s.IsNull(prevIdx) {
				valid[i] = true
				result[i] = s.Value(i) - s.Value(prevIdx)
			} else {
				valid[i] = false
				result[i] = 0
			}
		}
	}

	return series.NewInt64Series(name, alloc, result, valid), nil
}

func applyDiffFloat64(s *series.Float64Series, periods int, name string, alloc memory.Allocator) (series.Series, error) {
	length := s.Len()
	result := make([]float64, length)
	valid := make([]bool, length)

	for i := 0; i < length; i++ {
		if i < periods {
			valid[i] = false
			result[i] = 0
		} else {
			prevIdx := i - periods
			if !s.IsNull(i) && !s.IsNull(prevIdx) {
				valid[i] = true
				result[i] = s.Value(i) - s.Value(prevIdx)
			} else {
				valid[i] = false
				result[i] = 0
			}
		}
	}

	return series.NewFloat64Series(name, alloc, result, valid), nil
}

func applyPctChange(df *dataframe.DataFrame, e expr.PctChangeExpr, alloc memory.Allocator) (series.Series, error) {
	colExpr, ok := e.Expr.(expr.Column)
	if !ok {
		return nil, nil
	}

	col, err := df.ColByName(colExpr.Name)
	if err != nil {
		return nil, err
	}

	periods := e.Periods
	if periods < 1 {
		periods = 1
	}

	switch c := col.(type) {
	case *series.Int64Series:
		return applyPctChangeInt64(c, periods, e.Expr.String(), alloc)
	case *series.Float64Series:
		return applyPctChangeFloat64(c, periods, e.Expr.String(), alloc)
	default:
		return nil, nil
	}
}

func applyPctChangeInt64(s *series.Int64Series, periods int, name string, alloc memory.Allocator) (series.Series, error) {
	length := s.Len()
	result := make([]float64, length)
	valid := make([]bool, length)

	for i := 0; i < length; i++ {
		if i < periods {
			valid[i] = false
			result[i] = 0
		} else {
			prevIdx := i - periods
			if !s.IsNull(i) && !s.IsNull(prevIdx) && s.Value(prevIdx) != 0 {
				valid[i] = true
				result[i] = float64(s.Value(i)-s.Value(prevIdx)) / float64(s.Value(prevIdx))
			} else {
				valid[i] = false
				result[i] = 0
			}
		}
	}

	return series.NewFloat64Series(name, alloc, result, valid), nil
}

func applyPctChangeFloat64(s *series.Float64Series, periods int, name string, alloc memory.Allocator) (series.Series, error) {
	length := s.Len()
	result := make([]float64, length)
	valid := make([]bool, length)

	for i := 0; i < length; i++ {
		if i < periods {
			valid[i] = false
			result[i] = 0
		} else {
			prevIdx := i - periods
			if !s.IsNull(i) && !s.IsNull(prevIdx) && s.Value(prevIdx) != 0 {
				valid[i] = true
				result[i] = (s.Value(i) - s.Value(prevIdx)) / s.Value(prevIdx)
			} else {
				valid[i] = false
				result[i] = 0
			}
		}
	}

	return series.NewFloat64Series(name, alloc, result, valid), nil
}
