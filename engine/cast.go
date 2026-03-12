package engine

import (
	"strconv"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func applyCast(df *dataframe.DataFrame, e expr.CastExpr, alloc memory.Allocator) (series.Series, error) {
	col, err := df.ColByName(e.Expr.(expr.Column).Name)
	if err != nil {
		return nil, err
	}
	targetType := e.Dtype.Name()

	switch targetType {
	case "int64":
		switch typedCol := col.(type) {
		case *series.Float64Series:
			var resultVals []int64
			var valid []bool
			for j := 0; j < typedCol.Len(); j++ {
				if typedCol.IsNull(j) {
					resultVals = append(resultVals, 0)
					valid = append(valid, false)
				} else {
					resultVals = append(resultVals, int64(typedCol.Value(j)))
					valid = append(valid, true)
				}
			}
			return series.NewInt64Series(typedCol.Name(), alloc, resultVals, valid), nil
		case *series.StringSeries:
			var resultVals []int64
			var valid []bool
			for j := 0; j < typedCol.Len(); j++ {
				if typedCol.IsNull(j) {
					resultVals = append(resultVals, 0)
					valid = append(valid, false)
				} else {
					val, _ := strconv.ParseInt(typedCol.Value(j), 10, 64)
					resultVals = append(resultVals, val)
					valid = append(valid, true)
				}
			}
			return series.NewInt64Series(typedCol.Name(), alloc, resultVals, valid), nil
		}
	case "float64":
		switch typedCol := col.(type) {
		case *series.Int64Series:
			var resultVals []float64
			var valid []bool
			for j := 0; j < typedCol.Len(); j++ {
				if typedCol.IsNull(j) {
					resultVals = append(resultVals, 0)
					valid = append(valid, false)
				} else {
					resultVals = append(resultVals, float64(typedCol.Value(j)))
					valid = append(valid, true)
				}
			}
			return series.NewFloat64Series(typedCol.Name(), alloc, resultVals, valid), nil
		case *series.StringSeries:
			var resultVals []float64
			var valid []bool
			for j := 0; j < typedCol.Len(); j++ {
				if typedCol.IsNull(j) {
					resultVals = append(resultVals, 0)
					valid = append(valid, false)
				} else {
					val, _ := strconv.ParseFloat(typedCol.Value(j), 64)
					resultVals = append(resultVals, val)
					valid = append(valid, true)
				}
			}
			return series.NewFloat64Series(typedCol.Name(), alloc, resultVals, valid), nil
		}
	case "utf8":
		switch typedCol := col.(type) {
		case *series.Int64Series:
			var resultVals []string
			var valid []bool
			for j := 0; j < typedCol.Len(); j++ {
				if typedCol.IsNull(j) {
					resultVals = append(resultVals, "")
					valid = append(valid, false)
				} else {
					resultVals = append(resultVals, strconv.FormatInt(typedCol.Value(j), 10))
					valid = append(valid, true)
				}
			}
			return series.NewStringSeries(typedCol.Name(), alloc, resultVals, valid), nil
		case *series.Float64Series:
			var resultVals []string
			var valid []bool
			for j := 0; j < typedCol.Len(); j++ {
				if typedCol.IsNull(j) {
					resultVals = append(resultVals, "")
					valid = append(valid, false)
				} else {
					resultVals = append(resultVals, strconv.FormatFloat(typedCol.Value(j), 'f', -1, 64))
					valid = append(valid, true)
				}
			}
			return series.NewStringSeries(typedCol.Name(), alloc, resultVals, valid), nil
		}
	}
	return nil, nil
}
