package engine

import (
	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func applyOtherwise(df *dataframe.DataFrame, e expr.OtherwiseExpr, alloc memory.Allocator) (series.Series, error) {
	mask, err := evaluateCondition(df, e.ThenExpr.WhenExpr.Condition)
	if err != nil {
		return nil, err
	}
	thenVal := e.ThenExpr.Value.(expr.Literal).Value
	elseVal := e.Otherwise.(expr.Literal).Value

	col, err := df.Col(0)
	if err != nil {
		return nil, err
	}

	switch typedCol := col.(type) {
	case *series.Int64Series:
		var resultVals []int64
		var valid []bool
		var thenInt, elseInt int64
		switch v := thenVal.(type) {
		case int:
			thenInt = int64(v)
		case int64:
			thenInt = v
		}
		switch v := elseVal.(type) {
		case int:
			elseInt = int64(v)
		case int64:
			elseInt = v
		}
		for j := 0; j < typedCol.Len(); j++ {
			if mask[j] {
				resultVals = append(resultVals, thenInt)
			} else {
				resultVals = append(resultVals, elseInt)
			}
			valid = append(valid, true)
		}
		return series.NewInt64Series(typedCol.Name(), alloc, resultVals, valid), nil

	case *series.Float64Series:
		var resultVals []float64
		var valid []bool
		var thenFloat, elseFloat float64
		switch v := thenVal.(type) {
		case float64:
			thenFloat = v
		case int:
			thenFloat = float64(v)
		case int64:
			thenFloat = float64(v)
		}
		switch v := elseVal.(type) {
		case float64:
			elseFloat = v
		case int:
			elseFloat = float64(v)
		case int64:
			elseFloat = float64(v)
		}
		for j := 0; j < typedCol.Len(); j++ {
			if mask[j] {
				resultVals = append(resultVals, thenFloat)
			} else {
				resultVals = append(resultVals, elseFloat)
			}
			valid = append(valid, true)
		}
		return series.NewFloat64Series(typedCol.Name(), alloc, resultVals, valid), nil

	case *series.StringSeries:
		var resultVals []string
		var valid []bool
		thenStr, _ := thenVal.(string)
		elseStr, _ := elseVal.(string)
		for j := 0; j < typedCol.Len(); j++ {
			if mask[j] {
				resultVals = append(resultVals, thenStr)
			} else {
				resultVals = append(resultVals, elseStr)
			}
			valid = append(valid, true)
		}
		return series.NewStringSeries(typedCol.Name(), alloc, resultVals, valid), nil

	case *series.BooleanSeries:
		var resultVals []bool
		var valid []bool
		var thenBool, elseBool bool
		switch v := thenVal.(type) {
		case bool:
			thenBool = v
		}
		switch v := elseVal.(type) {
		case bool:
			elseBool = v
		}
		for j := 0; j < typedCol.Len(); j++ {
			if mask[j] {
				resultVals = append(resultVals, thenBool)
			} else {
				resultVals = append(resultVals, elseBool)
			}
			valid = append(valid, true)
		}
		return series.NewBooleanSeries(typedCol.Name(), alloc, resultVals, valid), nil
	}
	return nil, nil
}
