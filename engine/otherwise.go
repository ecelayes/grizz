package engine

import (
	"fmt"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func applyOtherwise(df *dataframe.DataFrame, e expr.OtherwiseExpr, alloc memory.Allocator) (series.Series, error) {
	return applyThenImpl(df, e.ThenExpr, e.Otherwise, alloc)
}

func applyThen(df *dataframe.DataFrame, e expr.ThenExpr, alloc memory.Allocator) (series.Series, error) {
	return applyThenImpl(df, e, nil, alloc)
}

func applyThenImpl(df *dataframe.DataFrame, thenExpr expr.ThenExpr, otherwiseExpr expr.Expr, alloc memory.Allocator) (series.Series, error) {
	mask, err := evaluateCondition(df, thenExpr.WhenExpr.Condition)
	if err != nil {
		return nil, err
	}
	thenVal := thenExpr.Value.(expr.Literal).Value

	col, err := df.Col(0)
	if err != nil {
		return nil, err
	}

	switch typedCol := col.(type) {
	case *series.Int64Series:
		var resultVals []int64
		var valid []bool
		var thenInt int64
		switch v := thenVal.(type) {
		case int:
			thenInt = int64(v)
		case int64:
			thenInt = v
		}
		for j := 0; j < typedCol.Len(); j++ {
			if mask[j] {
				resultVals = append(resultVals, thenInt)
			} else if otherwiseExpr != nil {
				elseVal := otherwiseExpr.(expr.Literal).Value
				switch v := elseVal.(type) {
				case int:
					resultVals = append(resultVals, int64(v))
				case int64:
					resultVals = append(resultVals, v)
				default:
					resultVals = append(resultVals, 0)
				}
			} else {
				resultVals = append(resultVals, 0)
			}
			valid = append(valid, mask[j] || otherwiseExpr != nil)
		}
		return series.NewInt64Series(typedCol.Name(), alloc, resultVals, valid), nil

	case *series.Float64Series:
		var resultVals []float64
		var valid []bool
		var thenFloat float64
		switch v := thenVal.(type) {
		case float64:
			thenFloat = v
		case int:
			thenFloat = float64(v)
		case int64:
			thenFloat = float64(v)
		}
		for j := 0; j < typedCol.Len(); j++ {
			if mask[j] {
				resultVals = append(resultVals, thenFloat)
			} else if otherwiseExpr != nil {
				elseVal := otherwiseExpr.(expr.Literal).Value
				switch v := elseVal.(type) {
				case float64:
					resultVals = append(resultVals, v)
				case int:
					resultVals = append(resultVals, float64(v))
				case int64:
					resultVals = append(resultVals, float64(v))
				default:
					resultVals = append(resultVals, 0)
				}
			} else {
				resultVals = append(resultVals, 0)
			}
			valid = append(valid, mask[j] || otherwiseExpr != nil)
		}
		return series.NewFloat64Series(typedCol.Name(), alloc, resultVals, valid), nil

	case *series.StringSeries:
		var resultVals []string
		var valid []bool
		thenStr := fmt.Sprintf("%v", thenVal)
		for j := 0; j < typedCol.Len(); j++ {
			if mask[j] {
				resultVals = append(resultVals, thenStr)
			} else if otherwiseExpr != nil {
				elseStr := fmt.Sprintf("%v", otherwiseExpr.(expr.Literal).Value)
				resultVals = append(resultVals, elseStr)
			} else {
				resultVals = append(resultVals, "")
			}
			valid = append(valid, mask[j] || otherwiseExpr != nil)
		}
		return series.NewStringSeries(typedCol.Name(), alloc, resultVals, valid), nil

	case *series.BooleanSeries:
		var resultVals []bool
		var valid []bool
		var thenBool bool
		if v, ok := thenVal.(bool); ok {
			thenBool = v
		}
		for j := 0; j < typedCol.Len(); j++ {
			if mask[j] {
				resultVals = append(resultVals, thenBool)
			} else if otherwiseExpr != nil {
				if v, ok := otherwiseExpr.(expr.Literal).Value.(bool); ok {
					resultVals = append(resultVals, v)
				} else {
					resultVals = append(resultVals, false)
				}
			} else {
				resultVals = append(resultVals, false)
			}
			valid = append(valid, mask[j] || otherwiseExpr != nil)
		}
		return series.NewBooleanSeries(typedCol.Name(), alloc, resultVals, valid), nil
	}
	return nil, nil
}
