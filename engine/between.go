package engine

import (
	"errors"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func applyBetween(df *dataframe.DataFrame, be expr.BetweenExpr, alloc memory.Allocator) (series.Series, error) {
	colExpr, ok := be.Expr.(expr.Column)
	if !ok {
		return nil, errors.New("Between only supports column expressions")
	}

	col, err := df.ColByName(colExpr.Name)
	if err != nil {
		return nil, err
	}

	lower := be.Lower.Value
	upper := be.Upper.Value

	if intVal, ok := lower.(int); ok {
		lower = int64(intVal)
	}
	if intVal, ok := upper.(int); ok {
		upper = int64(intVal)
	}

	switch c := col.(type) {
	case *series.Int64Series:
		lowerVal, lowerOk := lower.(int64)
		upperVal, upperOk := upper.(int64)
		if !lowerOk || !upperOk {
			return nil, errors.New("invalid type for between lower/upper bounds")
		}
		result := make([]bool, c.Len())
		for i := 0; i < c.Len(); i++ {
			if c.IsNull(i) {
				result[i] = false
				continue
			}
			val := c.Value(i)
			result[i] = val >= lowerVal && val <= upperVal
		}
		return series.NewBooleanSeries(colExpr.Name, alloc, result, nil), nil

	case *series.Float64Series:
		var lowerVal, upperVal float64
		switch l := lower.(type) {
		case float64:
			lowerVal = l
		case int64:
			lowerVal = float64(l)
		default:
			return nil, errors.New("invalid type for between lower bound")
		}
		switch u := upper.(type) {
		case float64:
			upperVal = u
		case int64:
			upperVal = float64(u)
		default:
			return nil, errors.New("invalid type for between upper bound")
		}
		result := make([]bool, c.Len())
		for i := 0; i < c.Len(); i++ {
			if c.IsNull(i) {
				result[i] = false
				continue
			}
			val := c.Value(i)
			result[i] = val >= lowerVal && val <= upperVal
		}
		return series.NewBooleanSeries(colExpr.Name, alloc, result, nil), nil

	default:
		return nil, errors.New("Between only supports numeric columns")
	}
}
