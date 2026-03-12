package engine

import (
	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func applyCoalesce(df *dataframe.DataFrame, e expr.CoalesceExpr, alloc memory.Allocator) (series.Series, error) {
	colName := e.Exprs[0].(expr.Column).Name
	col, err := df.ColByName(colName)
	if err != nil {
		return nil, err
	}

	otherCols := make([]series.Series, len(e.Exprs)-1)
	for i := 1; i < len(e.Exprs); i++ {
		otherCols[i-1], _ = df.ColByName(e.Exprs[i].(expr.Column).Name)
	}

	switch typedCol := col.(type) {
	case *series.Int64Series:
		var resultVals []int64
		var valid []bool
		for j := 0; j < typedCol.Len(); j++ {
			if !typedCol.IsNull(j) {
				resultVals = append(resultVals, typedCol.Value(j))
				valid = append(valid, true)
			} else {
				found := false
				for _, otherCol := range otherCols {
					if otherColInt, ok := otherCol.(*series.Int64Series); ok {
						if !otherColInt.IsNull(j) {
							resultVals = append(resultVals, otherColInt.Value(j))
							valid = append(valid, true)
							found = true
							break
						}
					}
				}
				if !found {
					resultVals = append(resultVals, 0)
					valid = append(valid, false)
				}
			}
		}
		return series.NewInt64Series(colName, alloc, resultVals, valid), nil

	case *series.Float64Series:
		var resultVals []float64
		var valid []bool
		for j := 0; j < typedCol.Len(); j++ {
			if !typedCol.IsNull(j) {
				resultVals = append(resultVals, typedCol.Value(j))
				valid = append(valid, true)
			} else {
				found := false
				for _, otherCol := range otherCols {
					if otherColFloat, ok := otherCol.(*series.Float64Series); ok {
						if !otherColFloat.IsNull(j) {
							resultVals = append(resultVals, otherColFloat.Value(j))
							valid = append(valid, true)
							found = true
							break
						}
					}
				}
				if !found {
					resultVals = append(resultVals, 0.0)
					valid = append(valid, false)
				}
			}
		}
		return series.NewFloat64Series(colName, alloc, resultVals, valid), nil

	case *series.StringSeries:
		var resultVals []string
		var valid []bool
		for j := 0; j < typedCol.Len(); j++ {
			if !typedCol.IsNull(j) {
				resultVals = append(resultVals, typedCol.Value(j))
				valid = append(valid, true)
			} else {
				found := false
				for _, otherCol := range otherCols {
					if otherColStr, ok := otherCol.(*series.StringSeries); ok {
						if !otherColStr.IsNull(j) {
							resultVals = append(resultVals, otherColStr.Value(j))
							valid = append(valid, true)
							found = true
							break
						}
					}
				}
				if !found {
					resultVals = append(resultVals, "")
					valid = append(valid, false)
				}
			}
		}
		return series.NewStringSeries(colName, alloc, resultVals, valid), nil

	case *series.BooleanSeries:
		var resultVals []bool
		var valid []bool
		for j := 0; j < typedCol.Len(); j++ {
			if !typedCol.IsNull(j) {
				resultVals = append(resultVals, typedCol.Value(j))
				valid = append(valid, true)
			} else {
				found := false
				for _, otherCol := range otherCols {
					if otherColBool, ok := otherCol.(*series.BooleanSeries); ok {
						if !otherColBool.IsNull(j) {
							resultVals = append(resultVals, otherColBool.Value(j))
							valid = append(valid, true)
							found = true
							break
						}
					}
				}
				if !found {
					resultVals = append(resultVals, false)
					valid = append(valid, false)
				}
			}
		}
		return series.NewBooleanSeries(colName, alloc, resultVals, valid), nil
	}
	return nil, nil
}
