package engine

import (
	"errors"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func applyProjection(df *dataframe.DataFrame, columns []expr.Expr) (*dataframe.DataFrame, error) {
	result := dataframe.New()
	alloc := memory.DefaultAllocator

	for _, exprCol := range columns {
		colExpr, ok := exprCol.(expr.Column)
		if !ok {
			return nil, errors.New("select only supports column expressions currently")
		}

		originalCol, err := df.ColByName(colExpr.Name)
		if err != nil {
			return nil, err
		}

		switch typedCol := originalCol.(type) {
		case *series.StringSeries:
			var copied []string
			for j := 0; j < typedCol.Len(); j++ {
				copied = append(copied, typedCol.Value(j))
			}
			newCol := series.NewStringSeries(typedCol.Name(), alloc, copied, nil)
			result.AddSeries(newCol)

		case *series.Int64Series:
			var copied []int64
			for j := 0; j < typedCol.Len(); j++ {
				copied = append(copied, typedCol.Value(j))
			}
			result.AddSeries(series.NewInt64Series(typedCol.Name(), alloc, copied, nil))

		case *series.Float64Series:
			var copied []float64
			for j := 0; j < typedCol.Len(); j++ {
				copied = append(copied, typedCol.Value(j))
			}
			newCol := series.NewFloat64Series(typedCol.Name(), alloc, copied, nil)
			result.AddSeries(newCol)

		case *series.BooleanSeries:
			var copied []bool
			for j := 0; j < typedCol.Len(); j++ {
				copied = append(copied, typedCol.Value(j))
			}
			newCol := series.NewBooleanSeries(typedCol.Name(), alloc, copied, nil)
			result.AddSeries(newCol)
		}
	}

	return result, nil
}

func applyProjectionAtScan(df *dataframe.DataFrame, columns []string) *dataframe.DataFrame {
	result := dataframe.New()
	alloc := memory.DefaultAllocator

	for _, colName := range columns {
		originalCol, err := df.ColByName(colName)
		if err != nil {
			continue
		}

		switch typedCol := originalCol.(type) {
		case *series.StringSeries:
			var copied []string
			for j := 0; j < typedCol.Len(); j++ {
				copied = append(copied, typedCol.Value(j))
			}
			newCol := series.NewStringSeries(typedCol.Name(), alloc, copied, nil)
			result.AddSeries(newCol)

		case *series.Int64Series:
			var copied []int64
			for j := 0; j < typedCol.Len(); j++ {
				copied = append(copied, typedCol.Value(j))
			}
			result.AddSeries(series.NewInt64Series(typedCol.Name(), alloc, copied, nil))

		case *series.Float64Series:
			var copied []float64
			for j := 0; j < typedCol.Len(); j++ {
				copied = append(copied, typedCol.Value(j))
			}
			newCol := series.NewFloat64Series(typedCol.Name(), alloc, copied, nil)
			result.AddSeries(newCol)

		case *series.BooleanSeries:
			var copied []bool
			for j := 0; j < typedCol.Len(); j++ {
				copied = append(copied, typedCol.Value(j))
			}
			newCol := series.NewBooleanSeries(typedCol.Name(), alloc, copied, nil)
			result.AddSeries(newCol)
		}
	}

	return result
}
