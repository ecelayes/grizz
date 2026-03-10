package engine

import (
	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func applyLimit(df *dataframe.DataFrame, limit int) (*dataframe.DataFrame, error) {
	if limit < 0 {
		limit = 0
	}

	numRows := df.NumRows()
	if limit > numRows {
		limit = numRows
	}

	result := dataframe.New()
	alloc := memory.DefaultAllocator

	for i := 0; i < df.NumCols(); i++ {
		col, _ := df.Col(i)

		switch typedCol := col.(type) {
		case *series.StringSeries:
			var copied []string
			for j := 0; j < limit; j++ {
				copied = append(copied, typedCol.Value(j))
			}
			result.AddSeries(series.NewStringSeries(typedCol.Name(), alloc, copied, nil))

		case *series.Float64Series:
			var copied []float64
			for j := 0; j < limit; j++ {
				copied = append(copied, typedCol.Value(j))
			}
			result.AddSeries(series.NewFloat64Series(typedCol.Name(), alloc, copied, nil))

		case *series.Int64Series:
			var copied []int64
			for j := 0; j < limit; j++ {
				copied = append(copied, typedCol.Value(j))
			}
			result.AddSeries(series.NewInt64Series(typedCol.Name(), alloc, copied, nil))

		case *series.BooleanSeries:
			var copied []bool
			for j := 0; j < limit; j++ {
				copied = append(copied, typedCol.Value(j))
			}
			result.AddSeries(series.NewBooleanSeries(typedCol.Name(), alloc, copied, nil))
		}
	}

	return result, nil
}
