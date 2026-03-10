package engine

import (
	"errors"
	"sort"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func applyOrderBy(df *dataframe.DataFrame, column string, descending bool) (*dataframe.DataFrame, error) {
	col, err := df.ColByName(column)
	if err != nil {
		return nil, err
	}

	indices := make([]int, df.NumRows())
	for i := range indices {
		indices[i] = i
	}

	var sortErr error
	sort.SliceStable(indices, func(i, j int) bool {
		rowI, rowJ := indices[i], indices[j]

		if col.IsNull(rowI) && col.IsNull(rowJ) {
			return false
		}
		if col.IsNull(rowI) {
			return !descending
		}
		if col.IsNull(rowJ) {
			return descending
		}

		switch c := col.(type) {
		case *series.Int64Series:
			if descending {
				return c.Value(rowI) > c.Value(rowJ)
			}
			return c.Value(rowI) < c.Value(rowJ)
		case *series.Float64Series:
			if descending {
				return c.Value(rowI) > c.Value(rowJ)
			}
			return c.Value(rowI) < c.Value(rowJ)
		case *series.StringSeries:
			if descending {
				return c.Value(rowI) > c.Value(rowJ)
			}
			return c.Value(rowI) < c.Value(rowJ)
		case *series.BooleanSeries:
			valI, valJ := c.Value(rowI), c.Value(rowJ)
			if valI == valJ {
				return false
			}
			if descending {
				return valI && !valJ
			}
			return !valI && valJ
		default:
			sortErr = errors.New("unsupported column type for sorting")
			return false
		}
	})

	if sortErr != nil {
		return nil, sortErr
	}

	return applyIndices(df, indices)
}

func applyIndices(df *dataframe.DataFrame, indices []int) (*dataframe.DataFrame, error) {
	result := dataframe.New()
	alloc := memory.DefaultAllocator

	for i := 0; i < df.NumCols(); i++ {
		col, _ := df.Col(i)

		switch typedCol := col.(type) {
		case *series.StringSeries:
			var copied []string
			for _, idx := range indices {
				copied = append(copied, typedCol.Value(idx))
			}
			result.AddSeries(series.NewStringSeries(typedCol.Name(), alloc, copied, nil))

		case *series.Float64Series:
			var copied []float64
			for _, idx := range indices {
				copied = append(copied, typedCol.Value(idx))
			}
			result.AddSeries(series.NewFloat64Series(typedCol.Name(), alloc, copied, nil))

		case *series.Int64Series:
			var copied []int64
			for _, idx := range indices {
				copied = append(copied, typedCol.Value(idx))
			}
			result.AddSeries(series.NewInt64Series(typedCol.Name(), alloc, copied, nil))

		case *series.BooleanSeries:
			var copied []bool
			for _, idx := range indices {
				copied = append(copied, typedCol.Value(idx))
			}
			result.AddSeries(series.NewBooleanSeries(typedCol.Name(), alloc, copied, nil))
		}
	}

	return result, nil
}
