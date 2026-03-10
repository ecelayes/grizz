package engine

import (
	"math/rand"

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

func applyTail(df *dataframe.DataFrame, n int) (*dataframe.DataFrame, error) {
	if n < 0 {
		n = 0
	}

	numRows := df.NumRows()
	if n > numRows {
		n = numRows
	}

	start := numRows - n
	result := dataframe.New()
	alloc := memory.DefaultAllocator

	for i := 0; i < df.NumCols(); i++ {
		col, _ := df.Col(i)

		switch typedCol := col.(type) {
		case *series.StringSeries:
			var copied []string
			for j := start; j < numRows; j++ {
				copied = append(copied, typedCol.Value(j))
			}
			result.AddSeries(series.NewStringSeries(typedCol.Name(), alloc, copied, nil))

		case *series.Float64Series:
			var copied []float64
			for j := start; j < numRows; j++ {
				copied = append(copied, typedCol.Value(j))
			}
			result.AddSeries(series.NewFloat64Series(typedCol.Name(), alloc, copied, nil))

		case *series.Int64Series:
			var copied []int64
			for j := start; j < numRows; j++ {
				copied = append(copied, typedCol.Value(j))
			}
			result.AddSeries(series.NewInt64Series(typedCol.Name(), alloc, copied, nil))

		case *series.BooleanSeries:
			var copied []bool
			for j := start; j < numRows; j++ {
				copied = append(copied, typedCol.Value(j))
			}
			result.AddSeries(series.NewBooleanSeries(typedCol.Name(), alloc, copied, nil))
		}
	}

	return result, nil
}

func applySample(df *dataframe.DataFrame, n int, frac float64, replace bool) (*dataframe.DataFrame, error) {
	numRows := df.NumRows()

	sampleSize := n
	if n <= 0 && frac > 0 {
		sampleSize = int(float64(numRows) * frac)
	}
	if sampleSize <= 0 {
		sampleSize = numRows
	}

	var indices []int
	if replace {
		for i := 0; i < sampleSize; i++ {
			indices = append(indices, rand.Intn(numRows))
		}
	} else {
		if sampleSize > numRows {
			sampleSize = numRows
		}
		perm := rand.Perm(numRows)
		indices = perm[:sampleSize]
	}

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
