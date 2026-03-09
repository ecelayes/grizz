package engine

import (
	"errors"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func applyJoin(left *dataframe.DataFrame, right *dataframe.DataFrame, on string, how dataframe.JoinType) (*dataframe.DataFrame, error) {
	if how != dataframe.Inner {
		return nil, errors.New("only inner join is currently supported")
	}

	leftKeyCol, err := left.ColByName(on)
	if err != nil {
		return nil, err
	}
	rightKeyCol, err := right.ColByName(on)
	if err != nil {
		return nil, err
	}

	leftStrCol, okLeft := leftKeyCol.(*series.StringSeries)
	rightStrCol, okRight := rightKeyCol.(*series.StringSeries)
	if !okLeft || !okRight {
		return nil, errors.New("join key must be a string column in both dataframes")
	}

	rightMap := make(map[string][]int)
	for i := 0; i < rightStrCol.Len(); i++ {
		if !rightStrCol.IsNull(i) {
			val := rightStrCol.Value(i)
			rightMap[val] = append(rightMap[val], i)
		}
	}

	var leftIndices []int
	var rightIndices []int

	for i := 0; i < leftStrCol.Len(); i++ {
		if leftStrCol.IsNull(i) {
			continue
		}
		val := leftStrCol.Value(i)
		if rIdxs, exists := rightMap[val]; exists {
			for _, rIdx := range rIdxs {
				leftIndices = append(leftIndices, i)
				rightIndices = append(rightIndices, rIdx)
			}
		}
	}

	result := dataframe.New()
	alloc := memory.DefaultAllocator()

	for i := 0; i < left.NumCols(); i++ {
		col, _ := left.Col(i)
		newCol := copySeriesByIndices(col, leftIndices, alloc)
		result.AddSeries(newCol)
	}

	for i := 0; i < right.NumCols(); i++ {
		col, _ := right.Col(i)
		if col.Name() == on {
			continue
		}
		newCol := copySeriesByIndices(col, rightIndices, alloc)
		result.AddSeries(newCol)
	}

	return result, nil
}

func copySeriesByIndices(col series.Series, indices []int, alloc memory.Allocator) series.Series {
	switch typedCol := col.(type) {
	case *series.StringSeries:
		var copied []string
		for _, idx := range indices {
			copied = append(copied, typedCol.Value(idx))
		}
		return series.NewStringSeries(typedCol.Name(), alloc, copied, nil)
	case *series.Float64Series:
		var copied []float64
		for _, idx := range indices {
			copied = append(copied, typedCol.Value(idx))
		}
		return series.NewFloat64Series(typedCol.Name(), alloc, copied, nil)
	case *series.Int64Series:
		var copied []int64
		for _, idx := range indices {
			copied = append(copied, typedCol.Value(idx))
		}
		return series.NewInt64Series(typedCol.Name(), alloc, copied, nil)
	case *series.BooleanSeries:
		var copied []bool
		for _, idx := range indices {
			copied = append(copied, typedCol.Value(idx))
		}
		return series.NewBooleanSeries(typedCol.Name(), alloc, copied, nil)
	}
	return nil
}
