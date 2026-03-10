package engine

import (
	"errors"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func applyJoin(left *dataframe.DataFrame, right *dataframe.DataFrame, on string, how dataframe.JoinType) (*dataframe.DataFrame, error) {
	switch how {
	case dataframe.Inner:
		return innerJoin(left, right, on)
	case dataframe.Left:
		return leftJoin(left, right, on)
	case dataframe.Right:
		return rightJoin(left, right, on)
	case dataframe.Outer:
		return outerJoin(left, right, on)
	case dataframe.Cross:
		return crossJoin(left, right)
	default:
		return nil, errors.New("unsupported join type")
	}
}

func innerJoin(left *dataframe.DataFrame, right *dataframe.DataFrame, on string) (*dataframe.DataFrame, error) {
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

	return buildJoinResult(left, right, on, leftIndices, rightIndices)
}

func leftJoin(left *dataframe.DataFrame, right *dataframe.DataFrame, on string) (*dataframe.DataFrame, error) {
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
			leftIndices = append(leftIndices, i)
			rightIndices = append(rightIndices, -1)
			continue
		}
		val := leftStrCol.Value(i)
		if rIdxs, exists := rightMap[val]; exists {
			for _, rIdx := range rIdxs {
				leftIndices = append(leftIndices, i)
				rightIndices = append(rightIndices, rIdx)
			}
		} else {
			leftIndices = append(leftIndices, i)
			rightIndices = append(rightIndices, -1)
		}
	}

	return buildJoinResultWithNulls(left, right, on, leftIndices, rightIndices)
}

func rightJoin(left *dataframe.DataFrame, right *dataframe.DataFrame, on string) (*dataframe.DataFrame, error) {
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

	leftMap := make(map[string][]int)
	for i := 0; i < leftStrCol.Len(); i++ {
		if !leftStrCol.IsNull(i) {
			val := leftStrCol.Value(i)
			leftMap[val] = append(leftMap[val], i)
		}
	}

	var leftIndices []int
	var rightIndices []int

	for i := 0; i < rightStrCol.Len(); i++ {
		if rightStrCol.IsNull(i) {
			leftIndices = append(leftIndices, -1)
			rightIndices = append(rightIndices, i)
			continue
		}
		val := rightStrCol.Value(i)
		if lIdxs, exists := leftMap[val]; exists {
			for _, lIdx := range lIdxs {
				leftIndices = append(leftIndices, lIdx)
				rightIndices = append(rightIndices, i)
			}
		} else {
			leftIndices = append(leftIndices, -1)
			rightIndices = append(rightIndices, i)
		}
	}

	return buildJoinResultWithNulls(left, right, on, leftIndices, rightIndices)
}

func outerJoin(left *dataframe.DataFrame, right *dataframe.DataFrame, on string) (*dataframe.DataFrame, error) {
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

	leftMap := make(map[string][]int)
	for i := 0; i < leftStrCol.Len(); i++ {
		if !leftStrCol.IsNull(i) {
			val := leftStrCol.Value(i)
			leftMap[val] = append(leftMap[val], i)
		}
	}

	var leftIndices []int
	var rightIndices []int

	leftMatched := make(map[int]bool)
	rightMatched := make(map[int]bool)

	for i := 0; i < leftStrCol.Len(); i++ {
		if leftStrCol.IsNull(i) {
			leftIndices = append(leftIndices, i)
			rightIndices = append(rightIndices, -1)
			continue
		}
		val := leftStrCol.Value(i)
		if rIdxs, exists := rightMap[val]; exists {
			for _, rIdx := range rIdxs {
				leftIndices = append(leftIndices, i)
				rightIndices = append(rightIndices, rIdx)
				leftMatched[i] = true
				rightMatched[rIdx] = true
			}
		} else {
			leftIndices = append(leftIndices, i)
			rightIndices = append(rightIndices, -1)
		}
	}

	for i := 0; i < rightStrCol.Len(); i++ {
		if !rightMatched[i] {
			leftIndices = append(leftIndices, -1)
			rightIndices = append(rightIndices, i)
		}
	}

	return buildJoinResultWithNulls(left, right, on, leftIndices, rightIndices)
}

func crossJoin(left *dataframe.DataFrame, right *dataframe.DataFrame) (*dataframe.DataFrame, error) {
	var leftIndices []int
	var rightIndices []int

	for i := 0; i < left.NumRows(); i++ {
		for j := 0; j < right.NumRows(); j++ {
			leftIndices = append(leftIndices, i)
			rightIndices = append(rightIndices, j)
		}
	}

	return buildJoinResult(left, right, "", leftIndices, rightIndices)
}

func buildJoinResult(left, right *dataframe.DataFrame, on string, leftIdx, rightIdx []int) (*dataframe.DataFrame, error) {
	result := dataframe.New()
	alloc := memory.DefaultAllocator

	for i := 0; i < left.NumCols(); i++ {
		col, _ := left.Col(i)
		newCol := copySeriesByIndices(col, leftIdx, alloc)
		result.AddSeries(newCol)
	}

	for i := 0; i < right.NumCols(); i++ {
		col, _ := right.Col(i)
		if on != "" && col.Name() == on {
			continue
		}
		newCol := copySeriesByIndices(col, rightIdx, alloc)
		result.AddSeries(newCol)
	}

	return result, nil
}

func buildJoinResultWithNulls(left, right *dataframe.DataFrame, on string, leftIdx, rightIdx []int) (*dataframe.DataFrame, error) {
	result := dataframe.New()
	alloc := memory.DefaultAllocator

	for i := 0; i < left.NumCols(); i++ {
		col, _ := left.Col(i)
		valid := make([]bool, len(leftIdx))
		for j, idx := range leftIdx {
			valid[j] = idx >= 0
		}
		newCol := copySeriesByIndicesWithNulls(col, leftIdx, valid, alloc)
		result.AddSeries(newCol)
	}

	for i := 0; i < right.NumCols(); i++ {
		col, _ := right.Col(i)
		if on != "" && col.Name() == on {
			continue
		}
		valid := make([]bool, len(rightIdx))
		for j, idx := range rightIdx {
			valid[j] = idx >= 0
		}
		newCol := copySeriesByIndicesWithNulls(col, rightIdx, valid, alloc)
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

func copySeriesByIndicesWithNulls(col series.Series, indices []int, valid []bool, alloc memory.Allocator) series.Series {
	switch typedCol := col.(type) {
	case *series.StringSeries:
		var copied []string
		for i, idx := range indices {
			if valid[i] && idx >= 0 {
				copied = append(copied, typedCol.Value(idx))
			} else {
				copied = append(copied, "")
			}
		}
		return series.NewStringSeries(typedCol.Name(), alloc, copied, valid)
	case *series.Float64Series:
		var copied []float64
		for i, idx := range indices {
			if valid[i] && idx >= 0 {
				copied = append(copied, typedCol.Value(idx))
			} else {
				copied = append(copied, 0)
			}
		}
		return series.NewFloat64Series(typedCol.Name(), alloc, copied, valid)
	case *series.Int64Series:
		var copied []int64
		for i, idx := range indices {
			if valid[i] && idx >= 0 {
				copied = append(copied, typedCol.Value(idx))
			} else {
				copied = append(copied, 0)
			}
		}
		return series.NewInt64Series(typedCol.Name(), alloc, copied, valid)
	case *series.BooleanSeries:
		var copied []bool
		for i, idx := range indices {
			if valid[i] && idx >= 0 {
				copied = append(copied, typedCol.Value(idx))
			} else {
				copied = append(copied, false)
			}
		}
		return series.NewBooleanSeries(typedCol.Name(), alloc, copied, valid)
	}
	return nil
}
