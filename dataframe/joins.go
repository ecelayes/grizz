package dataframe

import (
	"fmt"

	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func (df *DataFrame) Join(other *DataFrame, on string, how JoinType) (*DataFrame, error) {
	switch how {
	case Inner:
		return innerJoin(df, other, on)
	case Left:
		return leftJoin(df, other, on)
	case Right:
		return rightJoin(df, other, on)
	case Outer:
		return outerJoin(df, other, on)
	case Cross:
		return crossJoin(df, other)
	default:
		return nil, fmt.Errorf("unsupported join type")
	}
}

func innerJoin(left, right *DataFrame, on string) (*DataFrame, error) {
	leftKeyCol, err := left.ColByName(on)
	if err != nil {
		return nil, err
	}
	rightKeyCol, err := right.ColByName(on)
	if err != nil {
		return nil, err
	}

	if leftStrCol, ok := leftKeyCol.(*series.StringSeries); ok {
		if rightStrCol, ok := rightKeyCol.(*series.StringSeries); ok {
			return innerJoinString(left, right, on, leftStrCol, rightStrCol)
		}
	}

	if leftIntCol, ok := leftKeyCol.(*series.Int64Series); ok {
		if rightIntCol, ok := rightKeyCol.(*series.Int64Series); ok {
			return innerJoinInt(left, right, on, leftIntCol, rightIntCol)
		}
	}

	if leftFloatCol, ok := leftKeyCol.(*series.Float64Series); ok {
		if rightFloatCol, ok := rightKeyCol.(*series.Float64Series); ok {
			return innerJoinFloat(left, right, on, leftFloatCol, rightFloatCol)
		}
	}

	return nil, fmt.Errorf("join key must be the same type in both dataframes")
}

func innerJoinString(left, right *DataFrame, on string, leftCol, rightCol *series.StringSeries) (*DataFrame, error) {
	rightMap := make(map[string][]int)
	for i := 0; i < rightCol.Len(); i++ {
		if !rightCol.IsNull(i) {
			rightMap[rightCol.Value(i)] = append(rightMap[rightCol.Value(i)], i)
		}
	}

	var leftIdx, rightIdx []int
	for i := 0; i < leftCol.Len(); i++ {
		if leftCol.IsNull(i) {
			continue
		}
		val := leftCol.Value(i)
		if rIdxs, exists := rightMap[val]; exists {
			for _, rIdx := range rIdxs {
				leftIdx = append(leftIdx, i)
				rightIdx = append(rightIdx, rIdx)
			}
		}
	}
	return buildJoinResult(left, right, on, leftIdx, rightIdx)
}

func innerJoinInt(left, right *DataFrame, on string, leftCol, rightCol *series.Int64Series) (*DataFrame, error) {
	rightMap := make(map[int64][]int)
	for i := 0; i < rightCol.Len(); i++ {
		if !rightCol.IsNull(i) {
			rightMap[rightCol.Value(i)] = append(rightMap[rightCol.Value(i)], i)
		}
	}

	var leftIdx, rightIdx []int
	for i := 0; i < leftCol.Len(); i++ {
		if leftCol.IsNull(i) {
			continue
		}
		val := leftCol.Value(i)
		if rIdxs, exists := rightMap[val]; exists {
			for _, rIdx := range rIdxs {
				leftIdx = append(leftIdx, i)
				rightIdx = append(rightIdx, rIdx)
			}
		}
	}
	return buildJoinResult(left, right, on, leftIdx, rightIdx)
}

func innerJoinFloat(left, right *DataFrame, on string, leftCol, rightCol *series.Float64Series) (*DataFrame, error) {
	rightMap := make(map[float64][]int)
	for i := 0; i < rightCol.Len(); i++ {
		if !rightCol.IsNull(i) {
			rightMap[rightCol.Value(i)] = append(rightMap[rightCol.Value(i)], i)
		}
	}

	var leftIdx, rightIdx []int
	for i := 0; i < leftCol.Len(); i++ {
		if leftCol.IsNull(i) {
			continue
		}
		val := leftCol.Value(i)
		if rIdxs, exists := rightMap[val]; exists {
			for _, rIdx := range rIdxs {
				leftIdx = append(leftIdx, i)
				rightIdx = append(rightIdx, rIdx)
			}
		}
	}
	return buildJoinResult(left, right, on, leftIdx, rightIdx)
}

func leftJoin(left, right *DataFrame, on string) (*DataFrame, error) {
	leftKeyCol, _ := left.ColByName(on)
	rightKeyCol, _ := right.ColByName(on)
	leftStrCol := leftKeyCol.(*series.StringSeries)
	rightStrCol := rightKeyCol.(*series.StringSeries)

	rightMap := make(map[string][]int)
	for i := 0; i < rightStrCol.Len(); i++ {
		if !rightStrCol.IsNull(i) {
			rightMap[rightStrCol.Value(i)] = append(rightMap[rightStrCol.Value(i)], i)
		}
	}

	var leftIdx, rightIdx []int
	for i := 0; i < leftStrCol.Len(); i++ {
		if leftStrCol.IsNull(i) {
			leftIdx = append(leftIdx, i)
			rightIdx = append(rightIdx, -1)
			continue
		}
		val := leftStrCol.Value(i)
		if rIdxs, exists := rightMap[val]; exists {
			for _, rIdx := range rIdxs {
				leftIdx = append(leftIdx, i)
				rightIdx = append(rightIdx, rIdx)
			}
		} else {
			leftIdx = append(leftIdx, i)
			rightIdx = append(rightIdx, -1)
		}
	}
	return buildJoinResultWithNulls(left, right, on, leftIdx, rightIdx)
}

func rightJoin(left, right *DataFrame, on string) (*DataFrame, error) {
	leftKeyCol, _ := left.ColByName(on)
	rightKeyCol, _ := right.ColByName(on)
	leftStrCol := leftKeyCol.(*series.StringSeries)
	rightStrCol := rightKeyCol.(*series.StringSeries)

	leftMap := make(map[string][]int)
	for i := 0; i < leftStrCol.Len(); i++ {
		if !leftStrCol.IsNull(i) {
			leftMap[leftStrCol.Value(i)] = append(leftMap[leftStrCol.Value(i)], i)
		}
	}

	var leftIdx, rightIdx []int
	for i := 0; i < rightStrCol.Len(); i++ {
		if rightStrCol.IsNull(i) {
			leftIdx = append(leftIdx, -1)
			rightIdx = append(rightIdx, i)
			continue
		}
		val := rightStrCol.Value(i)
		if lIdxs, exists := leftMap[val]; exists {
			for _, lIdx := range lIdxs {
				leftIdx = append(leftIdx, lIdx)
				rightIdx = append(rightIdx, i)
			}
		} else {
			leftIdx = append(leftIdx, -1)
			rightIdx = append(rightIdx, i)
		}
	}
	return buildJoinResultWithNulls(left, right, on, leftIdx, rightIdx)
}

func outerJoin(left, right *DataFrame, on string) (*DataFrame, error) {
	leftKeyCol, _ := left.ColByName(on)
	rightKeyCol, _ := right.ColByName(on)
	leftStrCol := leftKeyCol.(*series.StringSeries)
	rightStrCol := rightKeyCol.(*series.StringSeries)

	rightMap := make(map[string][]int)
	for i := 0; i < rightStrCol.Len(); i++ {
		if !rightStrCol.IsNull(i) {
			rightMap[rightStrCol.Value(i)] = append(rightMap[rightStrCol.Value(i)], i)
		}
	}

	leftMap := make(map[string][]int)
	for i := 0; i < leftStrCol.Len(); i++ {
		if !leftStrCol.IsNull(i) {
			leftMap[leftStrCol.Value(i)] = append(leftMap[leftStrCol.Value(i)], i)
		}
	}

	var leftIdx, rightIdx []int
	rightMatched := make(map[int]bool)

	for i := 0; i < leftStrCol.Len(); i++ {
		if leftStrCol.IsNull(i) {
			leftIdx = append(leftIdx, i)
			rightIdx = append(rightIdx, -1)
			continue
		}
		val := leftStrCol.Value(i)
		if rIdxs, exists := rightMap[val]; exists {
			for _, rIdx := range rIdxs {
				leftIdx = append(leftIdx, i)
				rightIdx = append(rightIdx, rIdx)
				rightMatched[rIdx] = true
			}
		} else {
			leftIdx = append(leftIdx, i)
			rightIdx = append(rightIdx, -1)
		}
	}

	for i := 0; i < rightStrCol.Len(); i++ {
		if !rightMatched[i] {
			leftIdx = append(leftIdx, -1)
			rightIdx = append(rightIdx, i)
		}
	}
	return buildJoinResultWithNulls(left, right, on, leftIdx, rightIdx)
}

func crossJoin(left, right *DataFrame) (*DataFrame, error) {
	var leftIdx, rightIdx []int
	for i := 0; i < left.NumRows(); i++ {
		for j := 0; j < right.NumRows(); j++ {
			leftIdx = append(leftIdx, i)
			rightIdx = append(rightIdx, j)
		}
	}
	return buildJoinResult(left, right, "", leftIdx, rightIdx)
}

func buildJoinResult(left, right *DataFrame, on string, leftIdx, rightIdx []int) (*DataFrame, error) {
	result := New()
	alloc := memory.DefaultAllocator

	for i := 0; i < left.NumCols(); i++ {
		col, _ := left.Col(i)
		result.AddSeries(copySeriesByIndices(col, leftIdx, alloc))
	}

	for i := 0; i < right.NumCols(); i++ {
		col, _ := right.Col(i)
		if on != "" && col.Name() == on {
			continue
		}
		result.AddSeries(copySeriesByIndices(col, rightIdx, alloc))
	}
	return result, nil
}

func buildJoinResultWithNulls(left, right *DataFrame, on string, leftIdx, rightIdx []int) (*DataFrame, error) {
	result := New()
	alloc := memory.DefaultAllocator

	for i := 0; i < left.NumCols(); i++ {
		col, _ := left.Col(i)
		valid := make([]bool, len(leftIdx))
		for j, idx := range leftIdx {
			valid[j] = idx >= 0
		}
		result.AddSeries(copySeriesByIndicesWithNulls(col, leftIdx, valid, alloc))
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
		result.AddSeries(copySeriesByIndicesWithNulls(col, rightIdx, valid, alloc))
	}
	return result, nil
}

func copySeriesByIndices(col series.Series, indices []int, alloc memory.Allocator) series.Series {
	switch c := col.(type) {
	case *series.StringSeries:
		var v []string
		for _, idx := range indices {
			v = append(v, c.Value(idx))
		}
		return series.NewStringSeries(c.Name(), alloc, v, nil)
	case *series.Float64Series:
		var v []float64
		for _, idx := range indices {
			v = append(v, c.Value(idx))
		}
		return series.NewFloat64Series(c.Name(), alloc, v, nil)
	case *series.Int64Series:
		var v []int64
		for _, idx := range indices {
			v = append(v, c.Value(idx))
		}
		return series.NewInt64Series(c.Name(), alloc, v, nil)
	case *series.BooleanSeries:
		var v []bool
		for _, idx := range indices {
			v = append(v, c.Value(idx))
		}
		return series.NewBooleanSeries(c.Name(), alloc, v, nil)
	}
	return nil
}

func copySeriesByIndicesWithNulls(col series.Series, indices []int, valid []bool, alloc memory.Allocator) series.Series {
	switch c := col.(type) {
	case *series.StringSeries:
		var v []string
		for i, idx := range indices {
			if valid[i] && idx >= 0 {
				v = append(v, c.Value(idx))
			} else {
				v = append(v, "")
			}
		}
		return series.NewStringSeries(c.Name(), alloc, v, valid)
	case *series.Float64Series:
		var v []float64
		for i, idx := range indices {
			if valid[i] && idx >= 0 {
				v = append(v, c.Value(idx))
			} else {
				v = append(v, 0)
			}
		}
		return series.NewFloat64Series(c.Name(), alloc, v, valid)
	case *series.Int64Series:
		var v []int64
		for i, idx := range indices {
			if valid[i] && idx >= 0 {
				v = append(v, c.Value(idx))
			} else {
				v = append(v, 0)
			}
		}
		return series.NewInt64Series(c.Name(), alloc, v, valid)
	case *series.BooleanSeries:
		var v []bool
		for i, idx := range indices {
			if valid[i] && idx >= 0 {
				v = append(v, c.Value(idx))
			} else {
				v = append(v, false)
			}
		}
		return series.NewBooleanSeries(c.Name(), alloc, v, valid)
	}
	return nil
}
