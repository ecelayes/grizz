package dataframe

import (
	"math/rand"

	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func (df *DataFrame) Sample(n int, withReplacement bool, seed int64) (*DataFrame, error) {
	r := rand.New(rand.NewSource(seed))
	numRows := df.NumRows()

	if n <= 0 {
		n = numRows / 10
		if n < 1 {
			n = 1
		}
	}

	var indices []int
	if withReplacement {
		for i := 0; i < n; i++ {
			indices = append(indices, r.Intn(numRows))
		}
	} else {
		if n > numRows {
			n = numRows
		}
		perm := r.Perm(numRows)
		indices = perm[:n]
	}

	return df.take(indices)
}

func (df *DataFrame) SampleFrac(frac float64, withReplacement bool, seed int64) (*DataFrame, error) {
	n := int(float64(df.NumRows()) * frac)
	if n < 1 {
		n = 1
	}
	return df.Sample(n, withReplacement, seed)
}

func (df *DataFrame) Shuffle(seed int64) (*DataFrame, error) {
	r := rand.New(rand.NewSource(seed))
	perm := r.Perm(df.NumRows())
	return df.take(perm)
}

func (df *DataFrame) take(indices []int) (*DataFrame, error) {
	result := New()
	alloc := memory.DefaultAllocator

	for i := 0; i < df.NumCols(); i++ {
		col, err := df.Col(i)
		if err != nil {
			return nil, err
		}

		switch c := col.(type) {
		case *series.Int64Series:
			result.AddSeries(takeInt64(c, indices, alloc))
		case *series.Float64Series:
			result.AddSeries(takeFloat64(c, indices, alloc))
		case *series.StringSeries:
			result.AddSeries(takeString(c, indices, alloc))
		case *series.BooleanSeries:
			result.AddSeries(takeBoolean(c, indices, alloc))
		case *series.BinarySeries:
			result.AddSeries(takeBinary(c, indices, alloc))
		default:
			return nil, nil
		}
	}

	return result, nil
}

func takeInt64(s *series.Int64Series, indices []int, alloc memory.Allocator) *series.Int64Series {
	result := make([]int64, len(indices))
	valid := make([]bool, len(indices))
	for i, idx := range indices {
		result[i] = s.Value(idx)
		valid[i] = !s.IsNull(idx)
	}
	return series.NewInt64Series(s.Name(), alloc, result, valid)
}

func takeFloat64(s *series.Float64Series, indices []int, alloc memory.Allocator) *series.Float64Series {
	result := make([]float64, len(indices))
	valid := make([]bool, len(indices))
	for i, idx := range indices {
		result[i] = s.Value(idx)
		valid[i] = !s.IsNull(idx)
	}
	return series.NewFloat64Series(s.Name(), alloc, result, valid)
}

func takeString(s *series.StringSeries, indices []int, alloc memory.Allocator) *series.StringSeries {
	result := make([]string, len(indices))
	valid := make([]bool, len(indices))
	for i, idx := range indices {
		result[i] = s.Value(idx)
		valid[i] = !s.IsNull(idx)
	}
	return series.NewStringSeries(s.Name(), alloc, result, valid)
}

func takeBoolean(s *series.BooleanSeries, indices []int, alloc memory.Allocator) *series.BooleanSeries {
	result := make([]bool, len(indices))
	valid := make([]bool, len(indices))
	for i, idx := range indices {
		result[i] = s.Value(idx)
		valid[i] = !s.IsNull(idx)
	}
	return series.NewBooleanSeries(s.Name(), alloc, result, valid)
}

func takeBinary(s *series.BinarySeries, indices []int, alloc memory.Allocator) *series.BinarySeries {
	result := make([][]byte, len(indices))
	valid := make([]bool, len(indices))
	for i, idx := range indices {
		result[i] = s.Value(idx)
		valid[i] = !s.IsNull(idx)
	}
	return series.NewBinarySeries(s.Name(), alloc, result, valid)
}
