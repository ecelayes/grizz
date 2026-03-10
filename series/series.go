package series

import (
	grizzarrows "github.com/ecelayes/grizz/internal/arrow"
	grizzmemory "github.com/ecelayes/grizz/internal/memory"
)

type Series interface {
	Name() string
	Type() grizzarrows.DataType
	Len() int
	IsNull(i int) bool
	Release()
}

type Float64Series struct {
	name string
	data *grizzarrows.Float64Array
}

func NewFloat64Series(name string, mem grizzmemory.Allocator, values []float64, valid []bool) *Float64Series {
	builder := grizzarrows.NewFloat64Builder(mem)
	defer builder.Release()

	builder.AppendValues(values, valid)

	return &Float64Series{
		name: name,
		data: builder.NewFloat64Array(),
	}
}

func (s *Float64Series) Name() string {
	return s.name
}

func (s *Float64Series) Type() grizzarrows.DataType {
	return s.data.DataType()
}

func (s *Float64Series) Len() int {
	return s.data.Len()
}

func (s *Float64Series) IsNull(i int) bool {
	return s.data.IsNull(i)
}

func (s *Float64Series) Value(i int) float64 {
	return s.data.Value(i)
}

func (s *Float64Series) Release() {
	if s.data != nil {
		s.data.Release()
	}
}

func (s *Float64Series) Sum() float64 {
	var sum float64
	for i := 0; i < s.Len(); i++ {
		if !s.IsNull(i) {
			sum += s.Value(i)
		}
	}
	return sum
}

func (s *Float64Series) Mean() float64 {
	var sum float64
	count := 0
	for i := 0; i < s.Len(); i++ {
		if !s.IsNull(i) {
			sum += s.Value(i)
			count++
		}
	}
	if count == 0 {
		return 0
	}
	return sum / float64(count)
}

func (s *Float64Series) Min() float64 {
	var min float64 = 0
	first := true
	for i := 0; i < s.Len(); i++ {
		if !s.IsNull(i) {
			if first || s.Value(i) < min {
				min = s.Value(i)
				first = false
			}
		}
	}
	return min
}

func (s *Float64Series) Max() float64 {
	var max float64 = 0
	first := true
	for i := 0; i < s.Len(); i++ {
		if !s.IsNull(i) {
			if first || s.Value(i) > max {
				max = s.Value(i)
				first = false
			}
		}
	}
	return max
}

func (s *Float64Series) Count() int {
	count := 0
	for i := 0; i < s.Len(); i++ {
		if !s.IsNull(i) {
			count++
		}
	}
	return count
}
