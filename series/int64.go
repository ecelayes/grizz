package series

import (
	grizzarrows "github.com/ecelayes/grizz/internal/arrow"
	grizzmemory "github.com/ecelayes/grizz/internal/memory"
)

type Int64Series struct {
	name string
	data *grizzarrows.Int64Array
}

func NewInt64Series(name string, mem grizzmemory.Allocator, values []int64, valid []bool) *Int64Series {
	builder := grizzarrows.NewInt64Builder(mem)
	defer builder.Release()

	builder.AppendValues(values, valid)

	return &Int64Series{
		name: name,
		data: builder.NewInt64Array(),
	}
}

func (s *Int64Series) Name() string {
	return s.name
}

func (s *Int64Series) Type() grizzarrows.DataType {
	return s.data.DataType()
}

func (s *Int64Series) Len() int {
	return s.data.Len()
}

func (s *Int64Series) IsNull(i int) bool {
	return s.data.IsNull(i)
}

func (s *Int64Series) Value(i int) int64 {
	return s.data.Value(i)
}

func (s *Int64Series) Release() {
	if s.data != nil {
		s.data.Release()
	}
}

func (s *Int64Series) Sum() float64 {
	var sum int64
	count := 0
	for i := 0; i < s.Len(); i++ {
		if !s.IsNull(i) {
			sum += s.Value(i)
			count++
		}
	}
	return float64(sum)
}

func (s *Int64Series) Mean() float64 {
	var sum int64
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
	return float64(sum) / float64(count)
}

func (s *Int64Series) Min() int64 {
	var min int64 = 0
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

func (s *Int64Series) Max() int64 {
	var max int64 = 0
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

func (s *Int64Series) Count() int {
	count := 0
	for i := 0; i < s.Len(); i++ {
		if !s.IsNull(i) {
			count++
		}
	}
	return count
}
