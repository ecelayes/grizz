package series

import (
	grizzarrows "github.com/ecelayes/grizz/internal/arrow"
	grizzmemory "github.com/ecelayes/grizz/internal/memory"
)

type Int8Series struct {
	name string
	data *grizzarrows.Int8Array
}

func NewInt8Series(name string, mem grizzmemory.Allocator, values []int8, valid []bool) *Int8Series {
	builder := grizzarrows.NewInt8Builder(mem)
	defer builder.Release()
	builder.AppendValues(values, valid)
	return &Int8Series{name: name, data: builder.NewInt8Array()}
}

func (s *Int8Series) Name() string               { return s.name }
func (s *Int8Series) SetName(name string)        { s.name = name }
func (s *Int8Series) Type() grizzarrows.DataType { return s.data.DataType() }
func (s *Int8Series) Len() int                   { return s.data.Len() }
func (s *Int8Series) IsNull(i int) bool          { return s.data.IsNull(i) }
func (s *Int8Series) Value(i int) int8           { return s.data.Value(i) }

func (s *Int8Series) Release() {
	if s.data != nil {
		s.data.Release()
	}
}

func (s *Int8Series) Sum() float64 {
	var sum int64
	for i := 0; i < s.Len(); i++ {
		if !s.IsNull(i) {
			sum += int64(s.Value(i))
		}
	}
	return float64(sum)
}

func (s *Int8Series) Mean() float64 {
	var sum int64
	count := 0
	for i := 0; i < s.Len(); i++ {
		if !s.IsNull(i) {
			sum += int64(s.Value(i))
			count++
		}
	}
	if count == 0 {
		return 0
	}
	return float64(sum) / float64(count)
}

func (s *Int8Series) Min() int8 {
	var min int8 = 0
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

func (s *Int8Series) Max() int8 {
	var max int8 = 0
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
