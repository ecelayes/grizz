package series

import (
	grizzarrows "github.com/ecelayes/grizz/internal/arrow"
	grizzmemory "github.com/ecelayes/grizz/internal/memory"
)

type Int16Series struct {
	name string
	data *grizzarrows.Int16Array
}

func NewInt16Series(name string, mem grizzmemory.Allocator, values []int16, valid []bool) *Int16Series {
	builder := grizzarrows.NewInt16Builder(mem)
	defer builder.Release()
	builder.AppendValues(values, valid)
	return &Int16Series{name: name, data: builder.NewInt16Array()}
}

func (s *Int16Series) Name() string               { return s.name }
func (s *Int16Series) SetName(name string)        { s.name = name }
func (s *Int16Series) Type() grizzarrows.DataType { return s.data.DataType() }
func (s *Int16Series) Len() int                   { return s.data.Len() }
func (s *Int16Series) IsNull(i int) bool          { return s.data.IsNull(i) }
func (s *Int16Series) Value(i int) int16          { return s.data.Value(i) }

func (s *Int16Series) Release() {
	if s.data != nil {
		s.data.Release()
	}
}

func (s *Int16Series) Sum() float64 {
	var sum int64
	for i := 0; i < s.Len(); i++ {
		if !s.IsNull(i) {
			sum += int64(s.Value(i))
		}
	}
	return float64(sum)
}

func (s *Int16Series) Mean() float64 {
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

func (s *Int16Series) Min() int16 {
	var min int16 = 0
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

func (s *Int16Series) Max() int16 {
	var max int16 = 0
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
