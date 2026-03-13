package series

import (
	grizzarrows "github.com/ecelayes/grizz/internal/arrow"
	grizzmemory "github.com/ecelayes/grizz/internal/memory"
)

type UInt64Series struct {
	name string
	data *grizzarrows.UInt64Array
}

func NewUInt64Series(name string, mem grizzmemory.Allocator, values []uint64, valid []bool) *UInt64Series {
	builder := grizzarrows.NewUInt64Builder(mem)
	defer builder.Release()
	builder.AppendValues(values, valid)
	return &UInt64Series{name: name, data: builder.NewUint64Array()}
}

func (s *UInt64Series) Name() string               { return s.name }
func (s *UInt64Series) SetName(name string)        { s.name = name }
func (s *UInt64Series) Type() grizzarrows.DataType { return s.data.DataType() }
func (s *UInt64Series) Len() int                   { return s.data.Len() }
func (s *UInt64Series) IsNull(i int) bool          { return s.data.IsNull(i) }
func (s *UInt64Series) Value(i int) uint64         { return s.data.Value(i) }

func (s *UInt64Series) Release() {
	if s.data != nil {
		s.data.Release()
	}
}

func (s *UInt64Series) Sum() float64 {
	var sum uint64
	for i := 0; i < s.Len(); i++ {
		if !s.IsNull(i) {
			sum += s.Value(i)
		}
	}
	return float64(sum)
}

func (s *UInt64Series) Mean() float64 {
	var sum uint64
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

func (s *UInt64Series) Min() uint64 {
	var min uint64 = 0
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

func (s *UInt64Series) Max() uint64 {
	var max uint64 = 0
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
