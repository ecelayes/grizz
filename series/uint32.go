package series

import (
	grizzarrows "github.com/ecelayes/grizz/internal/arrow"
	grizzmemory "github.com/ecelayes/grizz/internal/memory"
)

type UInt32Series struct {
	name string
	data *grizzarrows.UInt32Array
}

func NewUInt32Series(name string, mem grizzmemory.Allocator, values []uint32, valid []bool) *UInt32Series {
	builder := grizzarrows.NewUInt32Builder(mem)
	defer builder.Release()
	builder.AppendValues(values, valid)
	return &UInt32Series{name: name, data: builder.NewUint32Array()}
}

func (s *UInt32Series) Name() string               { return s.name }
func (s *UInt32Series) SetName(name string)        { s.name = name }
func (s *UInt32Series) Type() grizzarrows.DataType { return s.data.DataType() }
func (s *UInt32Series) Len() int                   { return s.data.Len() }
func (s *UInt32Series) IsNull(i int) bool          { return s.data.IsNull(i) }
func (s *UInt32Series) Value(i int) uint32         { return s.data.Value(i) }

func (s *UInt32Series) Release() {
	if s.data != nil {
		s.data.Release()
	}
}

func (s *UInt32Series) Sum() float64 {
	var sum uint64
	for i := 0; i < s.Len(); i++ {
		if !s.IsNull(i) {
			sum += uint64(s.Value(i))
		}
	}
	return float64(sum)
}

func (s *UInt32Series) Mean() float64 {
	var sum uint64
	count := 0
	for i := 0; i < s.Len(); i++ {
		if !s.IsNull(i) {
			sum += uint64(s.Value(i))
			count++
		}
	}
	if count == 0 {
		return 0
	}
	return float64(sum) / float64(count)
}

func (s *UInt32Series) Min() uint32 {
	var min uint32 = 0
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

func (s *UInt32Series) Max() uint32 {
	var max uint32 = 0
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
