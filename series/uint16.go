package series

import (
	grizzarrows "github.com/ecelayes/grizz/internal/arrow"
	grizzmemory "github.com/ecelayes/grizz/internal/memory"
)

type UInt16Series struct {
	name string
	data *grizzarrows.UInt16Array
}

func NewUInt16Series(name string, mem grizzmemory.Allocator, values []uint16, valid []bool) *UInt16Series {
	builder := grizzarrows.NewUInt16Builder(mem)
	defer builder.Release()
	builder.AppendValues(values, valid)
	return &UInt16Series{name: name, data: builder.NewUint16Array()}
}

func (s *UInt16Series) Name() string               { return s.name }
func (s *UInt16Series) SetName(name string)        { s.name = name }
func (s *UInt16Series) Type() grizzarrows.DataType { return s.data.DataType() }
func (s *UInt16Series) Len() int                   { return s.data.Len() }
func (s *UInt16Series) IsNull(i int) bool          { return s.data.IsNull(i) }
func (s *UInt16Series) Value(i int) uint16         { return s.data.Value(i) }

func (s *UInt16Series) Release() {
	if s.data != nil {
		s.data.Release()
	}
}

func (s *UInt16Series) Sum() float64 {
	var sum uint64
	for i := 0; i < s.Len(); i++ {
		if !s.IsNull(i) {
			sum += uint64(s.Value(i))
		}
	}
	return float64(sum)
}

func (s *UInt16Series) Mean() float64 {
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

func (s *UInt16Series) Min() uint16 {
	var min uint16 = 0
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

func (s *UInt16Series) Max() uint16 {
	var max uint16 = 0
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
