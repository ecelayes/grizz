package series

import (
	grizzarrows "github.com/ecelayes/grizz/internal/arrow"
	grizzmemory "github.com/ecelayes/grizz/internal/memory"
)

type UInt8Series struct {
	name string
	data *grizzarrows.UInt8Array
}

func NewUInt8Series(name string, mem grizzmemory.Allocator, values []uint8, valid []bool) *UInt8Series {
	builder := grizzarrows.NewUInt8Builder(mem)
	defer builder.Release()
	builder.AppendValues(values, valid)
	return &UInt8Series{name: name, data: builder.NewUint8Array()}
}

func (s *UInt8Series) Name() string               { return s.name }
func (s *UInt8Series) SetName(name string)        { s.name = name }
func (s *UInt8Series) Type() grizzarrows.DataType { return s.data.DataType() }
func (s *UInt8Series) Len() int                   { return s.data.Len() }
func (s *UInt8Series) IsNull(i int) bool          { return s.data.IsNull(i) }
func (s *UInt8Series) Value(i int) uint8          { return s.data.Value(i) }

func (s *UInt8Series) Release() {
	if s.data != nil {
		s.data.Release()
	}
}

func (s *UInt8Series) Sum() float64 {
	var sum uint64
	for i := 0; i < s.Len(); i++ {
		if !s.IsNull(i) {
			sum += uint64(s.Value(i))
		}
	}
	return float64(sum)
}

func (s *UInt8Series) Mean() float64 {
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

func (s *UInt8Series) Min() uint8 {
	var min uint8 = 0
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

func (s *UInt8Series) Max() uint8 {
	var max uint8 = 0
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
