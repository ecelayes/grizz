package series

import (
	grizzarrows "github.com/ecelayes/grizz/internal/arrow"
	grizzmemory "github.com/ecelayes/grizz/internal/memory"
)

type BinarySeries struct {
	name string
	data *grizzarrows.BinaryArray
}

func NewBinarySeries(name string, mem grizzmemory.Allocator, values [][]byte, valid []bool) *BinarySeries {
	builder := grizzarrows.NewBinaryBuilder(mem)
	defer builder.Release()
	for _, v := range values {
		builder.Append(v)
	}
	if valid != nil {
		for i, v := range valid {
			if !v {
				builder.SetNull(i)
			}
		}
	}
	return &BinarySeries{name: name, data: builder.NewBinaryArray()}
}

func (s *BinarySeries) Name() string               { return s.name }
func (s *BinarySeries) SetName(name string)        { s.name = name }
func (s *BinarySeries) Type() grizzarrows.DataType { return s.data.DataType() }
func (s *BinarySeries) Len() int                   { return s.data.Len() }
func (s *BinarySeries) IsNull(i int) bool          { return s.data.IsNull(i) }
func (s *BinarySeries) Value(i int) []byte         { return s.data.Value(i) }

func (s *BinarySeries) Release() {
	if s.data != nil {
		s.data.Release()
	}
}
