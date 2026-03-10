package series

import (
	grizzarrows "github.com/ecelayes/grizz/internal/arrow"
	grizzmemory "github.com/ecelayes/grizz/internal/memory"
)

type BooleanSeries struct {
	name string
	data *grizzarrows.BooleanArray
}

func NewBooleanSeries(name string, mem grizzmemory.Allocator, values []bool, valid []bool) *BooleanSeries {
	builder := grizzarrows.NewBooleanBuilder(mem)
	defer builder.Release()

	builder.AppendValues(values, valid)

	return &BooleanSeries{
		name: name,
		data: builder.NewBooleanArray(),
	}
}

func (s *BooleanSeries) Name() string {
	return s.name
}

func (s *BooleanSeries) Type() grizzarrows.DataType {
	return s.data.DataType()
}

func (s *BooleanSeries) Len() int {
	return s.data.Len()
}

func (s *BooleanSeries) IsNull(i int) bool {
	return s.data.IsNull(i)
}

func (s *BooleanSeries) Value(i int) bool {
	return s.data.Value(i)
}

func (s *BooleanSeries) Release() {
	if s.data != nil {
		s.data.Release()
	}
}
