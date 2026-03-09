package series

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type BooleanSeries struct {
	name string
	data *array.Boolean
}

func NewBooleanSeries(name string, mem memory.Allocator, values []bool, valid []bool) *BooleanSeries {
	builder := array.NewBooleanBuilder(mem)
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

func (s *BooleanSeries) Type() arrow.DataType {
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
