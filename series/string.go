package series

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type StringSeries struct {
	name string
	data *array.String
}

func NewStringSeries(name string, mem memory.Allocator, values []string, valid []bool) *StringSeries {
	builder := array.NewStringBuilder(mem)
	defer builder.Release()

	builder.AppendValues(values, valid)

	return &StringSeries{
		name: name,
		data: builder.NewStringArray(),
	}
}

func (s *StringSeries) Name() string {
	return s.name
}

func (s *StringSeries) Type() arrow.DataType {
	return s.data.DataType()
}

func (s *StringSeries) Len() int {
	return s.data.Len()
}

func (s *StringSeries) IsNull(i int) bool {
	return s.data.IsNull(i)
}

func (s *StringSeries) Value(i int) string {
	return s.data.Value(i)
}

func (s *StringSeries) Release() {
	if s.data != nil {
		s.data.Release()
	}
}
