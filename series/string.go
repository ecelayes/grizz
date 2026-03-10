package series

import (
	grizzarrows "github.com/ecelayes/grizz/internal/arrow"
	grizzmemory "github.com/ecelayes/grizz/internal/memory"
)

type StringSeries struct {
	name string
	data *grizzarrows.StringArray
}

func NewStringSeries(name string, mem grizzmemory.Allocator, values []string, valid []bool) *StringSeries {
	builder := grizzarrows.NewStringBuilder(mem)
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

func (s *StringSeries) Type() grizzarrows.DataType {
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
