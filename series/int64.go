package series

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type Int64Series struct {
	name string
	data *array.Int64
}

func NewInt64Series(name string, mem memory.Allocator, values []int64, valid []bool) *Int64Series {
	builder := array.NewInt64Builder(mem)
	defer builder.Release()

	builder.AppendValues(values, valid)

	return &Int64Series{
		name: name,
		data: builder.NewInt64Array(),
	}
}

func (s *Int64Series) Name() string {
	return s.name
}

func (s *Int64Series) Type() arrow.DataType {
	return s.data.DataType()
}

func (s *Int64Series) Len() int {
	return s.data.Len()
}

func (s *Int64Series) IsNull(i int) bool {
	return s.data.IsNull(i)
}

func (s *Int64Series) Value(i int) int64 {
	return s.data.Value(i)
}

func (s *Int64Series) Release() {
	if s.data != nil {
		s.data.Release()
	}
}
