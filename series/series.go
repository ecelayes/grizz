package series

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type Series interface {
	Name() string
	Type() arrow.DataType
	Len() int
	IsNull(i int) bool
	Release()
}

type Float64Series struct {
	name string
	data *array.Float64
}

func NewFloat64Series(name string, mem memory.Allocator, values []float64, valid []bool) *Float64Series {
	builder := array.NewFloat64Builder(mem)
	defer builder.Release()

	builder.AppendValues(values, valid)

	return &Float64Series{
		name: name,
		data: builder.NewFloat64Array(),
	}
}

func (s *Float64Series) Name() string {
	return s.name
}

func (s *Float64Series) Type() arrow.DataType {
	return s.data.DataType()
}

func (s *Float64Series) Len() int {
	return s.data.Len()
}

func (s *Float64Series) IsNull(i int) bool {
	return s.data.IsNull(i)
}

func (s *Float64Series) Value(i int) float64 {
	return s.data.Value(i)
}

func (s *Float64Series) Release() {
	if s.data != nil {
		s.data.Release()
	}
}
