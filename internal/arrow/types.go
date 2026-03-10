package arrow

import (
	"github.com/apache/arrow-go/v18/arrow"
)

type DataType = arrow.DataType

var (
	Int32   DataType = arrow.PrimitiveTypes.Int32
	Int64   DataType = arrow.PrimitiveTypes.Int64
	Float32 DataType = arrow.PrimitiveTypes.Float32
	Float64 DataType = arrow.PrimitiveTypes.Float64
	String  DataType = &arrow.StringType{}
	Boolean DataType = &arrow.BooleanType{}
)
