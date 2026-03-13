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

	UInt8  DataType = arrow.PrimitiveTypes.Uint8
	UInt16 DataType = arrow.PrimitiveTypes.Uint16
	UInt32 DataType = arrow.PrimitiveTypes.Uint32
	UInt64 DataType = arrow.PrimitiveTypes.Uint64

	Int8  DataType = arrow.PrimitiveTypes.Int8
	Int16 DataType = arrow.PrimitiveTypes.Int16

	Date32 DataType = arrow.PrimitiveTypes.Date32
	Date64 DataType = arrow.PrimitiveTypes.Date64

	Binary DataType = &arrow.BinaryType{}
)
