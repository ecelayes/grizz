package arrow

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	grizzmemory "github.com/ecelayes/grizz/internal/memory"
)

type Array = arrow.Array

type Int32Array = array.Int32
type Int64Array = array.Int64
type Float32Array = array.Float32
type Float64Array = array.Float64
type StringArray = array.String
type BooleanArray = array.Boolean
type UInt8Array = array.Uint8
type UInt16Array = array.Uint16
type UInt32Array = array.Uint32
type UInt64Array = array.Uint64
type Int8Array = array.Int8
type Int16Array = array.Int16
type Date32Array = array.Date32
type Date64Array = array.Date64
type BinaryArray = array.Binary

type Int32Builder = array.Int32Builder
type Int64Builder = array.Int64Builder
type Float32Builder = array.Float32Builder
type Float64Builder = array.Float64Builder
type StringBuilder = array.StringBuilder
type BooleanBuilder = array.BooleanBuilder
type UInt8Builder = array.Uint8Builder
type UInt16Builder = array.Uint16Builder
type UInt32Builder = array.Uint32Builder
type UInt64Builder = array.Uint64Builder
type Int8Builder = array.Int8Builder
type Int16Builder = array.Int16Builder
type Date32Builder = array.Date32Builder
type Date64Builder = array.Date64Builder
type BinaryBuilder = array.BinaryBuilder

func NewInt32Builder(mem grizzmemory.Allocator) *Int32Builder {
	return array.NewInt32Builder(mem)
}

func NewInt64Builder(mem grizzmemory.Allocator) *Int64Builder {
	return array.NewInt64Builder(mem)
}

func NewFloat32Builder(mem grizzmemory.Allocator) *Float32Builder {
	return array.NewFloat32Builder(mem)
}

func NewFloat64Builder(mem grizzmemory.Allocator) *Float64Builder {
	return array.NewFloat64Builder(mem)
}

func NewStringBuilder(mem grizzmemory.Allocator) *StringBuilder {
	return array.NewStringBuilder(mem)
}

func NewBooleanBuilder(mem grizzmemory.Allocator) *BooleanBuilder {
	return array.NewBooleanBuilder(mem)
}

func NewUInt8Builder(mem grizzmemory.Allocator) *UInt8Builder {
	return array.NewUint8Builder(mem)
}

func NewUInt16Builder(mem grizzmemory.Allocator) *UInt16Builder {
	return array.NewUint16Builder(mem)
}

func NewUInt32Builder(mem grizzmemory.Allocator) *UInt32Builder {
	return array.NewUint32Builder(mem)
}

func NewUInt64Builder(mem grizzmemory.Allocator) *UInt64Builder {
	return array.NewUint64Builder(mem)
}

func NewInt8Builder(mem grizzmemory.Allocator) *Int8Builder {
	return array.NewInt8Builder(mem)
}

func NewInt16Builder(mem grizzmemory.Allocator) *Int16Builder {
	return array.NewInt16Builder(mem)
}

func NewDate32Builder(mem grizzmemory.Allocator) *Date32Builder {
	return array.NewDate32Builder(mem)
}

func NewDate64Builder(mem grizzmemory.Allocator) *Date64Builder {
	return array.NewDate64Builder(mem)
}

func NewBinaryBuilder(mem grizzmemory.Allocator) *BinaryBuilder {
	return array.NewBinaryBuilder(mem, &arrow.BinaryType{})
}
