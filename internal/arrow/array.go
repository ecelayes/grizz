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

type Int32Builder = array.Int32Builder
type Int64Builder = array.Int64Builder
type Float32Builder = array.Float32Builder
type Float64Builder = array.Float64Builder
type StringBuilder = array.StringBuilder
type BooleanBuilder = array.BooleanBuilder

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
