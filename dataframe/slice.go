package dataframe

import (
	"fmt"
	"strings"

	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func (df *DataFrame) Slice(offset, length int) *DataFrame {
	if offset < 0 {
		offset = 0
	}
	if offset >= df.NumRows() {
		return New()
	}

	end := offset + length
	if end > df.NumRows() {
		end = df.NumRows()
	}

	result := New()

	for i := 0; i < df.NumCols(); i++ {
		col, _ := df.Col(i)
		slicedCol := sliceSeries(col, offset, end)
		result.AddSeries(slicedCol)
	}

	return result
}

func sliceSeries(col series.Series, offset, end int) series.Series {
	switch c := col.(type) {
	case *series.Int64Series:
		values := make([]int64, end-offset)
		valid := make([]bool, end-offset)
		for i := offset; i < end; i++ {
			values[i-offset] = c.Value(i)
			valid[i-offset] = !c.IsNull(i)
		}
		return series.NewInt64Series(c.Name(), memory.DefaultAllocator, values, valid)
	case *series.Float64Series:
		values := make([]float64, end-offset)
		valid := make([]bool, end-offset)
		for i := offset; i < end; i++ {
			values[i-offset] = c.Value(i)
			valid[i-offset] = !c.IsNull(i)
		}
		return series.NewFloat64Series(c.Name(), memory.DefaultAllocator, values, valid)
	case *series.StringSeries:
		values := make([]string, end-offset)
		valid := make([]bool, end-offset)
		for i := offset; i < end; i++ {
			values[i-offset] = c.Value(i)
			valid[i-offset] = !c.IsNull(i)
		}
		return series.NewStringSeries(c.Name(), memory.DefaultAllocator, values, valid)
	case *series.BooleanSeries:
		values := make([]bool, end-offset)
		valid := make([]bool, end-offset)
		for i := offset; i < end; i++ {
			values[i-offset] = c.Value(i)
			valid[i-offset] = !c.IsNull(i)
		}
		return series.NewBooleanSeries(c.Name(), memory.DefaultAllocator, values, valid)
	default:
		return col
	}
}

func (lf *LazyFrame) Slice(offset, length int) *LazyFrame {
	return &LazyFrame{
		plan: SlicePlan{
			Input:  lf.plan,
			Offset: offset,
			Length: length,
		},
	}
}

type SlicePlan struct {
	Input  LogicalPlan
	Offset int
	Length int
}

func (s SlicePlan) Explain(indent int) string {
	pad := repeat("  ", indent)
	inputStr := s.Input.Explain(indent + 1)
	return fmt.Sprintf("%sSlice: offset=%d, length=%d\n%s", pad, s.Offset, s.Length, inputStr)
}

func repeat(s string, count int) string {
	return strings.Repeat(s, count)
}
