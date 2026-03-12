package dataframe

import (
	"fmt"

	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func (df *DataFrame) VStack(other *DataFrame) (*DataFrame, error) {
	if df.NumCols() != other.NumCols() {
		return nil, fmt.Errorf("cannot vstack: different number of columns (%d vs %d)", df.NumCols(), other.NumCols())
	}

	result := New()
	alloc := memory.DefaultAllocator

	for i := 0; i < df.NumCols(); i++ {
		col1, _ := df.Col(i)
		col2, _ := other.Col(i)

		switch c1 := col1.(type) {
		case *series.Int64Series:
			c2 := col2.(*series.Int64Series)
			values := make([]int64, c1.Len()+c2.Len())
			valid := make([]bool, c1.Len()+c2.Len())
			for j := 0; j < c1.Len(); j++ {
				values[j] = c1.Value(j)
				valid[j] = !c1.IsNull(j)
			}
			for j := 0; j < c2.Len(); j++ {
				values[c1.Len()+j] = c2.Value(j)
				valid[c1.Len()+j] = !c2.IsNull(j)
			}
			result.AddSeries(series.NewInt64Series(c1.Name(), alloc, values, valid))

		case *series.Float64Series:
			c2 := col2.(*series.Float64Series)
			values := make([]float64, c1.Len()+c2.Len())
			valid := make([]bool, c1.Len()+c2.Len())
			for j := 0; j < c1.Len(); j++ {
				values[j] = c1.Value(j)
				valid[j] = !c1.IsNull(j)
			}
			for j := 0; j < c2.Len(); j++ {
				values[c1.Len()+j] = c2.Value(j)
				valid[c1.Len()+j] = !c2.IsNull(j)
			}
			result.AddSeries(series.NewFloat64Series(c1.Name(), alloc, values, valid))

		case *series.StringSeries:
			c2 := col2.(*series.StringSeries)
			values := make([]string, c1.Len()+c2.Len())
			valid := make([]bool, c1.Len()+c2.Len())
			for j := 0; j < c1.Len(); j++ {
				values[j] = c1.Value(j)
				valid[j] = !c1.IsNull(j)
			}
			for j := 0; j < c2.Len(); j++ {
				values[c1.Len()+j] = c2.Value(j)
				valid[c1.Len()+j] = !c2.IsNull(j)
			}
			result.AddSeries(series.NewStringSeries(c1.Name(), alloc, values, valid))

		case *series.BooleanSeries:
			c2 := col2.(*series.BooleanSeries)
			values := make([]bool, c1.Len()+c2.Len())
			valid := make([]bool, c1.Len()+c2.Len())
			for j := 0; j < c1.Len(); j++ {
				values[j] = c1.Value(j)
				valid[j] = !c1.IsNull(j)
			}
			for j := 0; j < c2.Len(); j++ {
				values[c1.Len()+j] = c2.Value(j)
				valid[c1.Len()+j] = !c2.IsNull(j)
			}
			result.AddSeries(series.NewBooleanSeries(c1.Name(), alloc, values, valid))
		}
	}

	return result, nil
}
