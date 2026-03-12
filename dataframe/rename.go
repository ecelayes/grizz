package dataframe

import (
	"fmt"

	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func (df *DataFrame) Rename(columns map[string]string) *DataFrame {
	result := New()
	for i := 0; i < df.NumCols(); i++ {
		col, _ := df.Col(i)
		newName := columns[col.Name()]
		if newName == "" {
			newName = col.Name()
		}

		switch typedCol := col.(type) {
		case *series.Int64Series:
			var values []int64
			var valid []bool
			for j := 0; j < typedCol.Len(); j++ {
				values = append(values, typedCol.Value(j))
				valid = append(valid, !typedCol.IsNull(j))
			}
			result.AddSeries(series.NewInt64Series(newName, memory.DefaultAllocator, values, valid))

		case *series.Float64Series:
			var values []float64
			var valid []bool
			for j := 0; j < typedCol.Len(); j++ {
				values = append(values, typedCol.Value(j))
				valid = append(valid, !typedCol.IsNull(j))
			}
			result.AddSeries(series.NewFloat64Series(newName, memory.DefaultAllocator, values, valid))

		case *series.StringSeries:
			var values []string
			var valid []bool
			for j := 0; j < typedCol.Len(); j++ {
				values = append(values, typedCol.Value(j))
				valid = append(valid, !typedCol.IsNull(j))
			}
			result.AddSeries(series.NewStringSeries(newName, memory.DefaultAllocator, values, valid))

		case *series.BooleanSeries:
			var values []bool
			var valid []bool
			for j := 0; j < typedCol.Len(); j++ {
				values = append(values, typedCol.Value(j))
				valid = append(valid, !typedCol.IsNull(j))
			}
			result.AddSeries(series.NewBooleanSeries(newName, memory.DefaultAllocator, values, valid))
		}
	}
	return result
}

func (df *DataFrame) UniqueValues(colName string) []string {
	col, err := df.ColByName(colName)
	if err != nil {
		return nil
	}

	seen := make(map[string]bool)
	var unique []string

	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			continue
		}
		var val string
		switch c := col.(type) {
		case *series.StringSeries:
			val = c.Value(i)
		case *series.Int64Series:
			val = fmt.Sprintf("%d", c.Value(i))
		case *series.Float64Series:
			val = fmt.Sprintf("%f", c.Value(i))
		case *series.BooleanSeries:
			val = fmt.Sprintf("%t", c.Value(i))
		}
		if !seen[val] {
			seen[val] = true
			unique = append(unique, val)
		}
	}

	return unique
}
