package engine

import (
	"fmt"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func applyDropNulls(df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
	mask := make([]bool, df.NumRows())
	for i := 0; i < df.NumRows(); i++ {
		mask[i] = true
	}

	for colIdx := 0; colIdx < df.NumCols(); colIdx++ {
		col, _ := df.Col(colIdx)
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				mask[i] = false
			}
		}
	}

	return applyMask(df, mask)
}

func applyDistinct(df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
	seen := make(map[string]bool)
	var keepIndices []int

	for i := 0; i < df.NumRows(); i++ {
		key := ""
		for j := 0; j < df.NumCols(); j++ {
			col, _ := df.Col(j)
			if j > 0 {
				key += "|"
			}
			if col.IsNull(i) {
				key += "NULL"
			} else {
				switch c := col.(type) {
				case *series.StringSeries:
					key += c.Value(i)
				case *series.Int64Series:
					key += fmt.Sprintf("%d", c.Value(i))
				case *series.Float64Series:
					key += fmt.Sprintf("%f", c.Value(i))
				case *series.BooleanSeries:
					if c.Value(i) {
						key += "true"
					} else {
						key += "false"
					}
				}
			}
		}
		if !seen[key] {
			seen[key] = true
			keepIndices = append(keepIndices, i)
		}
	}

	result := dataframe.New()
	alloc := memory.DefaultAllocator

	for i := 0; i < df.NumCols(); i++ {
		col, _ := df.Col(i)
		newCol := copySeriesByIndices(col, keepIndices, alloc)
		result.AddSeries(newCol)
	}

	return result, nil
}
