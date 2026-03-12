package engine

import (
	"fmt"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func applyMelt(df *dataframe.DataFrame, idVars []string, valueVars []string) (*dataframe.DataFrame, error) {
	result := dataframe.New()
	alloc := memory.DefaultAllocator

	for _, idVar := range idVars {
		col, err := df.ColByName(idVar)
		if err != nil {
			return nil, err
		}

		var newVals []string
		for rep := 0; rep < len(valueVars); rep++ {
			for i := 0; i < df.NumRows(); i++ {
				if col.IsNull(i) {
					newVals = append(newVals, "")
				} else {
					switch c := col.(type) {
					case *series.StringSeries:
						newVals = append(newVals, c.Value(i))
					case *series.Int64Series:
						newVals = append(newVals, fmt.Sprintf("%d", c.Value(i)))
					case *series.Float64Series:
						newVals = append(newVals, fmt.Sprintf("%f", c.Value(i)))
					case *series.BooleanSeries:
						newVals = append(newVals, fmt.Sprintf("%t", c.Value(i)))
					}
				}
			}
		}
		result.AddSeries(series.NewStringSeries(idVar, alloc, newVals, nil))
	}

	var allValues []string
	var allVariables []string
	for _, valVar := range valueVars {
		col, err := df.ColByName(valVar)
		if err != nil {
			return nil, err
		}
		for i := 0; i < df.NumRows(); i++ {
			if col.IsNull(i) {
				allValues = append(allValues, "")
			} else {
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
				allValues = append(allValues, val)
			}
			allVariables = append(allVariables, valVar)
		}
	}

	result.AddSeries(series.NewStringSeries("variable", alloc, allVariables, nil))
	result.AddSeries(series.NewStringSeries("value", alloc, allValues, nil))

	return result, nil
}
