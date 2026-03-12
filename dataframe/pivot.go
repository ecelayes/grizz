package dataframe

import (
	"fmt"

	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func (df *DataFrame) Pivot(index, column, value string) (*DataFrame, error) {
	columnCol, err := df.ColByName(column)
	if err != nil {
		return nil, err
	}
	valueCol, err := df.ColByName(value)
	if err != nil {
		return nil, err
	}

	uniqueColumns := make(map[string]bool)
	for i := 0; i < columnCol.Len(); i++ {
		if !columnCol.IsNull(i) {
			switch c := columnCol.(type) {
			case *series.StringSeries:
				uniqueColumns[c.Value(i)] = true
			}
		}
	}

	indexCol, err := df.ColByName(index)
	if err != nil {
		return nil, err
	}

	indexGroups := make(map[string][]int)
	for i := 0; i < indexCol.Len(); i++ {
		var key string
		switch idx := indexCol.(type) {
		case *series.StringSeries:
			key = idx.Value(i)
		case *series.Int64Series:
			key = fmt.Sprintf("%d", idx.Value(i))
		case *series.Float64Series:
			key = fmt.Sprintf("%f", idx.Value(i))
		}
		indexGroups[key] = append(indexGroups[key], i)
	}

	result := New()
	alloc := memory.DefaultAllocator

	var indexValues []string
	var indexValid []bool
	for key := range indexGroups {
		indexValues = append(indexValues, key)
		indexValid = append(indexValid, true)
	}
	result.AddSeries(series.NewStringSeries(index, alloc, indexValues, indexValid))

	sortedCols := make([]string, 0, len(uniqueColumns))
	for k := range uniqueColumns {
		sortedCols = append(sortedCols, k)
	}

	for _, colName := range sortedCols {
		var values []string
		var valid []bool

		for _, indices := range indexGroups {
			var aggregated string
			var isValid bool

			for _, rowIdx := range indices {
				match := false
				if !columnCol.IsNull(rowIdx) {
					switch c := columnCol.(type) {
					case *series.StringSeries:
						if c.Value(rowIdx) == colName {
							match = true
						}
					}
				}

				if match {
					switch v := valueCol.(type) {
					case *series.StringSeries:
						if !v.IsNull(rowIdx) {
							aggregated = v.Value(rowIdx)
							isValid = true
							break
						}
					case *series.Int64Series:
						if !v.IsNull(rowIdx) {
							aggregated = fmt.Sprintf("%d", v.Value(rowIdx))
							isValid = true
							break
						}
					case *series.Float64Series:
						if !v.IsNull(rowIdx) {
							aggregated = fmt.Sprintf("%f", v.Value(rowIdx))
							isValid = true
							break
						}
					}
				}
			}

			if isValid {
				values = append(values, aggregated)
				valid = append(valid, true)
			} else {
				values = append(values, "")
				valid = append(valid, false)
			}
		}

		newCol := series.NewStringSeries(colName, alloc, values, valid)
		result.AddSeries(newCol)
	}

	return result, nil
}

func (df *DataFrame) Melt(idVars []string, valueVars []string) (*DataFrame, error) {
	result := New()
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
