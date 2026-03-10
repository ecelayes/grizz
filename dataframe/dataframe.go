package dataframe

import (
	"errors"
	"fmt"

	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

type DataFrame struct {
	columns []series.Series
	rows    int
}

func New() *DataFrame {
	return &DataFrame{
		columns: make([]series.Series, 0),
		rows:    0,
	}
}

func (df *DataFrame) AddSeries(s series.Series) error {
	if df.rows > 0 && s.Len() != df.rows {
		return fmt.Errorf("series length %d does not match dataframe rows %d", s.Len(), df.rows)
	}

	if df.rows == 0 {
		df.rows = s.Len()
	}

	df.columns = append(df.columns, s)
	return nil
}

func (df *DataFrame) NumCols() int {
	return len(df.columns)
}

func (df *DataFrame) NumRows() int {
	return df.rows
}

func (df *DataFrame) Col(index int) (series.Series, error) {
	if index < 0 || index >= len(df.columns) {
		return nil, errors.New("column index out of bounds")
	}
	return df.columns[index], nil
}

func (df *DataFrame) Release() {
	for _, col := range df.columns {
		col.Release()
	}
}

func (df *DataFrame) ColByName(name string) (series.Series, error) {
	for _, col := range df.columns {
		if col.Name() == name {
			return col, nil
		}
	}
	return nil, fmt.Errorf("column %s not found", name)
}

func (df *DataFrame) Concat(other *DataFrame) (*DataFrame, error) {
	if df.NumCols() != other.NumCols() {
		return nil, fmt.Errorf("cannot concat: different number of columns (%d vs %d)", df.NumCols(), other.NumCols())
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

func (df *DataFrame) Union(other *DataFrame) (*DataFrame, error) {
	return df.Concat(other)
}

func (df *DataFrame) Pivot(index, column, value string) (*DataFrame, error) {
	columnCol, err := df.ColByName(column)
	if err != nil {
		return nil, err
	}
	valueCol, err := df.ColByName(value)
	if err != nil {
		return nil, err
	}

	uniqueIndex := df.UniqueValues(index)
	uniqueColumns := df.UniqueValues(column)

	result := New()
	alloc := memory.DefaultAllocator

	result.AddSeries(series.NewStringSeries(index, alloc, uniqueIndex, nil))

	for _, colVal := range uniqueColumns {
		var values []string
		for i := 0; i < df.NumRows(); i++ {
			if columnCol.(*series.StringSeries).Value(i) == colVal {
				values = append(values, valueCol.(*series.StringSeries).Value(i))
			}
		}
		result.AddSeries(series.NewStringSeries(colVal, alloc, values, nil))
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
		result.AddSeries(col)
	}

	var allValues []string
	var allVariables []string
	for _, valVar := range valueVars {
		col, err := df.ColByName(valVar)
		if err != nil {
			return nil, err
		}
		for i := 0; i < df.NumRows(); i++ {
			allValues = append(allValues, col.(*series.StringSeries).Value(i))
			allVariables = append(allVariables, valVar)
		}
	}

	result.AddSeries(series.NewStringSeries("variable", alloc, allVariables, nil))
	result.AddSeries(series.NewStringSeries("value", alloc, allValues, nil))

	return result, nil
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
		val := col.(*series.StringSeries).Value(i)
		if !seen[val] {
			seen[val] = true
			unique = append(unique, val)
		}
	}

	return unique
}
