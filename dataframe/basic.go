package dataframe

import (
	"errors"
	"fmt"

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

func (df *DataFrame) Columns() []string {
	result := make([]string, len(df.columns))
	for i, col := range df.columns {
		result[i] = col.Name()
	}
	return result
}

func (df *DataFrame) Dtypes() []string {
	result := make([]string, len(df.columns))
	for i, col := range df.columns {
		result[i] = col.Type().Name()
	}
	return result
}

func (df *DataFrame) Shape() (int, int) {
	return df.rows, len(df.columns)
}

func (df *DataFrame) IsEmpty() bool {
	return df.rows == 0 || len(df.columns) == 0
}
