package sql

import (
	"github.com/ecelayes/grizz/dataframe"
)

func SQL(query string, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
	stmt, err := Parse(query)
	if err != nil {
		return nil, err
	}

	engine := NewEngine(df)
	return engine.Execute(stmt)
}
