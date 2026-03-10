package json

import (
	"encoding/json"
	"os"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/series"
)

func Write(df *dataframe.DataFrame, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	numRows := df.NumRows()
	numCols := df.NumCols()

	data := make([]map[string]interface{}, numRows)

	for i := 0; i < numRows; i++ {
		data[i] = make(map[string]interface{})
	}

	for colIdx := 0; colIdx < numCols; colIdx++ {
		col, err := df.Col(colIdx)
		if err != nil {
			return err
		}
		colName := col.Name()

		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			if col.IsNull(rowIdx) {
				data[rowIdx][colName] = nil
				continue
			}

			switch c := col.(type) {
			case *series.Int64Series:
				data[rowIdx][colName] = c.Value(rowIdx)
			case *series.Float64Series:
				data[rowIdx][colName] = c.Value(rowIdx)
			case *series.StringSeries:
				data[rowIdx][colName] = c.Value(rowIdx)
			case *series.BooleanSeries:
				data[rowIdx][colName] = c.Value(rowIdx)
			}
		}
	}

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(data); err != nil {
		return err
	}

	return nil
}
