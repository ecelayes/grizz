package csv

import (
	"encoding/csv"
	"fmt"
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

	writer := csv.NewWriter(file)
	defer writer.Flush()

	numCols := df.NumCols()
	numRows := df.NumRows()
	if numCols == 0 {
		return nil
	}

	headers := make([]string, numCols)
	for i := 0; i < numCols; i++ {
		col, _ := df.Col(i)
		headers[i] = col.Name()
	}

	if err := writer.Write(headers); err != nil {
		return err
	}

	for r := 0; r < numRows; r++ {
		row := make([]string, numCols)
		for c := 0; c < numCols; c++ {
			col, _ := df.Col(c)

			if col.IsNull(r) {
				row[c] = ""
				continue
			}

			switch typedCol := col.(type) {
			case *series.StringSeries:
				row[c] = typedCol.Value(r)
			case *series.Int64Series:
				row[c] = fmt.Sprintf("%d", typedCol.Value(r))
			case *series.Float64Series:
				row[c] = fmt.Sprintf("%v", typedCol.Value(r))
			case *series.BooleanSeries:
				row[c] = fmt.Sprintf("%t", typedCol.Value(r))
			default:
				row[c] = ""
			}
		}
		
		if err := writer.Write(row); err != nil {
			return err
		}
	}

	return nil
}
