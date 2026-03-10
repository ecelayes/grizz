package csv

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func Read(filePath string) (*dataframe.DataFrame, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	if len(records) < 2 {
		return nil, fmt.Errorf("CSV file must contain at least a header and one data row")
	}

	headers := records[0]
	dataRows := records[1:]
	numCols := len(headers)
	numRows := len(dataRows)

	colTypes := make([]string, numCols)
	for i := 0; i < numCols; i++ {
		sample := dataRows[0][i]
		if _, err := strconv.ParseBool(sample); err == nil {
			colTypes[i] = "bool"
		} else if _, err := strconv.ParseInt(sample, 10, 64); err == nil {
			colTypes[i] = "int"
		} else if _, err := strconv.ParseFloat(sample, 64); err == nil {
			colTypes[i] = "float"
		} else {
			colTypes[i] = "string"
		}
	}

	df := dataframe.New()
	alloc := memory.DefaultAllocator

	for colIdx := 0; colIdx < numCols; colIdx++ {
		colName := headers[colIdx]

		switch colTypes[colIdx] {
		case "bool":
			values := make([]bool, numRows)
			for rowIdx := 0; rowIdx < numRows; rowIdx++ {
				val, _ := strconv.ParseBool(dataRows[rowIdx][colIdx])
				values[rowIdx] = val
			}
			df.AddSeries(series.NewBooleanSeries(colName, alloc, values, nil))
		case "int":
			values := make([]int64, numRows)
			for rowIdx := 0; rowIdx < numRows; rowIdx++ {
				val, _ := strconv.ParseInt(dataRows[rowIdx][colIdx], 10, 64)
				values[rowIdx] = val
			}
			df.AddSeries(series.NewInt64Series(colName, alloc, values, nil))
		case "float":
			values := make([]float64, numRows)
			for rowIdx := 0; rowIdx < numRows; rowIdx++ {
				val, _ := strconv.ParseFloat(dataRows[rowIdx][colIdx], 64)
				values[rowIdx] = val
			}
			df.AddSeries(series.NewFloat64Series(colName, alloc, values, nil))
		default:
			values := make([]string, numRows)
			for rowIdx := 0; rowIdx < numRows; rowIdx++ {
				values[rowIdx] = dataRows[rowIdx][colIdx]
			}
			df.AddSeries(series.NewStringSeries(colName, alloc, values, nil))
		}
	}

	return df, nil
}
