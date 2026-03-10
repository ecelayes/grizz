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
		isBool := true
		isInt := true
		isFloat := true
		hasValue := false

		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			sample := dataRows[rowIdx][i]
			if sample == "" {
				continue
			}
			hasValue = true

			if _, err := strconv.ParseBool(sample); err != nil {
				isBool = false
			}
			if _, err := strconv.ParseInt(sample, 10, 64); err != nil {
				isInt = false
			}
			if _, err := strconv.ParseFloat(sample, 64); err != nil {
				isFloat = false
			}
		}

		if !hasValue {
			colTypes[i] = "string"
		} else if isInt {
			colTypes[i] = "int"
		} else if isFloat {
			colTypes[i] = "float"
		} else if isBool {
			colTypes[i] = "bool"
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
			valid := make([]bool, numRows)
			for rowIdx := 0; rowIdx < numRows; rowIdx++ {
				val := dataRows[rowIdx][colIdx]
				if val == "" {
					valid[rowIdx] = false
					continue
				}
				parsed, err := strconv.ParseBool(val)
				if err != nil {
					valid[rowIdx] = false
					continue
				}
				values[rowIdx] = parsed
				valid[rowIdx] = true
			}
			df.AddSeries(series.NewBooleanSeries(colName, alloc, values, valid))
		case "int":
			values := make([]int64, numRows)
			valid := make([]bool, numRows)
			for rowIdx := 0; rowIdx < numRows; rowIdx++ {
				val := dataRows[rowIdx][colIdx]
				if val == "" {
					valid[rowIdx] = false
					continue
				}
				parsed, err := strconv.ParseInt(val, 10, 64)
				if err != nil {
					valid[rowIdx] = false
					continue
				}
				values[rowIdx] = parsed
				valid[rowIdx] = true
			}
			df.AddSeries(series.NewInt64Series(colName, alloc, values, valid))
		case "float":
			values := make([]float64, numRows)
			valid := make([]bool, numRows)
			for rowIdx := 0; rowIdx < numRows; rowIdx++ {
				val := dataRows[rowIdx][colIdx]
				if val == "" {
					valid[rowIdx] = false
					continue
				}
				parsed, err := strconv.ParseFloat(val, 64)
				if err != nil {
					valid[rowIdx] = false
					continue
				}
				values[rowIdx] = parsed
				valid[rowIdx] = true
			}
			df.AddSeries(series.NewFloat64Series(colName, alloc, values, valid))
		default:
			values := make([]string, numRows)
			valid := make([]bool, numRows)
			for rowIdx := 0; rowIdx < numRows; rowIdx++ {
				val := dataRows[rowIdx][colIdx]
				values[rowIdx] = val
				valid[rowIdx] = val != ""
			}
			df.AddSeries(series.NewStringSeries(colName, alloc, values, valid))
		}
	}

	return df, nil
}
