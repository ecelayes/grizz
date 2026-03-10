package json

import (
	"encoding/json"
	"fmt"
	"os"

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

	var data []map[string]interface{}
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&data); err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("JSON file is empty")
	}

	headers := make([]string, 0)
	for key := range data[0] {
		headers = append(headers, key)
	}

	df := dataframe.New()
	alloc := memory.DefaultAllocator

	for _, colName := range headers {
		var strValues []string
		var intValues []int64
		var floatValues []float64
		var boolValues []bool
		var valid []bool

		isString := false
		isInt := true
		isFloat := true
		isBool := true

		for _, row := range data {
			val, exists := row[colName]
			if !exists || val == nil {
				strValues = append(strValues, "")
				intValues = append(intValues, 0)
				floatValues = append(floatValues, 0)
				boolValues = append(boolValues, false)
				valid = append(valid, false)
				continue
			}

			valid = append(valid, true)

			switch v := val.(type) {
			case string:
				strValues = append(strValues, v)
				isInt = false
				isFloat = false
				isBool = false
			case float64:
				strValues = append(strValues, fmt.Sprintf("%v", v))
				intValues = append(intValues, int64(v))
				floatValues = append(floatValues, v)
				boolValues = append(boolValues, v != 0)
			case bool:
				strValues = append(strValues, fmt.Sprintf("%t", v))
				intValues = append(intValues, 0)
				floatValues = append(floatValues, 0)
				boolValues = append(boolValues, v)
				isString = false
				isInt = false
				isFloat = false
			case nil:
				strValues = append(strValues, "")
				intValues = append(intValues, 0)
				floatValues = append(floatValues, 0)
				boolValues = append(boolValues, false)
				valid[len(valid)-1] = false
			default:
				strValues = append(strValues, fmt.Sprintf("%v", v))
				isInt = false
				isFloat = false
				isBool = false
			}
		}

		if isBool && !isString && !isInt {
			df.AddSeries(series.NewBooleanSeries(colName, alloc, boolValues, valid))
		} else if isInt && !isString {
			df.AddSeries(series.NewInt64Series(colName, alloc, intValues, valid))
		} else if isFloat && !isString {
			df.AddSeries(series.NewFloat64Series(colName, alloc, floatValues, valid))
		} else {
			df.AddSeries(series.NewStringSeries(colName, alloc, strValues, valid))
		}
	}

	return df, nil
}
