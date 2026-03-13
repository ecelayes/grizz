package arrowio

import (
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"

	"github.com/ecelayes/grizz/dataframe"
	grizzmem "github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func Read(filePath string) (*dataframe.DataFrame, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader, err := ipc.NewFileReader(file, ipc.WithAllocator(grizzmem.DefaultAllocator))
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	schema := reader.Schema()
	numCols := schema.NumFields()
	if numCols == 0 {
		return dataframe.New(), nil
	}

	var records []arrow.Record
	for {
		record, err := reader.Read()
		if record == nil {
			break
		}
		if err != nil {
			return nil, err
		}
		records = append(records, record)
		defer record.Release()
	}

	if len(records) == 0 {
		return dataframe.New(), nil
	}

	totalRows := 0
	for _, rec := range records {
		totalRows += int(rec.NumRows())
	}

	df := dataframe.New()
	alloc := grizzmem.DefaultAllocator

	for colIdx := 0; colIdx < numCols; colIdx++ {
		field := schema.Field(colIdx)
		colName := field.Name
		arrowType := field.Type

		var colSeries series.Series

		switch arrowType.ID() {
		case arrow.INT64:
			colSeries, err = readInt64Column(records, colIdx, colName, totalRows, alloc)
		case arrow.FLOAT64:
			colSeries, err = readFloat64Column(records, colIdx, colName, totalRows, alloc)
		case arrow.STRING:
			colSeries, err = readStringColumn(records, colIdx, colName, totalRows, alloc)
		case arrow.BOOL:
			colSeries, err = readBooleanColumn(records, colIdx, colName, totalRows, alloc)
		case arrow.INT32:
			colSeries, err = readInt32Column(records, colIdx, colName, totalRows, alloc)
		case arrow.FLOAT32:
			colSeries, err = readFloat32Column(records, colIdx, colName, totalRows, alloc)
		case arrow.UINT8:
			colSeries, err = readUint8Column(records, colIdx, colName, totalRows, alloc)
		case arrow.UINT16:
			colSeries, err = readUint16Column(records, colIdx, colName, totalRows, alloc)
		case arrow.UINT32:
			colSeries, err = readUint32Column(records, colIdx, colName, totalRows, alloc)
		case arrow.UINT64:
			colSeries, err = readUint64Column(records, colIdx, colName, totalRows, alloc)
		case arrow.INT8:
			colSeries, err = readInt8Column(records, colIdx, colName, totalRows, alloc)
		case arrow.INT16:
			colSeries, err = readInt16Column(records, colIdx, colName, totalRows, alloc)
		case arrow.DATE32:
			colSeries, err = readDate32Column(records, colIdx, colName, totalRows, alloc)
		case arrow.DATE64:
			colSeries, err = readDate64Column(records, colIdx, colName, totalRows, alloc)
		default:
			colSeries, err = readStringColumn(records, colIdx, colName, totalRows, alloc)
		}

		if err != nil {
			return nil, err
		}

		df.AddSeries(colSeries)
	}

	return df, nil
}

func readInt64Column(records []arrow.Record, colIdx int, colName string, totalRows int, alloc grizzmem.Allocator) (series.Series, error) {
	values := make([]int64, totalRows)
	valid := make([]bool, totalRows)
	offset := 0

	for _, rec := range records {
		col := rec.Column(colIdx)
		intCol := col.(*array.Int64)
		numRows := int(rec.NumRows())

		for i := 0; i < numRows; i++ {
			if intCol.IsValid(i) {
				values[offset+i] = intCol.Value(i)
				valid[offset+i] = true
			} else {
				valid[offset+i] = false
			}
		}
		offset += numRows
	}

	return series.NewInt64Series(colName, alloc, values, valid), nil
}

func readFloat64Column(records []arrow.Record, colIdx int, colName string, totalRows int, alloc grizzmem.Allocator) (series.Series, error) {
	values := make([]float64, totalRows)
	valid := make([]bool, totalRows)
	offset := 0

	for _, rec := range records {
		col := rec.Column(colIdx)
		floatCol := col.(*array.Float64)
		numRows := int(rec.NumRows())

		for i := 0; i < numRows; i++ {
			if floatCol.IsValid(i) {
				values[offset+i] = floatCol.Value(i)
				valid[offset+i] = true
			} else {
				valid[offset+i] = false
			}
		}
		offset += numRows
	}

	return series.NewFloat64Series(colName, alloc, values, valid), nil
}

func readStringColumn(records []arrow.Record, colIdx int, colName string, totalRows int, alloc grizzmem.Allocator) (series.Series, error) {
	values := make([]string, totalRows)
	valid := make([]bool, totalRows)
	offset := 0

	for _, rec := range records {
		col := rec.Column(colIdx)
		strCol := col.(*array.String)
		numRows := int(rec.NumRows())

		for i := 0; i < numRows; i++ {
			if strCol.IsValid(i) {
				values[offset+i] = strCol.Value(i)
				valid[offset+i] = true
			} else {
				valid[offset+i] = false
			}
		}
		offset += numRows
	}

	return series.NewStringSeries(colName, alloc, values, valid), nil
}

func readBooleanColumn(records []arrow.Record, colIdx int, colName string, totalRows int, alloc grizzmem.Allocator) (series.Series, error) {
	values := make([]bool, totalRows)
	valid := make([]bool, totalRows)
	offset := 0

	for _, rec := range records {
		col := rec.Column(colIdx)
		boolCol := col.(*array.Boolean)
		numRows := int(rec.NumRows())

		for i := 0; i < numRows; i++ {
			if boolCol.IsValid(i) {
				values[offset+i] = boolCol.Value(i)
				valid[offset+i] = true
			} else {
				valid[offset+i] = false
			}
		}
		offset += numRows
	}

	return series.NewBooleanSeries(colName, alloc, values, valid), nil
}

func readInt32Column(records []arrow.Record, colIdx int, colName string, totalRows int, alloc grizzmem.Allocator) (series.Series, error) {
	values := make([]int64, totalRows)
	valid := make([]bool, totalRows)
	offset := 0

	for _, rec := range records {
		col := rec.Column(colIdx)
		intCol := col.(*array.Int32)
		numRows := int(rec.NumRows())

		for i := 0; i < numRows; i++ {
			if intCol.IsValid(i) {
				values[offset+i] = int64(intCol.Value(i))
				valid[offset+i] = true
			} else {
				valid[offset+i] = false
			}
		}
		offset += numRows
	}

	return series.NewInt64Series(colName, alloc, values, valid), nil
}

func readFloat32Column(records []arrow.Record, colIdx int, colName string, totalRows int, alloc grizzmem.Allocator) (series.Series, error) {
	values := make([]float64, totalRows)
	valid := make([]bool, totalRows)
	offset := 0

	for _, rec := range records {
		col := rec.Column(colIdx)
		floatCol := col.(*array.Float32)
		numRows := int(rec.NumRows())

		for i := 0; i < numRows; i++ {
			if floatCol.IsValid(i) {
				values[offset+i] = float64(floatCol.Value(i))
				valid[offset+i] = true
			} else {
				valid[offset+i] = false
			}
		}
		offset += numRows
	}

	return series.NewFloat64Series(colName, alloc, values, valid), nil
}

func readUint8Column(records []arrow.Record, colIdx int, colName string, totalRows int, alloc grizzmem.Allocator) (series.Series, error) {
	values := make([]int64, totalRows)
	valid := make([]bool, totalRows)
	offset := 0

	for _, rec := range records {
		col := rec.Column(colIdx)
		intCol := col.(*array.Uint8)
		numRows := int(rec.NumRows())

		for i := 0; i < numRows; i++ {
			if intCol.IsValid(i) {
				values[offset+i] = int64(intCol.Value(i))
				valid[offset+i] = true
			} else {
				valid[offset+i] = false
			}
		}
		offset += numRows
	}

	return series.NewInt64Series(colName, alloc, values, valid), nil
}

func readUint16Column(records []arrow.Record, colIdx int, colName string, totalRows int, alloc grizzmem.Allocator) (series.Series, error) {
	values := make([]int64, totalRows)
	valid := make([]bool, totalRows)
	offset := 0

	for _, rec := range records {
		col := rec.Column(colIdx)
		intCol := col.(*array.Uint16)
		numRows := int(rec.NumRows())

		for i := 0; i < numRows; i++ {
			if intCol.IsValid(i) {
				values[offset+i] = int64(intCol.Value(i))
				valid[offset+i] = true
			} else {
				valid[offset+i] = false
			}
		}
		offset += numRows
	}

	return series.NewInt64Series(colName, alloc, values, valid), nil
}

func readUint32Column(records []arrow.Record, colIdx int, colName string, totalRows int, alloc grizzmem.Allocator) (series.Series, error) {
	values := make([]int64, totalRows)
	valid := make([]bool, totalRows)
	offset := 0

	for _, rec := range records {
		col := rec.Column(colIdx)
		intCol := col.(*array.Uint32)
		numRows := int(rec.NumRows())

		for i := 0; i < numRows; i++ {
			if intCol.IsValid(i) {
				values[offset+i] = int64(intCol.Value(i))
				valid[offset+i] = true
			} else {
				valid[offset+i] = false
			}
		}
		offset += numRows
	}

	return series.NewInt64Series(colName, alloc, values, valid), nil
}

func readUint64Column(records []arrow.Record, colIdx int, colName string, totalRows int, alloc grizzmem.Allocator) (series.Series, error) {
	values := make([]int64, totalRows)
	valid := make([]bool, totalRows)
	offset := 0

	for _, rec := range records {
		col := rec.Column(colIdx)
		intCol := col.(*array.Uint64)
		numRows := int(rec.NumRows())

		for i := 0; i < numRows; i++ {
			if intCol.IsValid(i) {
				values[offset+i] = int64(intCol.Value(i))
				valid[offset+i] = true
			} else {
				valid[offset+i] = false
			}
		}
		offset += numRows
	}

	return series.NewInt64Series(colName, alloc, values, valid), nil
}

func readInt8Column(records []arrow.Record, colIdx int, colName string, totalRows int, alloc grizzmem.Allocator) (series.Series, error) {
	values := make([]int64, totalRows)
	valid := make([]bool, totalRows)
	offset := 0

	for _, rec := range records {
		col := rec.Column(colIdx)
		intCol := col.(*array.Int8)
		numRows := int(rec.NumRows())

		for i := 0; i < numRows; i++ {
			if intCol.IsValid(i) {
				values[offset+i] = int64(intCol.Value(i))
				valid[offset+i] = true
			} else {
				valid[offset+i] = false
			}
		}
		offset += numRows
	}

	return series.NewInt64Series(colName, alloc, values, valid), nil
}

func readInt16Column(records []arrow.Record, colIdx int, colName string, totalRows int, alloc grizzmem.Allocator) (series.Series, error) {
	values := make([]int64, totalRows)
	valid := make([]bool, totalRows)
	offset := 0

	for _, rec := range records {
		col := rec.Column(colIdx)
		intCol := col.(*array.Int16)
		numRows := int(rec.NumRows())

		for i := 0; i < numRows; i++ {
			if intCol.IsValid(i) {
				values[offset+i] = int64(intCol.Value(i))
				valid[offset+i] = true
			} else {
				valid[offset+i] = false
			}
		}
		offset += numRows
	}

	return series.NewInt64Series(colName, alloc, values, valid), nil
}

func readDate32Column(records []arrow.Record, colIdx int, colName string, totalRows int, alloc grizzmem.Allocator) (series.Series, error) {
	values := make([]int64, totalRows)
	valid := make([]bool, totalRows)
	offset := 0

	for _, rec := range records {
		col := rec.Column(colIdx)
		dateCol := col.(*array.Date32)
		numRows := int(rec.NumRows())

		for i := 0; i < numRows; i++ {
			if dateCol.IsValid(i) {
				values[offset+i] = int64(dateCol.Value(i))
				valid[offset+i] = true
			} else {
				valid[offset+i] = false
			}
		}
		offset += numRows
	}

	return series.NewInt64Series(colName, alloc, values, valid), nil
}

func readDate64Column(records []arrow.Record, colIdx int, colName string, totalRows int, alloc grizzmem.Allocator) (series.Series, error) {
	values := make([]int64, totalRows)
	valid := make([]bool, totalRows)
	offset := 0

	for _, rec := range records {
		col := rec.Column(colIdx)
		dateCol := col.(*array.Date64)
		numRows := int(rec.NumRows())

		for i := 0; i < numRows; i++ {
			if dateCol.IsValid(i) {
				values[offset+i] = int64(dateCol.Value(i))
				valid[offset+i] = true
			} else {
				valid[offset+i] = false
			}
		}
		offset += numRows
	}

	return series.NewInt64Series(colName, alloc, values, valid), nil
}
