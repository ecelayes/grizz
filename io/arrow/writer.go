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

func Write(df *dataframe.DataFrame, filePath string) error {
	numCols := df.NumCols()
	numRows := df.NumRows()

	if numCols == 0 {
		file, err := os.Create(filePath)
		if err != nil {
			return err
		}
		file.Close()
		return nil
	}

	alloc := grizzmem.DefaultAllocator

	fields := make([]arrow.Field, numCols)
	columns := make([]arrow.Array, numCols)

	for colIdx := 0; colIdx < numCols; colIdx++ {
		col, err := df.Col(colIdx)
		if err != nil {
			return err
		}

		fields[colIdx] = arrow.Field{
			Name: col.Name(),
			Type: col.Type(),
		}

		columns[colIdx], err = seriesToArray(col, alloc)
		if err != nil {
			return err
		}
		defer columns[colIdx].Release()
	}

	schema := arrow.NewSchema(fields, nil)

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer, err := ipc.NewFileWriter(file, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
	if err != nil {
		return err
	}

	record := array.NewRecord(schema, columns, int64(numRows))
	if record == nil {
		writer.Close()
		return nil
	}
	defer record.Release()

	if err := writer.Write(record); err != nil {
		writer.Close()
		return err
	}

	return writer.Close()
}

func seriesToArray(s series.Series, alloc grizzmem.Allocator) (arrow.Array, error) {
	numRows := s.Len()

	switch col := s.(type) {
	case *series.Int64Series:
		return int64SeriesToArray(col, numRows, alloc)
	case *series.Float64Series:
		return float64SeriesToArray(col, numRows, alloc)
	case *series.StringSeries:
		return stringSeriesToArray(col, numRows, alloc)
	case *series.BooleanSeries:
		return booleanSeriesToArray(col, numRows, alloc)
	default:
		return nil, nil
	}
}

func int64SeriesToArray(s *series.Int64Series, numRows int, alloc grizzmem.Allocator) (arrow.Array, error) {
	builder := array.NewInt64Builder(alloc)
	defer builder.Release()

	values := make([]int64, numRows)
	valid := make([]bool, numRows)

	for i := 0; i < numRows; i++ {
		if s.IsNull(i) {
			valid[i] = false
			continue
		}
		values[i] = s.Value(i)
		valid[i] = true
	}

	builder.AppendValues(values, valid)
	return builder.NewArray(), nil
}

func float64SeriesToArray(s *series.Float64Series, numRows int, alloc grizzmem.Allocator) (arrow.Array, error) {
	builder := array.NewFloat64Builder(alloc)
	defer builder.Release()

	values := make([]float64, numRows)
	valid := make([]bool, numRows)

	for i := 0; i < numRows; i++ {
		if s.IsNull(i) {
			valid[i] = false
			continue
		}
		values[i] = s.Value(i)
		valid[i] = true
	}

	builder.AppendValues(values, valid)
	return builder.NewArray(), nil
}

func stringSeriesToArray(s *series.StringSeries, numRows int, alloc grizzmem.Allocator) (arrow.Array, error) {
	builder := array.NewStringBuilder(alloc)
	defer builder.Release()

	values := make([]string, numRows)
	valid := make([]bool, numRows)

	for i := 0; i < numRows; i++ {
		if s.IsNull(i) {
			valid[i] = false
			continue
		}
		values[i] = s.Value(i)
		valid[i] = true
	}

	builder.AppendValues(values, valid)
	return builder.NewArray(), nil
}

func booleanSeriesToArray(s *series.BooleanSeries, numRows int, alloc grizzmem.Allocator) (arrow.Array, error) {
	builder := array.NewBooleanBuilder(alloc)
	defer builder.Release()

	values := make([]bool, numRows)
	valid := make([]bool, numRows)

	for i := 0; i < numRows; i++ {
		if s.IsNull(i) {
			valid[i] = false
			continue
		}
		values[i] = s.Value(i)
		valid[i] = true
	}

	builder.AppendValues(values, valid)
	return builder.NewArray(), nil
}
