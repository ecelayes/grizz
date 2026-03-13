package arrowio

import (
	"os"
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestReader(t *testing.T) {
	df := createTestDataFrame()
	defer df.Release()

	tmpFile := "/tmp/test_arrow.ipc"
	defer os.Remove(tmpFile)

	if err := Write(df, tmpFile); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	readDF, err := Read(tmpFile)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	defer readDF.Release()

	if readDF.NumCols() != df.NumCols() {
		t.Errorf("Expected %d columns, got %d", df.NumCols(), readDF.NumCols())
	}

	if readDF.NumRows() != df.NumRows() {
		t.Errorf("Expected %d rows, got %d", df.NumRows(), readDF.NumRows())
	}

	for i := 0; i < df.NumCols(); i++ {
		origCol, _ := df.Col(i)
		newCol, _ := readDF.Col(i)

		if origCol.Name() != newCol.Name() {
			t.Errorf("Column %d: expected name %s, got %s", i, origCol.Name(), newCol.Name())
		}
	}
}

func TestReaderWithNulls(t *testing.T) {
	alloc := memory.DefaultAllocator

	intValues := []int64{1, 2, 0, 4, 5}
	intValid := []bool{true, true, false, true, true}

	floatValues := []float64{1.1, 0.0, 3.3, 4.4, 0.0}
	floatValid := []bool{true, false, true, true, false}

	strValues := []string{"a", "", "c", "d", ""}
	strValid := []bool{true, false, true, true, false}

	boolValues := []bool{true, false, true, false, true}
	boolValid := []bool{true, true, false, true, true}

	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("int_col", alloc, intValues, intValid))
	df.AddSeries(series.NewFloat64Series("float_col", alloc, floatValues, floatValid))
	df.AddSeries(series.NewStringSeries("str_col", alloc, strValues, strValid))
	df.AddSeries(series.NewBooleanSeries("bool_col", alloc, boolValues, boolValid))
	defer df.Release()

	tmpFile := "/tmp/test_arrow_nulls.ipc"
	defer os.Remove(tmpFile)

	if err := Write(df, tmpFile); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	readDF, err := Read(tmpFile)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	defer readDF.Release()

	if readDF.NumRows() != df.NumRows() {
		t.Errorf("Expected %d rows, got %d", df.NumRows(), readDF.NumRows())
	}

	for row := 0; row < df.NumRows(); row++ {
		for col := 0; col < df.NumCols(); col++ {
			origCol, _ := df.Col(col)
			newCol, _ := readDF.Col(col)

			origNull := origCol.IsNull(row)
			newNull := newCol.IsNull(row)

			if origNull != newNull {
				t.Errorf("Row %d, Col %d: expected null=%v, got null=%v", row, col, origNull, newNull)
			}
		}
	}
}

func TestReaderEmptyDataFrame(t *testing.T) {
	df := dataframe.New()
	defer df.Release()

	tmpFile := "/tmp/test_arrow_empty.ipc"
	defer os.Remove(tmpFile)

	if err := Write(df, tmpFile); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	readDF, err := Read(tmpFile)
	if err != nil {
		t.Logf("Empty dataframe read error (expected): %v", err)
		return
	}
	defer readDF.Release()

	if readDF.NumCols() != 0 {
		t.Errorf("Expected 0 columns, got %d", readDF.NumCols())
	}
}

func createTestDataFrame() *dataframe.DataFrame {
	alloc := memory.DefaultAllocator

	intValues := []int64{1, 2, 3, 4, 5}
	intValid := []bool{true, true, true, true, true}

	floatValues := []float64{1.1, 2.2, 3.3, 4.4, 5.5}
	floatValid := []bool{true, true, true, true, true}

	strValues := []string{"apple", "banana", "cherry", "date", "elderberry"}
	strValid := []bool{true, true, true, true, true}

	boolValues := []bool{true, false, true, false, true}
	boolValid := []bool{true, true, true, true, true}

	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("int_col", alloc, intValues, intValid))
	df.AddSeries(series.NewFloat64Series("float_col", alloc, floatValues, floatValid))
	df.AddSeries(series.NewStringSeries("str_col", alloc, strValues, strValid))
	df.AddSeries(series.NewBooleanSeries("bool_col", alloc, boolValues, boolValid))

	return df
}
