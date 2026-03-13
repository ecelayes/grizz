package arrowio

import (
	"os"
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestWriterBasic(t *testing.T) {
	df := createTestDataFrameForWriter()
	defer df.Release()

	tmpFile := "/tmp/test_writer_basic.ipc"
	defer os.Remove(tmpFile)

	if err := Write(df, tmpFile); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	info, err := os.Stat(tmpFile)
	if err != nil {
		t.Fatalf("File not created: %v", err)
	}
	if info.Size() == 0 {
		t.Error("File is empty")
	}
}

func TestWriterInt64(t *testing.T) {
	alloc := memory.DefaultAllocator
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", alloc, []int64{1, 2, 3}, nil))
	defer df.Release()

	tmpFile := "/tmp/test_writer_int64.ipc"
	defer os.Remove(tmpFile)

	if err := Write(df, tmpFile); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
}

func TestWriterFloat64(t *testing.T) {
	alloc := memory.DefaultAllocator
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("a", alloc, []float64{1.1, 2.2, 3.3}, nil))
	defer df.Release()

	tmpFile := "/tmp/test_writer_float64.ipc"
	defer os.Remove(tmpFile)

	if err := Write(df, tmpFile); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
}

func TestWriterString(t *testing.T) {
	alloc := memory.DefaultAllocator
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("a", alloc, []string{"hello", "world"}, nil))
	defer df.Release()

	tmpFile := "/tmp/test_writer_string.ipc"
	defer os.Remove(tmpFile)

	if err := Write(df, tmpFile); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
}

func TestWriterBoolean(t *testing.T) {
	alloc := memory.DefaultAllocator
	df := dataframe.New()
	df.AddSeries(series.NewBooleanSeries("a", alloc, []bool{true, false, true}, nil))
	defer df.Release()

	tmpFile := "/tmp/test_writer_bool.ipc"
	defer os.Remove(tmpFile)

	if err := Write(df, tmpFile); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
}

func TestWriterWithNulls(t *testing.T) {
	alloc := memory.DefaultAllocator
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", alloc, []int64{1, 0, 3}, []bool{true, false, true}))
	defer df.Release()

	tmpFile := "/tmp/test_writer_nulls.ipc"
	defer os.Remove(tmpFile)

	if err := Write(df, tmpFile); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
}

func TestWriterEmpty(t *testing.T) {
	df := dataframe.New()
	defer df.Release()

	tmpFile := "/tmp/test_writer_empty.ipc"
	defer os.Remove(tmpFile)

	if err := Write(df, tmpFile); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	_, err := os.Stat(tmpFile)
	if err != nil {
		t.Fatalf("File not created: %v", err)
	}
}

func TestWriterMultipleColumns(t *testing.T) {
	alloc := memory.DefaultAllocator
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("int", alloc, []int64{1, 2, 3}, nil))
	df.AddSeries(series.NewFloat64Series("float", alloc, []float64{1.1, 2.2, 3.3}, nil))
	df.AddSeries(series.NewStringSeries("str", alloc, []string{"a", "b", "c"}, nil))
	df.AddSeries(series.NewBooleanSeries("bool", alloc, []bool{true, false, true}, nil))
	defer df.Release()

	tmpFile := "/tmp/test_writer_multi.ipc"
	defer os.Remove(tmpFile)

	if err := Write(df, tmpFile); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
}

func createTestDataFrameForWriter() *dataframe.DataFrame {
	alloc := memory.DefaultAllocator

	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("int_col", alloc, []int64{1, 2, 3, 4, 5}, nil))
	df.AddSeries(series.NewFloat64Series("float_col", alloc, []float64{1.1, 2.2, 3.3, 4.4, 5.5}, nil))
	df.AddSeries(series.NewStringSeries("str_col", alloc, []string{"apple", "banana", "cherry", "date", "elderberry"}, nil))
	df.AddSeries(series.NewBooleanSeries("bool_col", alloc, []bool{true, false, true, false, true}, nil))

	return df
}
