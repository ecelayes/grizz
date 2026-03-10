package csv

import (
	"os"
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestWrite(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob"}, nil))
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{30, 25}, nil))

	tmpFile, err := os.CreateTemp("", "write*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	err = Write(df, tmpFile.Name())
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	df2, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read back failed: %v", err)
	}

	if df2.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", df2.NumRows())
	}

	if df2.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", df2.NumCols())
	}
}

func TestWriteEmptyDataFrame(t *testing.T) {
	df := dataframe.New()

	tmpFile, err := os.CreateTemp("", "empty*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	err = Write(df, tmpFile.Name())
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
}

func TestWriteWithNulls(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "", "Bob"}, nil))
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{30, 0, 25}, []bool{true, false, true}))

	tmpFile, err := os.CreateTemp("", "nulls*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	err = Write(df, tmpFile.Name())
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
}

func TestWriteMultipleTypes(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice"}, nil))
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{30}, nil))
	df.AddSeries(series.NewFloat64Series("score", memory.DefaultAllocator, []float64{95.5}, nil))
	df.AddSeries(series.NewBooleanSeries("active", memory.DefaultAllocator, []bool{true}, nil))

	tmpFile, err := os.CreateTemp("", "types*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	err = Write(df, tmpFile.Name())
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
}

func TestWriteToInvalidPath(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice"}, nil))

	err := Write(df, "/nonexistent/path/file.csv")
	if err == nil {
		t.Error("Expected error for invalid path")
	}
}

func TestWriteWithSeriesNulls(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob", "Charlie"}, []bool{true, false, true}))
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{30, 25, 35}, []bool{false, true, true}))

	tmpFile, err := os.CreateTemp("", "seriesnulls*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	err = Write(df, tmpFile.Name())
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	df2, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read back failed: %v", err)
	}

	if df2.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", df2.NumRows())
	}
}
