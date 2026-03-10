package json

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

	tmpFile, err := os.CreateTemp("", "write*.json")
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
}

func TestWriteEmptyDataFrame(t *testing.T) {
	df := dataframe.New()

	tmpFile, err := os.CreateTemp("", "empty*.json")
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

	tmpFile, err := os.CreateTemp("", "types*.json")
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

func TestWriteWithNullValues(t *testing.T) {
	valid := []bool{true, false, true}
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob", "Charlie"}, valid))
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{30, 0, 25}, valid))

	tmpFile, err := os.CreateTemp("", "nulls*.json")
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

	col, _ := df2.Col(0)
	if col.IsNull(1) != true {
		t.Errorf("Expected null at row 1 for name column")
	}
}

func TestWriteToInvalidPath(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice"}, nil))

	err := Write(df, "/nonexistent/path/file.json")
	if err == nil {
		t.Error("Expected error for invalid path")
	}
}

func TestWriteAllTypes(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob"}, nil))
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{30, 25}, nil))
	df.AddSeries(series.NewFloat64Series("score", memory.DefaultAllocator, []float64{95.5, 87.3}, nil))
	df.AddSeries(series.NewBooleanSeries("active", memory.DefaultAllocator, []bool{true, false}, nil))

	tmpFile, err := os.CreateTemp("", "alltypes*.json")
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
	if df2.NumCols() != 4 {
		t.Errorf("Expected 4 columns, got %d", df2.NumCols())
	}
}
