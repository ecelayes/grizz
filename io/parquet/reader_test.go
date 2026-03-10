package parquet

import (
	"testing"
)

func TestNewReader(t *testing.T) {
	_, err := NewReader("nonexistent.parquet")
	if err == nil {
		t.Error("Expected error for nonexistent file")
	}
}

func TestReadFile(t *testing.T) {
	_, err := ReadFile("nonexistent.parquet")
	if err == nil {
		t.Error("Expected error for nonexistent file")
	}
}

func TestReadAll(t *testing.T) {
	_, err := ReadAll("nonexistent.parquet")
	if err == nil {
		t.Error("Expected error for nonexistent file")
	}
}

func TestReadToDataFrame(t *testing.T) {
	_, err := ReadToDataFrame("nonexistent.parquet")
	if err == nil {
		t.Error("Expected error for nonexistent file")
	}
}

func TestReadAllWithValidFile(t *testing.T) {
	parquetPath := createTestParquetFile(t)

	tbl, err := ReadAll(parquetPath)
	if err != nil {
		t.Fatalf("Failed to read parquet file: %v", err)
	}
	defer tbl.Release()

	if tbl.NumRows() != 5 {
		t.Errorf("Expected 5 rows, got %d", tbl.NumRows())
	}
	if tbl.NumCols() != 4 {
		t.Errorf("Expected 4 columns, got %d", tbl.NumCols())
	}
}

func TestReadToDataFrameWithValidFile(t *testing.T) {
	parquetPath := createTestParquetFile(t)

	tbl, err := ReadToDataFrame(parquetPath)
	if err != nil {
		t.Fatalf("Failed to read parquet file: %v", err)
	}
	defer tbl.Release()

	if tbl.NumRows() != 5 {
		t.Errorf("Expected 5 rows, got %d", tbl.NumRows())
	}
}

func TestNewReaderWithValidFile(t *testing.T) {
	parquetPath := createTestParquetFile(t)

	reader, err := NewReader(parquetPath)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	if reader.NumRows() != 5 {
		t.Errorf("Expected 5 rows, got %d", reader.NumRows())
	}
	if reader.NumRowGroups() != 1 {
		t.Errorf("Expected 1 row group, got %d", reader.NumRowGroups())
	}
}

func TestReadFileWithValidFile(t *testing.T) {
	parquetPath := createTestParquetFile(t)

	reader, err := ReadFile(parquetPath)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	if reader.NumRows() != 5 {
		t.Errorf("Expected 5 rows, got %d", reader.NumRows())
	}
}
