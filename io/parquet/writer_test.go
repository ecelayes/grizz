package parquet

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestWriteFileToInvalidPath(t *testing.T) {
	err := WriteFile("/nonexistent/path/file.parquet", nil)
	if err == nil {
		t.Error("Expected error for invalid path")
	}
}

func createTestParquetFile(t *testing.T) string {
	t.Helper()

	pool := memory.DefaultAllocator

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	}, nil)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int64Builder).AppendValues(
		[]int64{1, 2, 3, 4, 5}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues(
		[]string{"Alice", "Bob", "Charlie", "Diana", "Eve"},
		[]bool{true, true, false, true, true})
	builder.Field(2).(*array.Float64Builder).AppendValues(
		[]float64{95.5, 87.3, 92.1, 88.0, 91.7},
		[]bool{true, true, true, false, true})
	builder.Field(3).(*array.BooleanBuilder).AppendValues(
		[]bool{true, true, false, true, true}, nil)

	rec := builder.NewRecordBatch()
	defer rec.Release()

	tbl := array.NewTableFromRecords(schema, []arrow.RecordBatch{rec})
	defer tbl.Release()

	tmpDir := t.TempDir()
	parquetPath := filepath.Join(tmpDir, "test.parquet")

	err := WriteFile(parquetPath, tbl)
	if err != nil {
		t.Fatalf("Failed to write parquet file: %v", err)
	}

	return parquetPath
}

func TestWriteFileWithValidTable(t *testing.T) {
	parquetPath := createTestParquetFile(t)

	_, err := os.Stat(parquetPath)
	if err != nil {
		t.Errorf("Expected parquet file to exist: %v", err)
	}
}

func TestWriteTableDirect(t *testing.T) {
	pool := memory.DefaultAllocator

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "value", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int32Builder).AppendValues(
		[]int32{10, 20, 30}, nil)

	rec := builder.NewRecordBatch()
	defer rec.Release()

	tbl := array.NewTableFromRecords(schema, []arrow.RecordBatch{rec})
	defer tbl.Release()

	tmpDir := t.TempDir()
	parquetPath := filepath.Join(tmpDir, "test_write_table.parquet")

	err := WriteFile(parquetPath, tbl)
	if err != nil {
		t.Fatalf("Failed to write parquet: %v", err)
	}

	readTable, err := ReadAll(parquetPath)
	if err != nil {
		t.Fatalf("Failed to read parquet: %v", err)
	}
	defer readTable.Release()

	if readTable.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", readTable.NumRows())
	}
}

func TestNewWriter(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	tmpDir := t.TempDir()
	parquetPath := filepath.Join(tmpDir, "new_writer_test.parquet")

	writer, err := NewWriter(parquetPath, schema)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}
	defer writer.Close()

	_, err = os.Stat(parquetPath)
	if err != nil {
		t.Errorf("Expected file to be created: %v", err)
	}
}

func TestNewWriterInvalidPath(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	_, err := NewWriter("/invalid/path/file.parquet", schema)
	if err == nil {
		t.Error("Expected error for invalid path")
	}
}

func TestWriteTableWithBuffer(t *testing.T) {
	pool := memory.DefaultAllocator

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues(
		[]string{"a", "b", "c"}, []bool{true, true, true})

	rec := builder.NewRecordBatch()
	defer rec.Release()

	tbl := array.NewTableFromRecords(schema, []arrow.RecordBatch{rec})
	defer tbl.Release()

	var buf bytes.Buffer
	err := WriteTable(&buf, tbl)
	if err != nil {
		t.Fatalf("Failed to write table: %v", err)
	}

	if buf.Len() == 0 {
		t.Error("Expected non-empty buffer")
	}
}
