package arrow

import (
	"context"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	grizzmemory "github.com/ecelayes/grizz/internal/memory"
)

type ParquetReader = file.Reader
type ParquetFileReader = pqarrow.FileReader
type ParquetFileWriter = pqarrow.FileWriter
type ParquetArrowReadProps = pqarrow.ArrowReadProperties
type ParquetArrowWriteProps = pqarrow.ArrowWriterProperties
type ParquetWriterProperties = parquet.WriterProperties
type ParquetReaderProps = parquet.ReaderProperties

func OpenParquetFile(path string, memoryMap bool) (*ParquetReader, error) {
	return file.OpenParquetFile(path, memoryMap)
}

func NewParquetFileReader(r *ParquetReader, props ParquetArrowReadProps) (*ParquetFileReader, error) {
	return pqarrow.NewFileReader(r, props, grizzmemory.DefaultAllocator)
}

func NewParquetFileWriter(schema *arrow.Schema, w io.Writer, props *ParquetWriterProperties, arrowProps ParquetArrowWriteProps) (*ParquetFileWriter, error) {
	writerProps := parquet.NewWriterProperties()
	if props != nil {
		writerProps = props
	}
	return pqarrow.NewFileWriter(schema, w, writerProps, arrowProps)
}

var DefaultArrowReadProps = pqarrow.ArrowReadProperties{}

func DefaultWriterProps() ParquetArrowWriteProps {
	return pqarrow.DefaultWriterProps()
}

type ParquetTableReader struct {
	reader  *ParquetFileReader
	parquet *ParquetReader
	schema  *arrow.Schema
}

func OpenParquetTable(path string) (*ParquetTableReader, error) {
	parquetReader, err := file.OpenParquetFile(path, false)
	if err != nil {
		return nil, err
	}

	reader, err := pqarrow.NewFileReader(
		parquetReader,
		DefaultArrowReadProps,
		grizzmemory.DefaultAllocator,
	)
	if err != nil {
		parquetReader.Close()
		return nil, err
	}

	schema, err := reader.Schema()
	if err != nil {
		parquetReader.Close()
		return nil, err
	}

	return &ParquetTableReader{
		reader:  reader,
		parquet: parquetReader,
		schema:  schema,
	}, nil
}

func (r *ParquetTableReader) Schema() *arrow.Schema {
	return r.schema
}

func (r *ParquetTableReader) NumRows() int64 {
	return r.parquet.NumRows()
}

func (r *ParquetTableReader) NumRowGroups() int {
	return r.parquet.NumRowGroups()
}

func (r *ParquetTableReader) Read(ctx context.Context) (arrow.Table, error) {
	return r.reader.ReadTable(ctx)
}

func (r *ParquetTableReader) Close() error {
	return r.parquet.Close()
}

func ReadParquetTable(path string) (arrow.Table, error) {
	reader, err := OpenParquetTable(path)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return reader.Read(context.Background())
}

type ParquetTableWriter struct {
	writer *ParquetFileWriter
	schema *arrow.Schema
}

func NewParquetTableWriter(w io.Writer, schema *arrow.Schema) (*ParquetTableWriter, error) {
	writer, err := pqarrow.NewFileWriter(
		schema,
		w,
		parquet.NewWriterProperties(),
		pqarrow.DefaultWriterProps(),
	)
	if err != nil {
		return nil, err
	}

	return &ParquetTableWriter{
		writer: writer,
		schema: schema,
	}, nil
}

func (w *ParquetTableWriter) WriteTable(tbl arrow.Table) error {
	return w.writer.WriteTable(tbl, 1024)
}

func (w *ParquetTableWriter) Close() error {
	return w.writer.Close()
}

func WriteParquetTable(w io.Writer, tbl arrow.Table) error {
	writer, err := NewParquetTableWriter(w, tbl.Schema())
	if err != nil {
		return err
	}
	defer writer.Close()
	return writer.WriteTable(tbl)
}
