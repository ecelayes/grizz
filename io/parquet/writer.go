package parquet

import (
	"io"
	"os"

	"github.com/apache/arrow-go/v18/arrow"

	grizzarrows "github.com/ecelayes/grizz/internal/arrow"
)

type Writer = grizzarrows.ParquetTableWriter

func NewWriter(outputPath string, schema *arrow.Schema) (*Writer, error) {
	file, err := os.Create(outputPath)
	if err != nil {
		return nil, err
	}
	return grizzarrows.NewParquetTableWriter(file, schema)
}

func WriteFile(path string, tbl arrow.Table) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return grizzarrows.WriteParquetTable(file, tbl)
}

func WriteTable(w io.Writer, tbl arrow.Table) error {
	return grizzarrows.WriteParquetTable(w, tbl)
}
