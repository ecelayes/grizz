package parquet

import (
	"github.com/apache/arrow-go/v18/arrow"

	grizzarrows "github.com/ecelayes/grizz/internal/arrow"
)

type Reader = grizzarrows.ParquetTableReader

func NewReader(path string) (*Reader, error) {
	return grizzarrows.OpenParquetTable(path)
}

func ReadFile(path string) (*Reader, error) {
	return grizzarrows.OpenParquetTable(path)
}

func ReadAll(path string) (arrow.Table, error) {
	return grizzarrows.ReadParquetTable(path)
}

func ReadToDataFrame(path string) (arrow.Table, error) {
	return grizzarrows.ReadParquetTable(path)
}
