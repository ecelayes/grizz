package dataframe

import (
	"testing"

	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestDescribeNumeric(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3, 4, 5}, nil))
	df.AddSeries(series.NewFloat64Series("b", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0, 4.0, 5.0}, nil))

	result := df.Describe()

	if result.NumRows() != 8 {
		t.Errorf("Expected 8 rows, got %d", result.NumRows())
	}
	if result.NumCols() != 3 {
		t.Errorf("Expected 3 cols, got %d", result.NumCols())
	}
}

func TestDescribeWithString(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	df.AddSeries(series.NewStringSeries("b", memory.DefaultAllocator, []string{"x", "y", "z"}, nil))

	result := df.Describe()

	if result.NumRows() != 8 {
		t.Errorf("Expected 8 rows, got %d", result.NumRows())
	}
}

func TestDescribeWithBoolean(t *testing.T) {
	df := New()
	df.AddSeries(series.NewBooleanSeries("flag", memory.DefaultAllocator, []bool{true, false, true}, nil))

	result := df.Describe()

	if result.NumRows() != 8 {
		t.Errorf("Expected 8 rows, got %d", result.NumRows())
	}
}

func TestDescribeEmpty(t *testing.T) {
	df := New()
	result := df.Describe()

	if result.NumRows() != 8 {
		t.Errorf("Expected 8 rows (stats labels), got %d", result.NumRows())
	}
}

func TestSchema(t *testing.T) {
	df := New()
	s1 := series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil)
	s2 := series.NewFloat64Series("b", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0}, nil)
	s3 := series.NewStringSeries("c", memory.DefaultAllocator, []string{"x", "y", "z"}, nil)

	err1 := df.AddSeries(s1)
	err2 := df.AddSeries(s2)
	err3 := df.AddSeries(s3)

	if err1 != nil || err2 != nil || err3 != nil {
		t.Errorf("AddSeries failed: %v, %v, %v", err1, err2, err3)
	}

	t.Logf("NumCols: %d, NumRows: %d", df.NumCols(), df.NumRows())

	schema := df.Schema()
	dtypes := df.Dtypes()

	t.Logf("Schema: %+v", schema)
	t.Logf("Dtypes: %+v", dtypes)

	if schema["a"] != "int64" {
		t.Errorf("Expected a=int64, got %s", schema["a"])
	}
	if schema["b"] != "float64" {
		t.Errorf("Expected b=float64, got %s", schema["b"])
	}
	if schema["c"] != "utf8" {
		t.Errorf("Expected c=utf8, got %s", schema["c"])
	}
}

func TestInfo(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	df.AddSeries(series.NewFloat64Series("b", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0}, nil))

	info := df.Info()

	if len(info) == 0 {
		t.Error("Expected non-empty info string")
	}
}
