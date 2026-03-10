package series

import (
	"testing"

	"github.com/ecelayes/grizz/internal/memory"
)

func TestInt64SeriesName(t *testing.T) {
	s := NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil)
	if s.Name() != "age" {
		t.Errorf("Expected age, got %s", s.Name())
	}
}

func TestInt64SeriesLen(t *testing.T) {
	s := NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil)
	if s.Len() != 3 {
		t.Errorf("Expected 3, got %d", s.Len())
	}
}

func TestInt64SeriesValue(t *testing.T) {
	s := NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30}, nil)
	if s.Value(1) != 20 {
		t.Errorf("Expected 20, got %d", s.Value(1))
	}
}

func TestInt64SeriesIsNull(t *testing.T) {
	s := NewInt64Series("age", memory.DefaultAllocator, []int64{0, 20, 0}, []bool{true, false, false})
	if s.IsNull(0) != false {
		t.Error("Expected false for IsNull(0)")
	}
	if s.IsNull(1) != true {
		t.Error("Expected true for IsNull(1)")
	}
	if s.IsNull(2) != true {
		t.Error("Expected true for IsNull(2)")
	}
}

func TestInt64SeriesSum(t *testing.T) {
	s := NewInt64Series("values", memory.DefaultAllocator, []int64{10, 20, 30}, nil)
	sum := s.Sum()
	if sum != 60 {
		t.Errorf("Expected 60, got %f", sum)
	}
}

func TestInt64SeriesMean(t *testing.T) {
	s := NewInt64Series("values", memory.DefaultAllocator, []int64{10, 20, 30}, nil)
	mean := s.Mean()
	if mean != 20 {
		t.Errorf("Expected 20, got %f", mean)
	}
}

func TestInt64SeriesMin(t *testing.T) {
	s := NewInt64Series("values", memory.DefaultAllocator, []int64{30, 10, 20}, nil)
	min := s.Min()
	if min != 10 {
		t.Errorf("Expected 10, got %d", min)
	}
}

func TestInt64SeriesMax(t *testing.T) {
	s := NewInt64Series("values", memory.DefaultAllocator, []int64{30, 10, 20}, nil)
	max := s.Max()
	if max != 30 {
		t.Errorf("Expected 30, got %d", max)
	}
}

func TestInt64SeriesCount(t *testing.T) {
	s := NewInt64Series("values", memory.DefaultAllocator, []int64{10, 20, 30}, []bool{true, false, true})
	count := s.Count()
	if count != 2 {
		t.Errorf("Expected 2, got %d", count)
	}
}

func TestFloat64SeriesName(t *testing.T) {
	s := NewFloat64Series("score", memory.DefaultAllocator, []float64{1.5, 2.5, 3.5}, nil)
	if s.Name() != "score" {
		t.Errorf("Expected score, got %s", s.Name())
	}
}

func TestFloat64SeriesLen(t *testing.T) {
	s := NewFloat64Series("score", memory.DefaultAllocator, []float64{1.5, 2.5, 3.5}, nil)
	if s.Len() != 3 {
		t.Errorf("Expected 3, got %d", s.Len())
	}
}

func TestFloat64SeriesValue(t *testing.T) {
	s := NewFloat64Series("score", memory.DefaultAllocator, []float64{1.5, 2.5, 3.5}, nil)
	if s.Value(1) != 2.5 {
		t.Errorf("Expected 2.5, got %f", s.Value(1))
	}
}

func TestFloat64SeriesSum(t *testing.T) {
	s := NewFloat64Series("values", memory.DefaultAllocator, []float64{10.5, 20.5, 30.0}, nil)
	sum := s.Sum()
	if sum != 61.0 {
		t.Errorf("Expected 61.0, got %f", sum)
	}
}

func TestFloat64SeriesMean(t *testing.T) {
	s := NewFloat64Series("values", memory.DefaultAllocator, []float64{10.0, 20.0, 30.0}, nil)
	mean := s.Mean()
	if mean != 20.0 {
		t.Errorf("Expected 20.0, got %f", mean)
	}
}

func TestFloat64SeriesMin(t *testing.T) {
	s := NewFloat64Series("values", memory.DefaultAllocator, []float64{30.0, 10.5, 20.0}, nil)
	min := s.Min()
	if min != 10.5 {
		t.Errorf("Expected 10.5, got %f", min)
	}
}

func TestFloat64SeriesMax(t *testing.T) {
	s := NewFloat64Series("values", memory.DefaultAllocator, []float64{30.0, 10.5, 20.0}, nil)
	max := s.Max()
	if max != 30.0 {
		t.Errorf("Expected 30.0, got %f", max)
	}
}

func TestFloat64SeriesCount(t *testing.T) {
	s := NewFloat64Series("values", memory.DefaultAllocator, []float64{10.0, 20.0, 30.0}, []bool{true, false, true})
	count := s.Count()
	if count != 2 {
		t.Errorf("Expected 2, got %d", count)
	}
}

func TestStringSeriesName(t *testing.T) {
	s := NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob"}, nil)
	if s.Name() != "name" {
		t.Errorf("Expected name, got %s", s.Name())
	}
}

func TestStringSeriesLen(t *testing.T) {
	s := NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob"}, nil)
	if s.Len() != 2 {
		t.Errorf("Expected 2, got %d", s.Len())
	}
}

func TestStringSeriesValue(t *testing.T) {
	s := NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob"}, nil)
	if s.Value(1) != "Bob" {
		t.Errorf("Expected Bob, got %s", s.Value(1))
	}
}

func TestStringSeriesIsNull(t *testing.T) {
	s := NewStringSeries("name", memory.DefaultAllocator, []string{"", "Bob"}, nil)
	if s.IsNull(0) != false {
		t.Error("Expected false for IsNull(0) with empty string")
	}
	if s.IsNull(1) != false {
		t.Error("Expected false for IsNull(1)")
	}
}

func TestBooleanSeriesName(t *testing.T) {
	s := NewBooleanSeries("active", memory.DefaultAllocator, []bool{true, false}, nil)
	if s.Name() != "active" {
		t.Errorf("Expected active, got %s", s.Name())
	}
}

func TestBooleanSeriesLen(t *testing.T) {
	s := NewBooleanSeries("active", memory.DefaultAllocator, []bool{true, false}, nil)
	if s.Len() != 2 {
		t.Errorf("Expected 2, got %d", s.Len())
	}
}

func TestBooleanSeriesValue(t *testing.T) {
	s := NewBooleanSeries("active", memory.DefaultAllocator, []bool{true, false}, nil)
	if s.Value(1) != false {
		t.Errorf("Expected false, got %v", s.Value(1))
	}
}

func TestBooleanSeriesIsNull(t *testing.T) {
	s := NewBooleanSeries("active", memory.DefaultAllocator, []bool{true, false}, nil)
	if s.IsNull(0) != false {
		t.Error("Expected false for IsNull(0)")
	}
	if s.IsNull(1) != false {
		t.Error("Expected false for IsNull(1)")
	}
}

func TestSeriesType(t *testing.T) {
	ints := NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil)
	if ints.Type() == nil {
		t.Error("Expected non-nil Type() for Int64Series")
	}

	floats := NewFloat64Series("score", memory.DefaultAllocator, []float64{1.0, 2.0}, nil)
	if floats.Type() == nil {
		t.Error("Expected non-nil Type() for Float64Series")
	}

	strings := NewStringSeries("name", memory.DefaultAllocator, []string{"a", "b"}, nil)
	if strings.Type() == nil {
		t.Error("Expected non-nil Type() for StringSeries")
	}

	bools := NewBooleanSeries("active", memory.DefaultAllocator, []bool{true, false}, nil)
	if bools.Type() == nil {
		t.Error("Expected non-nil Type() for BooleanSeries")
	}
}

func TestSeriesRelease(t *testing.T) {
	s := NewInt64Series("age", memory.DefaultAllocator, []int64{1, 2, 3}, nil)
	s.Release()
}

func TestFloat64SeriesRelease(t *testing.T) {
	s := NewFloat64Series("score", memory.DefaultAllocator, []float64{1.0, 2.0}, nil)
	s.Release()
}

func TestStringSeriesRelease(t *testing.T) {
	s := NewStringSeries("name", memory.DefaultAllocator, []string{"a", "b"}, nil)
	s.Release()
}

func TestBooleanSeriesRelease(t *testing.T) {
	s := NewBooleanSeries("active", memory.DefaultAllocator, []bool{true, false}, nil)
	s.Release()
}

func TestMeanEmptySeries(t *testing.T) {
	s := NewInt64Series("values", memory.DefaultAllocator, []int64{}, nil)
	mean := s.Mean()
	if mean != 0 {
		t.Errorf("Expected 0 for empty series, got %f", mean)
	}
}

func TestFloat64MeanEmptySeries(t *testing.T) {
	s := NewFloat64Series("values", memory.DefaultAllocator, []float64{}, nil)
	mean := s.Mean()
	if mean != 0 {
		t.Errorf("Expected 0 for empty series, got %f", mean)
	}
}
