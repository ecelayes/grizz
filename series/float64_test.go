package series

import (
	"testing"

	"github.com/ecelayes/grizz/internal/memory"
)

func TestFloat64SeriesName(t *testing.T) {
	s := NewFloat64Series("test", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0}, nil)
	if s.Name() != "test" {
		t.Errorf("Expected test, got %s", s.Name())
	}
}

func TestFloat64SeriesLen(t *testing.T) {
	s := NewFloat64Series("test", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0}, nil)
	if s.Len() != 3 {
		t.Errorf("Expected 3, got %d", s.Len())
	}
}

func TestFloat64SeriesValue(t *testing.T) {
	s := NewFloat64Series("test", memory.DefaultAllocator, []float64{10.0, 20.0, 30.0}, nil)
	if s.Value(1) != 20.0 {
		t.Errorf("Expected 20.0, got %f", s.Value(1))
	}
}

func TestFloat64SeriesIsNull(t *testing.T) {
	s := NewFloat64Series("test", memory.DefaultAllocator, []float64{0.0, 20.0, 0.0}, []bool{true, false, true})
	if s.IsNull(0) {
		t.Error("Expected IsNull(0) = false (valid)")
	}
	if !s.IsNull(1) {
		t.Error("Expected IsNull(1) = true (invalid)")
	}
}

func TestFloat64SeriesSum(t *testing.T) {
	s := NewFloat64Series("test", memory.DefaultAllocator, []float64{10.0, 20.0, 30.0}, nil)
	if s.Sum() != 60.0 {
		t.Errorf("Expected 60.0, got %f", s.Sum())
	}
}

func TestFloat64SeriesMean(t *testing.T) {
	s := NewFloat64Series("test", memory.DefaultAllocator, []float64{10.0, 20.0, 30.0}, nil)
	if s.Mean() != 20.0 {
		t.Errorf("Expected 20.0, got %f", s.Mean())
	}
}

func TestFloat64SeriesMin(t *testing.T) {
	s := NewFloat64Series("test", memory.DefaultAllocator, []float64{30.0, 10.0, 20.0}, nil)
	if s.Min() != 10.0 {
		t.Errorf("Expected 10.0, got %f", s.Min())
	}
}

func TestFloat64SeriesMax(t *testing.T) {
	s := NewFloat64Series("test", memory.DefaultAllocator, []float64{30.0, 10.0, 20.0}, nil)
	if s.Max() != 30.0 {
		t.Errorf("Expected 30.0, got %f", s.Max())
	}
}

func TestFloat64SeriesCount(t *testing.T) {
	s := NewFloat64Series("test", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0}, nil)
	if s.Count() != 3 {
		t.Errorf("Expected 3, got %d", s.Count())
	}
}

func TestFloat64SeriesStd(t *testing.T) {
	s := NewFloat64Series("test", memory.DefaultAllocator, []float64{2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0}, nil)
	std := s.Std()
	if std < 2.0 || std > 2.5 {
		t.Errorf("Expected Std around 2.0, got %f", std)
	}
}

func TestFloat64SeriesVariance(t *testing.T) {
	s := NewFloat64Series("test", memory.DefaultAllocator, []float64{2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0}, nil)
	variance := s.Variance()
	if variance < 4.0 || variance > 5.0 {
		t.Errorf("Expected Variance around 4.5, got %f", variance)
	}
}

func TestFloat64SeriesMedian(t *testing.T) {
	s := NewFloat64Series("test", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0, 4.0, 5.0}, nil)
	if s.Median() != 3.0 {
		t.Errorf("Expected 3.0, got %f", s.Median())
	}
}

func TestFloat64SeriesQuantile(t *testing.T) {
	s := NewFloat64Series("test", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0, 4.0, 5.0}, nil)
	q := s.Quantile(0.5)
	if q < 2.5 || q > 3.5 {
		t.Errorf("Expected Quantile(0.5) around 3.0, got %f", q)
	}
}

func TestFloat64SeriesNUnique(t *testing.T) {
	s := NewFloat64Series("test", memory.DefaultAllocator, []float64{1.0, 2.0, 2.0, 3.0, 3.0, 3.0}, nil)
	if s.NUnique() != 3 {
		t.Errorf("Expected 3, got %d", s.NUnique())
	}
}

func TestFloat64SeriesFirst(t *testing.T) {
	s := NewFloat64Series("test", memory.DefaultAllocator, []float64{10.0, 20.0, 30.0}, nil)
	if s.First() != 10.0 {
		t.Errorf("Expected 10.0, got %f", s.First())
	}
}

func TestFloat64SeriesLast(t *testing.T) {
	s := NewFloat64Series("test", memory.DefaultAllocator, []float64{10.0, 20.0, 30.0}, nil)
	if s.Last() != 30.0 {
		t.Errorf("Expected 30.0, got %f", s.Last())
	}
}

func TestFloat64SeriesType(t *testing.T) {
	s := NewFloat64Series("test", memory.DefaultAllocator, []float64{1.0}, nil)
	if s.Type().Name() != "float64" {
		t.Errorf("Expected float64, got %s", s.Type().Name())
	}
}

func TestFloat64SeriesRelease(t *testing.T) {
	s := NewFloat64Series("test", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0}, nil)
	s.Release()
}
