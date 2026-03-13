package series

import (
	"testing"

	"github.com/ecelayes/grizz/internal/memory"
)

func TestInt64SeriesName(t *testing.T) {
	s := NewInt64Series("test", memory.DefaultAllocator, []int64{1, 2, 3}, nil)
	if s.Name() != "test" {
		t.Errorf("Expected test, got %s", s.Name())
	}
}

func TestInt64SeriesLen(t *testing.T) {
	s := NewInt64Series("test", memory.DefaultAllocator, []int64{1, 2, 3}, nil)
	if s.Len() != 3 {
		t.Errorf("Expected 3, got %d", s.Len())
	}
}

func TestInt64SeriesValue(t *testing.T) {
	s := NewInt64Series("test", memory.DefaultAllocator, []int64{10, 20, 30}, nil)
	if s.Value(1) != 20 {
		t.Errorf("Expected 20, got %d", s.Value(1))
	}
}

func TestInt64SeriesIsNull(t *testing.T) {
	s := NewInt64Series("test", memory.DefaultAllocator, []int64{0, 20, 0}, []bool{true, false, true})
	if s.IsNull(0) {
		t.Error("Expected IsNull(0) = false (valid)")
	}
	if !s.IsNull(1) {
		t.Error("Expected IsNull(1) = true (invalid)")
	}
}

func TestInt64SeriesSum(t *testing.T) {
	s := NewInt64Series("test", memory.DefaultAllocator, []int64{10, 20, 30}, nil)
	if s.Sum() != 60 {
		t.Errorf("Expected 60, got %f", s.Sum())
	}
}

func TestInt64SeriesMean(t *testing.T) {
	s := NewInt64Series("test", memory.DefaultAllocator, []int64{10, 20, 30}, nil)
	if s.Mean() != 20 {
		t.Errorf("Expected 20, got %f", s.Mean())
	}
}

func TestInt64SeriesMin(t *testing.T) {
	s := NewInt64Series("test", memory.DefaultAllocator, []int64{30, 10, 20}, nil)
	if s.Min() != 10 {
		t.Errorf("Expected 10, got %d", s.Min())
	}
}

func TestInt64SeriesMax(t *testing.T) {
	s := NewInt64Series("test", memory.DefaultAllocator, []int64{30, 10, 20}, nil)
	if s.Max() != 30 {
		t.Errorf("Expected 30, got %d", s.Max())
	}
}

func TestInt64SeriesCount(t *testing.T) {
	s := NewInt64Series("test", memory.DefaultAllocator, []int64{1, 2, 3}, nil)
	if s.Count() != 3 {
		t.Errorf("Expected 3, got %d", s.Count())
	}
}

func TestInt64SeriesStd(t *testing.T) {
	s := NewInt64Series("test", memory.DefaultAllocator, []int64{2, 4, 4, 4, 5, 5, 7, 9}, nil)
	std := s.Std()
	if std < 2.0 || std > 2.5 {
		t.Errorf("Expected Std around 2.0, got %f", std)
	}
}

func TestInt64SeriesVariance(t *testing.T) {
	s := NewInt64Series("test", memory.DefaultAllocator, []int64{2, 4, 4, 4, 5, 5, 7, 9}, nil)
	variance := s.Variance()
	if variance < 4.0 || variance > 5.5 {
		t.Errorf("Expected Variance around 4.5, got %f", variance)
	}
}

func TestInt64SeriesMedian(t *testing.T) {
	s := NewInt64Series("test", memory.DefaultAllocator, []int64{1, 2, 3, 4, 5}, nil)
	if s.Median() != 3 {
		t.Errorf("Expected 3, got %f", s.Median())
	}
}

func TestInt64SeriesQuantile(t *testing.T) {
	s := NewInt64Series("test", memory.DefaultAllocator, []int64{1, 2, 3, 4, 5}, nil)
	q := s.Quantile(0.5)
	if q < 2.5 || q > 3.5 {
		t.Errorf("Expected Quantile(0.5) around 3, got %f", q)
	}
}

func TestInt64SeriesNUnique(t *testing.T) {
	s := NewInt64Series("test", memory.DefaultAllocator, []int64{1, 2, 2, 3, 3, 3}, nil)
	if s.NUnique() != 3 {
		t.Errorf("Expected 3, got %d", s.NUnique())
	}
}

func TestInt64SeriesFirst(t *testing.T) {
	s := NewInt64Series("test", memory.DefaultAllocator, []int64{10, 20, 30}, nil)
	if s.First() != 10 {
		t.Errorf("Expected 10, got %d", s.First())
	}
}

func TestInt64SeriesLast(t *testing.T) {
	s := NewInt64Series("test", memory.DefaultAllocator, []int64{10, 20, 30}, nil)
	if s.Last() != 30 {
		t.Errorf("Expected 30, got %d", s.Last())
	}
}

func TestInt64SeriesType(t *testing.T) {
	s := NewInt64Series("test", memory.DefaultAllocator, []int64{1}, nil)
	if s.Type().Name() != "int64" {
		t.Errorf("Expected int64, got %s", s.Type().Name())
	}
}

func TestInt64SeriesRelease(t *testing.T) {
	s := NewInt64Series("test", memory.DefaultAllocator, []int64{1, 2, 3}, nil)
	s.Release()
}
