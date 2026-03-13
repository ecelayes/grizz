package series

import (
	"testing"

	"github.com/ecelayes/grizz/internal/memory"
)

func TestInt8SeriesName(t *testing.T) {
	s := NewInt8Series("test", memory.DefaultAllocator, []int8{1, 2, 3}, nil)
	if s.Name() != "test" {
		t.Errorf("Expected test, got %s", s.Name())
	}
}

func TestInt8SeriesLen(t *testing.T) {
	s := NewInt8Series("test", memory.DefaultAllocator, []int8{1, 2, 3}, nil)
	if s.Len() != 3 {
		t.Errorf("Expected 3, got %d", s.Len())
	}
}

func TestInt8SeriesValue(t *testing.T) {
	s := NewInt8Series("test", memory.DefaultAllocator, []int8{-10, 20, 30}, nil)
	if s.Value(1) != 20 {
		t.Errorf("Expected 20, got %d", s.Value(1))
	}
}

func TestInt8SeriesSum(t *testing.T) {
	s := NewInt8Series("test", memory.DefaultAllocator, []int8{10, 20, 30}, nil)
	if s.Sum() != 60 {
		t.Errorf("Expected 60, got %f", s.Sum())
	}
}

func TestInt8SeriesMean(t *testing.T) {
	s := NewInt8Series("test", memory.DefaultAllocator, []int8{10, 20, 30}, nil)
	if s.Mean() != 20 {
		t.Errorf("Expected 20, got %f", s.Mean())
	}
}

func TestInt8SeriesMin(t *testing.T) {
	s := NewInt8Series("test", memory.DefaultAllocator, []int8{30, -10, 20}, nil)
	if s.Min() != -10 {
		t.Errorf("Expected -10, got %d", s.Min())
	}
}

func TestInt8SeriesMax(t *testing.T) {
	s := NewInt8Series("test", memory.DefaultAllocator, []int8{30, -10, 20}, nil)
	if s.Max() != 30 {
		t.Errorf("Expected 30, got %d", s.Max())
	}
}

func TestInt8SeriesType(t *testing.T) {
	s := NewInt8Series("test", memory.DefaultAllocator, []int8{1}, nil)
	if s.Type().Name() != "int8" {
		t.Errorf("Expected int8, got %s", s.Type().Name())
	}
}

func TestInt8SeriesRelease(t *testing.T) {
	s := NewInt8Series("test", memory.DefaultAllocator, []int8{1, 2, 3}, nil)
	s.Release()
}
