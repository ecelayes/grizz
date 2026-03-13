package series

import (
	"testing"

	"github.com/ecelayes/grizz/internal/memory"
)

func TestUInt16SeriesName(t *testing.T) {
	s := NewUInt16Series("test", memory.DefaultAllocator, []uint16{1, 2, 3}, nil)
	if s.Name() != "test" {
		t.Errorf("Expected test, got %s", s.Name())
	}
}

func TestUInt16SeriesLen(t *testing.T) {
	s := NewUInt16Series("test", memory.DefaultAllocator, []uint16{1, 2, 3}, nil)
	if s.Len() != 3 {
		t.Errorf("Expected 3, got %d", s.Len())
	}
}

func TestUInt16SeriesValue(t *testing.T) {
	s := NewUInt16Series("test", memory.DefaultAllocator, []uint16{10, 20, 30}, nil)
	if s.Value(1) != 20 {
		t.Errorf("Expected 20, got %d", s.Value(1))
	}
}

func TestUInt16SeriesSum(t *testing.T) {
	s := NewUInt16Series("test", memory.DefaultAllocator, []uint16{10, 20, 30}, nil)
	if s.Sum() != 60 {
		t.Errorf("Expected 60, got %f", s.Sum())
	}
}

func TestUInt16SeriesMean(t *testing.T) {
	s := NewUInt16Series("test", memory.DefaultAllocator, []uint16{10, 20, 30}, nil)
	if s.Mean() != 20 {
		t.Errorf("Expected 20, got %f", s.Mean())
	}
}

func TestUInt16SeriesMin(t *testing.T) {
	s := NewUInt16Series("test", memory.DefaultAllocator, []uint16{30, 10, 20}, nil)
	if s.Min() != 10 {
		t.Errorf("Expected 10, got %d", s.Min())
	}
}

func TestUInt16SeriesMax(t *testing.T) {
	s := NewUInt16Series("test", memory.DefaultAllocator, []uint16{30, 10, 20}, nil)
	if s.Max() != 30 {
		t.Errorf("Expected 30, got %d", s.Max())
	}
}

func TestUInt16SeriesType(t *testing.T) {
	s := NewUInt16Series("test", memory.DefaultAllocator, []uint16{1}, nil)
	if s.Type().Name() != "uint16" {
		t.Errorf("Expected uint16, got %s", s.Type().Name())
	}
}

func TestUInt16SeriesRelease(t *testing.T) {
	s := NewUInt16Series("test", memory.DefaultAllocator, []uint16{1, 2, 3}, nil)
	s.Release()
}
