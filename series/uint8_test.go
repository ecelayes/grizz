package series

import (
	"testing"

	"github.com/ecelayes/grizz/internal/memory"
)

func TestUInt8SeriesName(t *testing.T) {
	s := NewUInt8Series("test", memory.DefaultAllocator, []uint8{1, 2, 3}, nil)
	if s.Name() != "test" {
		t.Errorf("Expected test, got %s", s.Name())
	}
}

func TestUInt8SeriesLen(t *testing.T) {
	s := NewUInt8Series("test", memory.DefaultAllocator, []uint8{1, 2, 3}, nil)
	if s.Len() != 3 {
		t.Errorf("Expected 3, got %d", s.Len())
	}
}

func TestUInt8SeriesValue(t *testing.T) {
	s := NewUInt8Series("test", memory.DefaultAllocator, []uint8{10, 20, 30}, nil)
	if s.Value(1) != 20 {
		t.Errorf("Expected 20, got %d", s.Value(1))
	}
}

func TestUInt8SeriesIsNull(t *testing.T) {
	s := NewUInt8Series("test", memory.DefaultAllocator, []uint8{0, 20, 0}, []bool{true, false, false})
	if s.IsNull(0) {
		t.Error("Expected IsNull(0) = false (valid value)")
	}
	if !s.IsNull(1) {
		t.Error("Expected IsNull(1) = true (invalid value)")
	}
}

func TestUInt8SeriesSum(t *testing.T) {
	s := NewUInt8Series("test", memory.DefaultAllocator, []uint8{10, 20, 30}, nil)
	if s.Sum() != 60 {
		t.Errorf("Expected 60, got %f", s.Sum())
	}
}

func TestUInt8SeriesMean(t *testing.T) {
	s := NewUInt8Series("test", memory.DefaultAllocator, []uint8{10, 20, 30}, nil)
	if s.Mean() != 20 {
		t.Errorf("Expected 20, got %f", s.Mean())
	}
}

func TestUInt8SeriesMin(t *testing.T) {
	s := NewUInt8Series("test", memory.DefaultAllocator, []uint8{30, 10, 20}, nil)
	if s.Min() != 10 {
		t.Errorf("Expected 10, got %d", s.Min())
	}
}

func TestUInt8SeriesMax(t *testing.T) {
	s := NewUInt8Series("test", memory.DefaultAllocator, []uint8{30, 10, 20}, nil)
	if s.Max() != 30 {
		t.Errorf("Expected 30, got %d", s.Max())
	}
}

func TestUInt8SeriesType(t *testing.T) {
	s := NewUInt8Series("test", memory.DefaultAllocator, []uint8{1}, nil)
	if s.Type().Name() != "uint8" {
		t.Errorf("Expected uint8, got %s", s.Type().Name())
	}
}

func TestUInt8SeriesRelease(t *testing.T) {
	s := NewUInt8Series("test", memory.DefaultAllocator, []uint8{1, 2, 3}, nil)
	s.Release()
}
