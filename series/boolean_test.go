package series

import (
	"testing"

	"github.com/ecelayes/grizz/internal/memory"
)

func TestBooleanSeriesAny(t *testing.T) {
	s := NewBooleanSeries("active", memory.DefaultAllocator, []bool{false, false, true}, nil)
	any := s.Any()
	if !any {
		t.Error("Expected true when any value is true")
	}
}

func TestBooleanSeriesAnyAllFalse(t *testing.T) {
	s := NewBooleanSeries("active", memory.DefaultAllocator, []bool{false, false, false}, nil)
	any := s.Any()
	if any {
		t.Error("Expected false when all values are false")
	}
}

func TestBooleanSeriesAnyEmpty(t *testing.T) {
	s := NewBooleanSeries("active", memory.DefaultAllocator, []bool{}, nil)
	any := s.Any()
	if any {
		t.Error("Expected false for empty series")
	}
}

func TestBooleanSeriesAll(t *testing.T) {
	s := NewBooleanSeries("active", memory.DefaultAllocator, []bool{true, true, true}, nil)
	all := s.All()
	if !all {
		t.Error("Expected true when all values are true")
	}
}

func TestBooleanSeriesAllSomeFalse(t *testing.T) {
	s := NewBooleanSeries("active", memory.DefaultAllocator, []bool{true, false, true}, nil)
	all := s.All()
	if all {
		t.Error("Expected false when any value is false")
	}
}

func TestBooleanSeriesAllEmpty(t *testing.T) {
	s := NewBooleanSeries("active", memory.DefaultAllocator, []bool{}, nil)
	all := s.All()
	if all {
		t.Error("Expected false for empty series")
	}
}
