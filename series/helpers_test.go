package series

import (
	"testing"

	"github.com/ecelayes/grizz/internal/memory"
)

func TestHelpersClone(t *testing.T) {
	s := NewInt64Series("test", memory.DefaultAllocator, []int64{1, 2, 3}, nil)
	cloned := Clone(s)

	if cloned.Name() != s.Name() {
		t.Errorf("Expected name %s, got %s", s.Name(), cloned.Name())
	}
	if cloned.Len() != s.Len() {
		t.Errorf("Expected len %d, got %d", s.Len(), cloned.Len())
	}
}

func TestHelpersTypeName(t *testing.T) {
	s := NewInt64Series("test", memory.DefaultAllocator, []int64{1}, nil)
	if TypeName(s) != "int64" {
		t.Errorf("Expected int64, got %s", TypeName(s))
	}
}

func TestHelpersIsNumeric(t *testing.T) {
	ints := NewInt64Series("test", memory.DefaultAllocator, []int64{1}, nil)
	float := NewFloat64Series("test", memory.DefaultAllocator, []float64{1}, nil)
	str := NewStringSeries("test", memory.DefaultAllocator, []string{"a"}, nil)
	bool := NewBooleanSeries("test", memory.DefaultAllocator, []bool{true}, nil)

	if !IsNumeric(ints) {
		t.Error("Expected Int64Series to be numeric")
	}
	if !IsNumeric(float) {
		t.Error("Expected Float64Series to be numeric")
	}
	if IsNumeric(str) {
		t.Error("Expected StringSeries to not be numeric")
	}
	if IsNumeric(bool) {
		t.Error("Expected BooleanSeries to not be numeric")
	}
}

func TestHelpersIsString(t *testing.T) {
	str := NewStringSeries("test", memory.DefaultAllocator, []string{"a"}, nil)
	ints := NewInt64Series("test", memory.DefaultAllocator, []int64{1}, nil)

	if !IsString(str) {
		t.Error("Expected StringSeries to be string")
	}
	if IsString(ints) {
		t.Error("Expected Int64Series to not be string")
	}
}

func TestHelpersIsBoolean(t *testing.T) {
	bool := NewBooleanSeries("test", memory.DefaultAllocator, []bool{true}, nil)
	str := NewStringSeries("test", memory.DefaultAllocator, []string{"a"}, nil)

	if !IsBoolean(bool) {
		t.Error("Expected BooleanSeries to be boolean")
	}
	if IsBoolean(str) {
		t.Error("Expected StringSeries to not be boolean")
	}
}

func TestHelpersValidateSeries(t *testing.T) {
	s := NewInt64Series("test", memory.DefaultAllocator, []int64{1, 2, 3}, nil)

	err := ValidateSeries(s)
	if err != nil {
		t.Errorf("Expected nil error, got %v", err)
	}

	err = ValidateSeries(nil)
	if err == nil {
		t.Error("Expected error for nil series")
	}
}

func TestHelpersValidateSeriesMatch(t *testing.T) {
	a := NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil)
	b := NewInt64Series("b", memory.DefaultAllocator, []int64{1, 2, 3}, nil)
	c := NewInt64Series("c", memory.DefaultAllocator, []int64{1, 2}, nil)
	d := NewInt64Series("d", memory.DefaultAllocator, []int64{1, 2, 3}, nil)

	err := ValidateSeriesMatch(a, b)
	if err != nil {
		t.Errorf("Expected nil error, got %v", err)
	}

	err = ValidateSeriesMatch(a, c)
	if err == nil {
		t.Error("Expected error for mismatched lengths")
	}

	err = ValidateSeriesMatch(a, d)
	if err != nil {
		t.Errorf("Expected nil error for same length, got %v", err)
	}
}
