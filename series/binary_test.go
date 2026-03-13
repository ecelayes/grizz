package series

import (
	"testing"

	"github.com/ecelayes/grizz/internal/memory"
)

func TestBinarySeriesName(t *testing.T) {
	s := NewBinarySeries("test", memory.DefaultAllocator, [][]byte{[]byte("abc")}, nil)
	if s.Name() != "test" {
		t.Errorf("Expected test, got %s", s.Name())
	}
}

func TestBinarySeriesLen(t *testing.T) {
	s := NewBinarySeries("test", memory.DefaultAllocator, [][]byte{[]byte("abc"), []byte("def")}, nil)
	if s.Len() != 2 {
		t.Errorf("Expected 2, got %d", s.Len())
	}
}

func TestBinarySeriesValue(t *testing.T) {
	s := NewBinarySeries("test", memory.DefaultAllocator, [][]byte{[]byte("abc"), []byte("def")}, nil)
	if string(s.Value(1)) != "def" {
		t.Errorf("Expected def, got %s", string(s.Value(1)))
	}
}

func TestBinarySeriesIsNull(t *testing.T) {
	s := NewBinarySeries("test", memory.DefaultAllocator, [][]byte{[]byte("abc"), []byte("def")}, []bool{false, true})
	if !s.IsNull(0) {
		t.Error("Expected IsNull(0) = true (invalid value)")
	}
	if s.IsNull(1) {
		t.Error("Expected IsNull(1) = false (valid value)")
	}
}

func TestBinarySeriesType(t *testing.T) {
	s := NewBinarySeries("test", memory.DefaultAllocator, [][]byte{[]byte("a")}, nil)
	if s.Type().Name() != "binary" {
		t.Errorf("Expected binary, got %s", s.Type().Name())
	}
}

func TestBinarySeriesRelease(t *testing.T) {
	s := NewBinarySeries("test", memory.DefaultAllocator, [][]byte{[]byte("abc")}, nil)
	s.Release()
}
