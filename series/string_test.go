package series

import (
	"testing"

	"github.com/ecelayes/grizz/internal/memory"
)

func TestStringSeriesUpper(t *testing.T) {
	s := NewStringSeries("name", memory.DefaultAllocator, []string{"hello", "world"}, nil)
	upper := s.Upper()
	if upper.Value(0) != "HELLO" {
		t.Errorf("Expected HELLO, got %s", upper.Value(0))
	}
	if upper.Value(1) != "WORLD" {
		t.Errorf("Expected WORLD, got %s", upper.Value(1))
	}
}

func TestStringSeriesLower(t *testing.T) {
	s := NewStringSeries("name", memory.DefaultAllocator, []string{"HELLO", "WORLD"}, nil)
	lower := s.Lower()
	if lower.Value(0) != "hello" {
		t.Errorf("Expected hello, got %s", lower.Value(0))
	}
	if lower.Value(1) != "world" {
		t.Errorf("Expected world, got %s", lower.Value(1))
	}
}

func TestStringSeriesContains(t *testing.T) {
	s := NewStringSeries("text", memory.DefaultAllocator, []string{"hello world", "foo bar", "baz"}, nil)

	contains := s.Contains("world")
	if !contains.Value(0) {
		t.Error("Expected true for first element containing 'world'")
	}
	if contains.Value(1) {
		t.Error("Expected false for second element not containing 'world'")
	}
	if contains.Value(2) {
		t.Error("Expected false for third element not containing 'world'")
	}
}

func TestStringSeriesReplace(t *testing.T) {
	s := NewStringSeries("text", memory.DefaultAllocator, []string{"hello world", "foo bar"}, nil)
	replaced := s.Replace("world", "go")
	if replaced.Value(0) != "hello go" {
		t.Errorf("Expected 'hello go', got %s", replaced.Value(0))
	}
	if replaced.Value(1) != "foo bar" {
		t.Errorf("Expected 'foo bar', got %s", replaced.Value(1))
	}
}

func TestStringSeriesLength(t *testing.T) {
	s := NewStringSeries("text", memory.DefaultAllocator, []string{"hello", "world", ""}, nil)
	lengths := s.Length()
	if lengths.Value(0) != 5 {
		t.Errorf("Expected 5, got %d", lengths.Value(0))
	}
	if lengths.Value(1) != 5 {
		t.Errorf("Expected 5, got %d", lengths.Value(1))
	}
	if lengths.Value(2) != 0 {
		t.Errorf("Expected 0, got %d", lengths.Value(2))
	}
}

func TestStringSeriesStrip(t *testing.T) {
	s := NewStringSeries("text", memory.DefaultAllocator, []string{"  hello  ", "\tworld\t", "  foo  "}, nil)
	stripped := s.Strip()
	if stripped.Value(0) != "hello" {
		t.Errorf("Expected 'hello', got '%s'", stripped.Value(0))
	}
	if stripped.Value(1) != "world" {
		t.Errorf("Expected 'world', got '%s'", stripped.Value(1))
	}
}

func TestStringSeriesContainsRegex(t *testing.T) {
	s := NewStringSeries("text", memory.DefaultAllocator, []string{"abc123", "def456", "xyz"}, nil)

	contains := s.ContainsRegex("\\d+")
	if !contains.Value(0) {
		t.Error("Expected true for first element containing digits")
	}
	if !contains.Value(1) {
		t.Error("Expected true for second element containing digits")
	}
	if contains.Value(2) {
		t.Error("Expected false for third element not containing digits")
	}
}
