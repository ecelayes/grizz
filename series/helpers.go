package series

import (
	"fmt"

	grizzmemory "github.com/ecelayes/grizz/internal/memory"
)

func Clone(s Series) Series {
	switch c := s.(type) {
	case *Int64Series:
		values := make([]int64, c.Len())
		valid := make([]bool, c.Len())
		for i := 0; i < c.Len(); i++ {
			values[i] = c.Value(i)
			valid[i] = !c.IsNull(i)
		}
		return NewInt64Series(c.Name(), grizzmemory.DefaultAllocator, values, valid)
	case *Float64Series:
		values := make([]float64, c.Len())
		valid := make([]bool, c.Len())
		for i := 0; i < c.Len(); i++ {
			values[i] = c.Value(i)
			valid[i] = !c.IsNull(i)
		}
		return NewFloat64Series(c.Name(), grizzmemory.DefaultAllocator, values, valid)
	case *StringSeries:
		values := make([]string, c.Len())
		valid := make([]bool, c.Len())
		for i := 0; i < c.Len(); i++ {
			values[i] = c.Value(i)
			valid[i] = !c.IsNull(i)
		}
		return NewStringSeries(c.Name(), grizzmemory.DefaultAllocator, values, valid)
	case *BooleanSeries:
		values := make([]bool, c.Len())
		valid := make([]bool, c.Len())
		for i := 0; i < c.Len(); i++ {
			values[i] = c.Value(i)
			valid[i] = !c.IsNull(i)
		}
		return NewBooleanSeries(c.Name(), grizzmemory.DefaultAllocator, values, valid)
	case *UInt8Series:
		values := make([]uint8, c.Len())
		valid := make([]bool, c.Len())
		for i := 0; i < c.Len(); i++ {
			values[i] = c.Value(i)
			valid[i] = !c.IsNull(i)
		}
		return NewUInt8Series(c.Name(), grizzmemory.DefaultAllocator, values, valid)
	case *UInt16Series:
		values := make([]uint16, c.Len())
		valid := make([]bool, c.Len())
		for i := 0; i < c.Len(); i++ {
			values[i] = c.Value(i)
			valid[i] = !c.IsNull(i)
		}
		return NewUInt16Series(c.Name(), grizzmemory.DefaultAllocator, values, valid)
	case *UInt32Series:
		values := make([]uint32, c.Len())
		valid := make([]bool, c.Len())
		for i := 0; i < c.Len(); i++ {
			values[i] = c.Value(i)
			valid[i] = !c.IsNull(i)
		}
		return NewUInt32Series(c.Name(), grizzmemory.DefaultAllocator, values, valid)
	case *UInt64Series:
		values := make([]uint64, c.Len())
		valid := make([]bool, c.Len())
		for i := 0; i < c.Len(); i++ {
			values[i] = c.Value(i)
			valid[i] = !c.IsNull(i)
		}
		return NewUInt64Series(c.Name(), grizzmemory.DefaultAllocator, values, valid)
	case *Int8Series:
		values := make([]int8, c.Len())
		valid := make([]bool, c.Len())
		for i := 0; i < c.Len(); i++ {
			values[i] = c.Value(i)
			valid[i] = !c.IsNull(i)
		}
		return NewInt8Series(c.Name(), grizzmemory.DefaultAllocator, values, valid)
	case *Int16Series:
		values := make([]int16, c.Len())
		valid := make([]bool, c.Len())
		for i := 0; i < c.Len(); i++ {
			values[i] = c.Value(i)
			valid[i] = !c.IsNull(i)
		}
		return NewInt16Series(c.Name(), grizzmemory.DefaultAllocator, values, valid)
	}
	return nil
}

func TypeName(s Series) string {
	return s.Type().Name()
}

func IsNumeric(s Series) bool {
	switch s.(type) {
	case *Int64Series, *Float64Series, *UInt8Series, *UInt16Series, *UInt32Series, *UInt64Series, *Int8Series, *Int16Series:
		return true
	}
	return false
}

func IsString(s Series) bool {
	_, ok := s.(*StringSeries)
	return ok
}

func IsBoolean(s Series) bool {
	_, ok := s.(*BooleanSeries)
	return ok
}

func ValidateSeries(s Series) error {
	if s == nil {
		return fmt.Errorf("series is nil")
	}
	if s.Len() < 0 {
		return fmt.Errorf("series length cannot be negative")
	}
	return nil
}

func ValidateSeriesMatch(a, b Series) error {
	if err := ValidateSeries(a); err != nil {
		return fmt.Errorf("series a: %w", err)
	}
	if err := ValidateSeries(b); err != nil {
		return fmt.Errorf("series b: %w", err)
	}
	if a.Len() != b.Len() {
		return fmt.Errorf("series length mismatch: %d vs %d", a.Len(), b.Len())
	}
	return nil
}
