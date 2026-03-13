package series

import (
	"regexp"
	"strings"

	grizzarrows "github.com/ecelayes/grizz/internal/arrow"
	grizzmemory "github.com/ecelayes/grizz/internal/memory"
)

type StringSeries struct {
	name string
	data *grizzarrows.StringArray
}

func NewStringSeries(name string, mem grizzmemory.Allocator, values []string, valid []bool) *StringSeries {
	builder := grizzarrows.NewStringBuilder(mem)
	defer builder.Release()

	builder.AppendValues(values, valid)

	return &StringSeries{
		name: name,
		data: builder.NewStringArray(),
	}
}

func (s *StringSeries) Name() string {
	return s.name
}

func (s *StringSeries) SetName(name string) {
	s.name = name
}

func (s *StringSeries) Type() grizzarrows.DataType {
	return s.data.DataType()
}

func (s *StringSeries) Len() int {
	return s.data.Len()
}

func (s *StringSeries) IsNull(i int) bool {
	return s.data.IsNull(i)
}

func (s *StringSeries) Value(i int) string {
	return s.data.Value(i)
}

func (s *StringSeries) Release() {
	if s.data != nil {
		s.data.Release()
	}
}

func (s *StringSeries) Upper() *StringSeries {
	values := make([]string, s.Len())
	for i := 0; i < s.Len(); i++ {
		if s.IsNull(i) {
			values[i] = ""
		} else {
			values[i] = strings.ToUpper(s.Value(i))
		}
	}
	return NewStringSeries(s.name, grizzmemory.DefaultAllocator, values, nil)
}

func (s *StringSeries) Lower() *StringSeries {
	values := make([]string, s.Len())
	for i := 0; i < s.Len(); i++ {
		if s.IsNull(i) {
			values[i] = ""
		} else {
			values[i] = strings.ToLower(s.Value(i))
		}
	}
	return NewStringSeries(s.name, grizzmemory.DefaultAllocator, values, nil)
}

func (s *StringSeries) Contains(substr string) *BooleanSeries {
	values := make([]bool, s.Len())
	for i := 0; i < s.Len(); i++ {
		if s.IsNull(i) {
			values[i] = false
		} else {
			values[i] = strings.Contains(s.Value(i), substr)
		}
	}
	return NewBooleanSeries(s.name, grizzmemory.DefaultAllocator, values, nil)
}

func (s *StringSeries) Replace(old, new string) *StringSeries {
	values := make([]string, s.Len())
	for i := 0; i < s.Len(); i++ {
		if s.IsNull(i) {
			values[i] = ""
		} else {
			values[i] = strings.ReplaceAll(s.Value(i), old, new)
		}
	}
	return NewStringSeries(s.name, grizzmemory.DefaultAllocator, values, nil)
}

func (s *StringSeries) Length() *Int64Series {
	values := make([]int64, s.Len())
	for i := 0; i < s.Len(); i++ {
		if s.IsNull(i) {
			values[i] = 0
		} else {
			values[i] = int64(len(s.Value(i)))
		}
	}
	return NewInt64Series(s.name, grizzmemory.DefaultAllocator, values, nil)
}

func (s *StringSeries) Strip() *StringSeries {
	values := make([]string, s.Len())
	for i := 0; i < s.Len(); i++ {
		if s.IsNull(i) {
			values[i] = ""
		} else {
			values[i] = strings.TrimSpace(s.Value(i))
		}
	}
	return NewStringSeries(s.name, grizzmemory.DefaultAllocator, values, nil)
}

func (s *StringSeries) ContainsRegex(pattern string) *BooleanSeries {
	re := regexp.MustCompile(pattern)
	values := make([]bool, s.Len())
	for i := 0; i < s.Len(); i++ {
		if s.IsNull(i) {
			values[i] = false
		} else {
			values[i] = re.MatchString(s.Value(i))
		}
	}
	return NewBooleanSeries(s.name, grizzmemory.DefaultAllocator, values, nil)
}
