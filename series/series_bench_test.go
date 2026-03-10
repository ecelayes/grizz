package series

import (
	"testing"

	"github.com/ecelayes/grizz/internal/memory"
)

func BenchmarkInt64SeriesNew(b *testing.B) {
	values := make([]int64, 10000)
	for i := range values {
		values[i] = int64(i)
	}
	valid := make([]bool, 10000)
	for i := range valid {
		valid[i] = i%2 == 0
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewInt64Series("test", memory.DefaultAllocator, values, valid)
	}
}

func BenchmarkInt64SeriesSum(b *testing.B) {
	s := NewInt64Series("test", memory.DefaultAllocator, makeInt64Values(100000), nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Sum()
	}
}

func BenchmarkInt64SeriesMean(b *testing.B) {
	s := NewInt64Series("test", memory.DefaultAllocator, makeInt64Values(100000), nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Mean()
	}
}

func BenchmarkInt64SeriesMin(b *testing.B) {
	s := NewInt64Series("test", memory.DefaultAllocator, makeInt64Values(100000), nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Min()
	}
}

func BenchmarkInt64SeriesMax(b *testing.B) {
	s := NewInt64Series("test", memory.DefaultAllocator, makeInt64Values(100000), nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Max()
	}
}

func makeInt64Values(n int) []int64 {
	values := make([]int64, n)
	for i := 0; i < n; i++ {
		values[i] = int64(i)
	}
	return values
}

func BenchmarkFloat64SeriesNew(b *testing.B) {
	values := make([]float64, 10000)
	for i := range values {
		values[i] = float64(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewFloat64Series("test", memory.DefaultAllocator, values, nil)
	}
}

func BenchmarkFloat64SeriesSum(b *testing.B) {
	s := NewFloat64Series("test", memory.DefaultAllocator, makeFloat64Values(100000), nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Sum()
	}
}

func BenchmarkFloat64SeriesMean(b *testing.B) {
	s := NewFloat64Series("test", memory.DefaultAllocator, makeFloat64Values(100000), nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Mean()
	}
}

func makeFloat64Values(n int) []float64 {
	values := make([]float64, n)
	for i := 0; i < n; i++ {
		values[i] = float64(i)
	}
	return values
}

func BenchmarkStringSeriesNew(b *testing.B) {
	values := make([]string, 10000)
	for i := range values {
		values[i] = "string"
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewStringSeries("test", memory.DefaultAllocator, values, nil)
	}
}
