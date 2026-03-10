package engine

import (
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestApplyLimit(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30, 40, 50}, nil))

	result, err := applyLimit(df, 3)
	if err != nil {
		t.Fatalf("applyLimit failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplyLimitNegative(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30}, nil))

	result, err := applyLimit(df, -1)
	if err != nil {
		t.Fatalf("applyLimit failed: %v", err)
	}
	if result.NumRows() != 0 {
		t.Errorf("Expected 0 rows, got %d", result.NumRows())
	}
}

func TestApplyLimitExceedsRows(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20}, nil))

	result, err := applyLimit(df, 100)
	if err != nil {
		t.Fatalf("applyLimit failed: %v", err)
	}
	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestApplyTail(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30, 40, 50}, nil))

	result, err := applyTail(df, 2)
	if err != nil {
		t.Fatalf("applyTail failed: %v", err)
	}
	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
	ageCol, _ := result.ColByName("age")
	if ageCol.(*series.Int64Series).Value(0) != 40 {
		t.Errorf("Expected first value 40, got %d", ageCol.(*series.Int64Series).Value(0))
	}
}

func TestApplyTailExceedsRows(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20}, nil))

	result, err := applyTail(df, 100)
	if err != nil {
		t.Fatalf("applyTail failed: %v", err)
	}
	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestApplySample(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30, 40, 50}, nil))

	result, err := applySample(df, 3, 0, false)
	if err != nil {
		t.Fatalf("applySample failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplySampleWithFraction(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30, 40, 50}, nil))

	result, err := applySample(df, 0, 0.5, false)
	if err != nil {
		t.Fatalf("applySample failed: %v", err)
	}
	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestApplySampleWithReplacement(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30}, nil))

	result, err := applySample(df, 5, 0, true)
	if err != nil {
		t.Fatalf("applySample failed: %v", err)
	}
	if result.NumRows() != 5 {
		t.Errorf("Expected 5 rows, got %d", result.NumRows())
	}
}

func TestApplySampleZeroN(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30}, nil))

	result, err := applySample(df, 0, 0, false)
	if err != nil {
		t.Fatalf("applySample failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplySampleFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.1, 2.2, 3.3, 4.4, 5.5}, nil))

	result, err := applySample(df, 3, 0, false)
	if err != nil {
		t.Fatalf("applySample failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplySampleString(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"a", "b", "c", "d", "e"}, nil))

	result, err := applySample(df, 3, 0, false)
	if err != nil {
		t.Fatalf("applySample failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplySampleBool(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewBooleanSeries("flag", memory.DefaultAllocator, []bool{true, false, true, false, true}, nil))

	result, err := applySample(df, 3, 0, false)
	if err != nil {
		t.Fatalf("applySample failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplySampleFloatWithFraction(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.1, 2.2, 3.3, 4.4}, nil))

	result, err := applySample(df, 0, 0.5, false)
	if err != nil {
		t.Fatalf("applySample failed: %v", err)
	}
	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestApplyLimitFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.1, 2.2, 3.3, 4.4, 5.5}, nil))

	result, err := applyLimit(df, 3)
	if err != nil {
		t.Fatalf("applyLimit failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplyLimitString(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"a", "b", "c", "d", "e"}, nil))

	result, err := applyLimit(df, 3)
	if err != nil {
		t.Fatalf("applyLimit failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplyLimitBool(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewBooleanSeries("flag", memory.DefaultAllocator, []bool{true, false, true, false, true}, nil))

	result, err := applyLimit(df, 3)
	if err != nil {
		t.Fatalf("applyLimit failed: %v", err)
	}
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestApplyTailFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("value", memory.DefaultAllocator, []float64{1.1, 2.2, 3.3, 4.4, 5.5}, nil))

	result, err := applyTail(df, 2)
	if err != nil {
		t.Fatalf("applyTail failed: %v", err)
	}
	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestApplyTailString(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"a", "b", "c", "d", "e"}, nil))

	result, err := applyTail(df, 2)
	if err != nil {
		t.Fatalf("applyTail failed: %v", err)
	}
	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestApplyTailBool(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewBooleanSeries("flag", memory.DefaultAllocator, []bool{true, false, true, false, true}, nil))

	result, err := applyTail(df, 2)
	if err != nil {
		t.Fatalf("applyTail failed: %v", err)
	}
	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}
