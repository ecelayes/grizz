package dataframe

import (
	"testing"

	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestSample(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil))

	sampled, err := df.Sample(5, false, 42)
	if err != nil {
		t.Fatalf("Sample failed: %v", err)
	}

	if sampled.NumRows() != 5 {
		t.Errorf("Expected 5 rows, got %d", sampled.NumRows())
	}
}

func TestSampleFrac(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil))

	sampled, err := df.SampleFrac(0.3, false, 42)
	if err != nil {
		t.Fatalf("SampleFrac failed: %v", err)
	}

	if sampled.NumRows() != 3 {
		t.Errorf("Expected 3 rows (30%% of 10), got %d", sampled.NumRows())
	}
}

func TestShuffle(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3, 4, 5}, nil))

	shuffled, err := df.Shuffle(42)
	if err != nil {
		t.Fatalf("Shuffle failed: %v", err)
	}

	if shuffled.NumRows() != 5 {
		t.Errorf("Expected 5 rows, got %d", shuffled.NumRows())
	}

	col, _ := shuffled.ColByName("a")
	intCol := col.(*series.Int64Series)
	original := []int64{1, 2, 3, 4, 5}
	found := make([]bool, 5)
	for i := 0; i < 5; i++ {
		for j, v := range original {
			if intCol.Value(i) == v {
				found[j] = true
			}
		}
	}
	for _, f := range found {
		if !f {
			t.Error("Shuffle should contain all original values")
		}
	}
}

func TestSampleWithReplacement(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	sampled, err := df.Sample(5, true, 42)
	if err != nil {
		t.Fatalf("Sample with replacement failed: %v", err)
	}

	if sampled.NumRows() != 5 {
		t.Errorf("Expected 5 rows, got %d", sampled.NumRows())
	}
}

func TestSampleSeed(t *testing.T) {
	df := New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil))

	sampled1, _ := df.Sample(5, false, 42)
	sampled2, _ := df.Sample(5, false, 42)

	col1, _ := sampled1.ColByName("a")
	col2, _ := sampled2.ColByName("a")
	intCol1 := col1.(*series.Int64Series)
	intCol2 := col2.(*series.Int64Series)

	for i := 0; i < 5; i++ {
		if intCol1.Value(i) != intCol2.Value(i) {
			t.Error("Same seed should produce same results")
		}
	}
}
