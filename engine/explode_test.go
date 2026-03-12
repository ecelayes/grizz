package engine

import (
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestApplyExplode(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("tags", memory.DefaultAllocator, []string{"a,b,c", "d,e", "f"}, nil))

	result, err := applyExplode(df, expr.ExplodeExpr{
		Expr:      expr.Col("tags"),
		Delimiter: ",",
	}, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("applyExplode failed: %v", err)
	}
	if result.Name() != "tags_exploded" {
		t.Errorf("Expected name tags_exploded, got %s", result.Name())
	}
	strResult := result.(*series.StringSeries)
	if strResult.Len() != 6 {
		t.Errorf("Expected 6 values, got %d", strResult.Len())
	}
	if strResult.Value(0) != "a" {
		t.Errorf("Expected 'a', got %s", strResult.Value(0))
	}
	if strResult.Value(3) != "d" {
		t.Errorf("Expected 'd', got %s", strResult.Value(3))
	}
}

func TestApplyExplodeSingle(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("words", memory.DefaultAllocator, []string{"hello", "world"}, nil))

	result, err := applyExplode(df, expr.ExplodeExpr{
		Expr:      expr.Col("words"),
		Delimiter: "",
	}, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("applyExplode failed: %v", err)
	}
	strResult := result.(*series.StringSeries)
	if strResult.Len() != 10 {
		t.Errorf("Expected 10 values, got %d", strResult.Len())
	}
}
