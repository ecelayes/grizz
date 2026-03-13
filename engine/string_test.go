package engine

import (
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestApplyWithColumnsContains(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"hello", "world", "test"}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Contains(expr.Col("name"), expr.Lit("lo")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsReplace(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"hello", "world"}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Replace(expr.Col("name"), expr.Lit("o"), expr.Lit("x")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsUpper(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"HELLO", "WORLD"}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Upper(expr.Col("name")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsLower(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"HELLO", "WORLD"}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Lower(expr.Col("name")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsTrim(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"  hello  ", "world"}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Trim(expr.Col("name")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsStrip(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"  hello  ", "  world  "}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Strip(expr.Col("name")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsLpad(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"hi", "hello"}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.LPad(expr.Col("name"), expr.Lit(5)),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsRpad(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"hi", "hello"}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.RPad(expr.Col("name"), expr.Lit(5)),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsSplit(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"a,b", "c,d"}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Split(expr.Col("name"), expr.Lit(",")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsSlice(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"hello", "world"}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Slice(expr.Col("name"), expr.Lit(0), expr.Lit(2)),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsLength(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"hi", "hello"}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Length(expr.Col("name")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestApplyWithColumnsContainsRegex(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"hello123", "world"}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.ContainsRegex(expr.Col("name"), expr.Lit("[0-9]+")),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestExtract(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("text", memory.DefaultAllocator, []string{"abc123def", "hello456", "nomatch"}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Extract(expr.Col("text"), expr.Lit(`(\d+)`)).Alias("extracted"),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}

	resCol, err := result.ColByName("extracted")
	if err != nil {
		t.Fatalf("ColByName failed: %v", err)
	}
	res := resCol.(*series.StringSeries)

	if res.Value(0) != "123" {
		t.Errorf("Expected '123' at index 0, got '%s'", res.Value(0))
	}
	if res.Value(1) != "456" {
		t.Errorf("Expected '456' at index 1, got '%s'", res.Value(1))
	}
	if res.Value(2) != "" {
		t.Errorf("Expected '' at index 2, got '%s'", res.Value(2))
	}

	result.Release()
}

func TestFind(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("text", memory.DefaultAllocator, []string{"hello world", "test", "abc"}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Find(expr.Col("text"), expr.Lit("world")).Alias("pos"),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}

	resCol, err := result.ColByName("pos")
	if err != nil {
		t.Fatalf("ColByName failed: %v", err)
	}
	res := resCol.(*series.Int64Series)

	if res.Value(0) != 6 {
		t.Errorf("Expected 6 at index 0, got %d", res.Value(0))
	}
	if res.Value(1) != -1 {
		t.Errorf("Expected -1 at index 1, got %d", res.Value(1))
	}
	if res.Value(2) != -1 {
		t.Errorf("Expected -1 at index 2, got %d", res.Value(2))
	}

	result.Release()
}
