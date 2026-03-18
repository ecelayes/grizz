package engine

import (
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestApplyProjection(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	df.AddSeries(series.NewInt64Series("b", memory.DefaultAllocator, []int64{4, 5, 6}, nil))

	result, err := applyProjection(df, []expr.Expr{expr.Col("a")})
	if err != nil {
		t.Fatalf("applyProjection failed: %v", err)
	}
	if result.NumCols() != 1 {
		t.Errorf("Expected 1 column, got %d", result.NumCols())
	}
}

func TestApplyProjectionFloat(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewFloat64Series("a", memory.DefaultAllocator, []float64{1.5, 2.5, 3.5}, nil))
	df.AddSeries(series.NewFloat64Series("b", memory.DefaultAllocator, []float64{4.5, 5.5, 6.5}, nil))

	result, err := applyProjection(df, []expr.Expr{expr.Col("a")})
	if err != nil {
		t.Fatalf("applyProjection failed: %v", err)
	}
	if result.NumCols() != 1 {
		t.Errorf("Expected 1 column, got %d", result.NumCols())
	}
	valCol, _ := result.ColByName("a")
	if valCol.(*series.Float64Series).Value(0) != 1.5 {
		t.Errorf("Expected first value 1.5, got %f", valCol.(*series.Float64Series).Value(0))
	}
}

func TestApplyProjectionBool(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewBooleanSeries("a", memory.DefaultAllocator, []bool{true, false, true}, nil))
	df.AddSeries(series.NewBooleanSeries("b", memory.DefaultAllocator, []bool{false, true, false}, nil))

	result, err := applyProjection(df, []expr.Expr{expr.Col("a")})
	if err != nil {
		t.Fatalf("applyProjection failed: %v", err)
	}
	if result.NumCols() != 1 {
		t.Errorf("Expected 1 column, got %d", result.NumCols())
	}
}

func TestApplyProjectionString(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("a", memory.DefaultAllocator, []string{"x", "y", "z"}, nil))
	df.AddSeries(series.NewStringSeries("b", memory.DefaultAllocator, []string{"p", "q", "r"}, nil))

	result, err := applyProjection(df, []expr.Expr{expr.Col("a")})
	if err != nil {
		t.Fatalf("applyProjection failed: %v", err)
	}
	if result.NumCols() != 1 {
		t.Errorf("Expected 1 column, got %d", result.NumCols())
	}
}

func TestApplyProjectionNonColumn(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30}, nil))

	_, err := applyProjection(df, []expr.Expr{expr.Lit(10)})
	if err == nil {
		t.Error("Expected error for non-column expression in projection")
	}
}

func TestApplyProjectionNonExistentColumn(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{10, 20, 30}, nil))

	_, err := applyProjection(df, []expr.Expr{expr.Col("nonexistent")})
	if err == nil {
		t.Error("Expected error for non-existent column in projection")
	}
}

func TestApplyProjectionAtScan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	df.AddSeries(series.NewFloat64Series("b", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0}, nil))
	df.AddSeries(series.NewStringSeries("c", memory.DefaultAllocator, []string{"x", "y", "z"}, nil))

	result := applyProjectionAtScan(df, []string{"a", "c"})

	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}

	colA, err := result.ColByName("a")
	if err != nil {
		t.Fatalf("ColByName failed: %v", err)
	}
	if colA.Len() != 3 {
		t.Errorf("Expected 3 rows, got %d", colA.Len())
	}
}

func TestApplyProjectionAtScanMissingColumn(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	result := applyProjectionAtScan(df, []string{"a", "nonexistent"})

	if result.NumCols() != 1 {
		t.Errorf("Expected 1 column, got %d", result.NumCols())
	}
}

func TestApplyProjectionAtScanAllTypes(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	df.AddSeries(series.NewFloat64Series("b", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0}, nil))
	df.AddSeries(series.NewStringSeries("c", memory.DefaultAllocator, []string{"x", "y", "z"}, nil))
	df.AddSeries(series.NewBooleanSeries("d", memory.DefaultAllocator, []bool{true, false, true}, nil))

	result := applyProjectionAtScan(df, []string{"a", "b", "c", "d"})

	if result.NumCols() != 4 {
		t.Errorf("Expected 4 columns, got %d", result.NumCols())
	}
}

func TestResolveSelectorAll(t *testing.T) {
	columnNames := []string{"a", "b", "c", "d"}
	columnTypes := []string{"int64", "float64", "string", "bool"}

	result := expr.ResolveSelector(expr.All(), columnNames, columnTypes)

	if len(result) != 4 {
		t.Errorf("Expected 4 columns, got %d", len(result))
	}
	expected := []string{"a", "b", "c", "d"}
	for i, name := range expected {
		if result[i] != name {
			t.Errorf("Expected column %s at index %d, got %s", name, i, result[i])
		}
	}
}

func TestResolveSelectorNumeric(t *testing.T) {
	columnNames := []string{"a", "b", "c", "d"}
	columnTypes := []string{"int64", "float64", "string", "bool"}

	result := expr.ResolveSelector(expr.Numeric(), columnNames, columnTypes)

	if len(result) != 2 {
		t.Errorf("Expected 2 numeric columns, got %d", len(result))
	}
	expected := []string{"a", "b"}
	for i, name := range expected {
		if result[i] != name {
			t.Errorf("Expected column %s at index %d, got %s", name, i, result[i])
		}
	}
}

func TestResolveSelectorString(t *testing.T) {
	columnNames := []string{"a", "b", "c", "d"}
	columnTypes := []string{"int64", "float64", "string", "bool"}

	result := expr.ResolveSelector(expr.String(), columnNames, columnTypes)

	if len(result) != 1 {
		t.Errorf("Expected 1 string column, got %d", len(result))
	}
	if result[0] != "c" {
		t.Errorf("Expected column c, got %s", result[0])
	}
}

func TestResolveSelectorBoolean(t *testing.T) {
	columnNames := []string{"a", "b", "c", "d"}
	columnTypes := []string{"int64", "float64", "string", "bool"}

	result := expr.ResolveSelector(expr.Boolean(), columnNames, columnTypes)

	if len(result) != 1 {
		t.Errorf("Expected 1 boolean column, got %d", len(result))
	}
	if result[0] != "d" {
		t.Errorf("Expected column d, got %s", result[0])
	}
}

func TestResolveSelectorByName(t *testing.T) {
	columnNames := []string{"col_a", "col_b", "other", "col_c"}
	columnTypes := []string{"int64", "float64", "string", "string"}

	result := expr.ResolveSelector(expr.ByName("col_*"), columnNames, columnTypes)

	if len(result) != 3 {
		t.Errorf("Expected 3 columns matching 'col_*', got %d", len(result))
	}
}

func TestResolveSelectorByNameExactMatch(t *testing.T) {
	columnNames := []string{"col_a", "col_b", "other", "col_c"}
	columnTypes := []string{"int64", "float64", "string", "string"}

	result := expr.ResolveSelector(expr.ByName("other"), columnNames, columnTypes)

	if len(result) != 1 {
		t.Errorf("Expected 1 column matching 'other', got %d", len(result))
	}
	if result[0] != "other" {
		t.Errorf("Expected column 'other', got %s", result[0])
	}
}

func TestResolveSelectorContains(t *testing.T) {
	columnNames := []string{"user_id", "user_name", "email", "created_at"}
	columnTypes := []string{"int64", "string", "string", "string"}

	result := expr.ResolveSelector(expr.NameContains("user"), columnNames, columnTypes)

	if len(result) != 2 {
		t.Errorf("Expected 2 columns containing 'user', got %d", len(result))
	}
	if result[0] != "user_id" || result[1] != "user_name" {
		t.Errorf("Expected user_id, user_name got %v", result)
	}
}

func TestResolveSelectorStartsWith(t *testing.T) {
	columnNames := []string{"user_id", "user_name", "email", "created_at"}
	columnTypes := []string{"int64", "string", "string", "string"}

	result := expr.ResolveSelector(expr.StartsWith("user"), columnNames, columnTypes)

	if len(result) != 2 {
		t.Errorf("Expected 2 columns starting with 'user', got %d", len(result))
	}
	if result[0] != "user_id" || result[1] != "user_name" {
		t.Errorf("Expected user_id, user_name got %v", result)
	}
}

func TestResolveSelectorEndsWith(t *testing.T) {
	columnNames := []string{"user_id", "user_name", "email", "created_at"}
	columnTypes := []string{"int64", "string", "string", "string"}

	result := expr.ResolveSelector(expr.EndsWith("_id"), columnNames, columnTypes)

	if len(result) != 1 {
		t.Errorf("Expected 1 column ending with '_id', got %d", len(result))
	}
	if result[0] != "user_id" {
		t.Errorf("Expected user_id, got %s", result[0])
	}
}

func TestResolveSelectorNoMatch(t *testing.T) {
	columnNames := []string{"a", "b", "c", "d"}
	columnTypes := []string{"int64", "float64", "string", "bool"}

	result := expr.ResolveSelector(expr.ByName("xyz_*"), columnNames, columnTypes)

	if len(result) != 0 {
		t.Errorf("Expected 0 columns matching 'xyz_*', got %d", len(result))
	}
}

func TestSelectorWithProjectionAtScan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("num_a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	df.AddSeries(series.NewFloat64Series("num_b", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0}, nil))
	df.AddSeries(series.NewStringSeries("str_c", memory.DefaultAllocator, []string{"x", "y", "z"}, nil))
	df.AddSeries(series.NewBooleanSeries("flag_d", memory.DefaultAllocator, []bool{true, false, true}, nil))

	columnNames := []string{"num_a", "num_b", "str_c", "flag_d"}
	columnTypes := []string{"int64", "float64", "string", "bool"}

	numericCols := expr.ResolveSelector(expr.Numeric(), columnNames, columnTypes)

	result := applyProjectionAtScan(df, numericCols)

	if result.NumCols() != 2 {
		t.Errorf("Expected 2 numeric columns, got %d", result.NumCols())
	}

	colA, err := result.ColByName("num_a")
	if err != nil {
		t.Fatalf("ColByName num_a failed: %v", err)
	}
	if colA.Len() != 3 {
		t.Errorf("Expected 3 rows, got %d", colA.Len())
	}

	colB, err := result.ColByName("num_b")
	if err != nil {
		t.Fatalf("ColByName num_b failed: %v", err)
	}
	if colB.Len() != 3 {
		t.Errorf("Expected 3 rows, got %d", colB.Len())
	}
}

func TestSelectorByNameWithProjectionAtScan(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("user_id", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	df.AddSeries(series.NewFloat64Series("user_score", memory.DefaultAllocator, []float64{1.0, 2.0, 3.0}, nil))
	df.AddSeries(series.NewStringSeries("user_name", memory.DefaultAllocator, []string{"x", "y", "z"}, nil))
	df.AddSeries(series.NewBooleanSeries("active", memory.DefaultAllocator, []bool{true, false, true}, nil))

	columnNames := []string{"user_id", "user_score", "user_name", "active"}
	columnTypes := []string{"int64", "float64", "string", "bool"}

	userCols := expr.ResolveSelector(expr.ByName("user_*"), columnNames, columnTypes)

	result := applyProjectionAtScan(df, userCols)

	if result.NumCols() != 3 {
		t.Errorf("Expected 3 user columns, got %d", result.NumCols())
	}
}

func TestEvaluateExpressionAlias(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	df.AddSeries(series.NewInt64Series("b", memory.DefaultAllocator, []int64{4, 5, 6}, nil))

	result, err := evaluateExpression(df, expr.Col("a").Add(expr.Col("b")).Alias("sum_ab"), memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("evaluateExpression failed: %v", err)
	}
	if result.Name() != "sum_ab" {
		t.Errorf("Expected name 'sum_ab', got %s", result.Name())
	}
}

func TestEvaluateExpressionArithmetic(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))
	df.AddSeries(series.NewInt64Series("b", memory.DefaultAllocator, []int64{4, 5, 6}, nil))

	result, err := evaluateExpression(df, expr.Col("a").Add(expr.Col("b")), memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("evaluateExpression failed: %v", err)
	}
	if result.Len() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.Len())
	}
}

func TestEvaluateExpressionUnsupported(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("a", memory.DefaultAllocator, []int64{1, 2, 3}, nil))

	_, _ = evaluateExpression(df, nil, memory.DefaultAllocator)
}
