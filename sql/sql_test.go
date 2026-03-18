package sql

import (
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestSQLSimpleSelect(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{25, 30, 35}, nil))
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob", "Charlie"}, nil))

	result, err := SQL("SELECT name, age FROM users", df)
	if err != nil {
		t.Fatalf("SQL failed: %v", err)
	}

	cols := result.Columns()
	if len(cols) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(cols))
	}

	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestSQLSelectStar(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{25, 30}, nil))
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob"}, nil))

	result, err := SQL("SELECT * FROM users", df)
	if err != nil {
		t.Fatalf("SQL failed: %v", err)
	}

	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
}

func TestSQLWhere(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{25, 30, 35}, nil))
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob", "Charlie"}, nil))

	result, err := SQL("SELECT name, age FROM users WHERE age > 28", df)
	if err != nil {
		t.Fatalf("SQL failed: %v", err)
	}

	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}

	ageCol, _ := result.ColByName("age")
	ageSeries := ageCol.(*series.Int64Series)
	if ageSeries.Value(0) != 30 || ageSeries.Value(1) != 35 {
		t.Errorf("Unexpected ages: %d, %d", ageSeries.Value(0), ageSeries.Value(1))
	}
}

func TestSQLWhereAnd(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{25, 30, 35, 40}, nil))
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob", "Charlie", "David"}, nil))

	result, err := SQL("SELECT name FROM users WHERE age > 25 AND age < 40", df)
	if err != nil {
		t.Fatalf("SQL failed: %v", err)
	}

	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestSQLWhereOr(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{25, 30, 35}, nil))
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob", "Charlie"}, nil))

	result, err := SQL("SELECT name FROM users WHERE age = 25 OR age = 35", df)
	if err != nil {
		t.Fatalf("SQL failed: %v", err)
	}

	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestSQLLimit(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{25, 30, 35, 40, 45}, nil))
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"A", "B", "C", "D", "E"}, nil))

	result, err := SQL("SELECT name FROM users LIMIT 3", df)
	if err != nil {
		t.Fatalf("SQL failed: %v", err)
	}

	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestSQLOrderBy(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{35, 25, 45}, nil))
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Charlie", "Alice", "David"}, nil))

	result, err := SQL("SELECT name, age FROM users ORDER BY age ASC", df)
	if err != nil {
		t.Fatalf("SQL failed: %v", err)
	}

	nameCol, _ := result.ColByName("name")
	nameSeries := nameCol.(*series.StringSeries)
	if nameSeries.Value(0) != "Alice" || nameSeries.Value(1) != "Charlie" || nameSeries.Value(2) != "David" {
		t.Errorf("Unexpected order: %s, %s, %s", nameSeries.Value(0), nameSeries.Value(1), nameSeries.Value(2))
	}
}

func TestSQLOrderByDesc(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{35, 25, 45}, nil))
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Charlie", "Alice", "David"}, nil))

	result, err := SQL("SELECT name, age FROM users ORDER BY age DESC", df)
	if err != nil {
		t.Fatalf("SQL failed: %v", err)
	}

	nameCol, _ := result.ColByName("name")
	nameSeries := nameCol.(*series.StringSeries)
	if nameSeries.Value(0) != "David" || nameSeries.Value(1) != "Charlie" || nameSeries.Value(2) != "Alice" {
		t.Errorf("Unexpected order: %s, %s, %s", nameSeries.Value(0), nameSeries.Value(1), nameSeries.Value(2))
	}
}

func TestSQLGroupBy(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("department", memory.DefaultAllocator, []string{"Sales", "Sales", "Engineering", "Engineering"}, nil))
	df.AddSeries(series.NewInt64Series("salary", memory.DefaultAllocator, []int64{50000, 60000, 80000, 90000}, nil))

	result, err := SQL("SELECT department, SUM(salary) FROM employees GROUP BY department", df)
	if err != nil {
		t.Fatalf("SQL failed: %v", err)
	}

	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestSQLGroupByWithWhere(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("department", memory.DefaultAllocator, []string{"Sales", "Sales", "Engineering", "Engineering"}, nil))
	df.AddSeries(series.NewInt64Series("salary", memory.DefaultAllocator, []int64{50000, 60000, 80000, 90000}, nil))

	result, err := SQL("SELECT department, SUM(salary) FROM employees WHERE salary > 50000 GROUP BY department", df)
	if err != nil {
		t.Fatalf("SQL failed: %v", err)
	}

	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestSQLIn(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{25, 30, 35, 40}, nil))
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob", "Charlie", "David"}, nil))

	result, err := SQL("SELECT name FROM users WHERE age IN (25, 35)", df)
	if err != nil {
		t.Fatalf("SQL failed: %v", err)
	}

	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestSQLBetween(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{25, 30, 35, 40}, nil))
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"A", "B", "C", "D"}, nil))

	result, err := SQL("SELECT name FROM users WHERE age BETWEEN 28 AND 38", df)
	if err != nil {
		t.Fatalf("SQL failed: %v", err)
	}

	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestSQLNot(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{25, 30, 35}, nil))
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob", "Charlie"}, nil))

	result, err := SQL("SELECT name FROM users WHERE NOT (age < 30)", df)
	if err != nil {
		t.Fatalf("SQL failed: %v", err)
	}

	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestSQLNotEqual(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{25, 30, 35}, nil))
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob", "Charlie"}, nil))

	result, err := SQL("SELECT name FROM users WHERE age != 30", df)
	if err != nil {
		t.Fatalf("SQL failed: %v", err)
	}

	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestSQLCount(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("department", memory.DefaultAllocator, []string{"Sales", "Sales", "Engineering"}, nil))
	df.AddSeries(series.NewInt64Series("salary", memory.DefaultAllocator, []int64{50000, 60000, 80000}, nil))

	result, err := SQL("SELECT department, COUNT(salary) FROM employees GROUP BY department", df)
	if err != nil {
		t.Fatalf("SQL failed: %v", err)
	}

	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestSQLMinMax(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("department", memory.DefaultAllocator, []string{"Sales", "Sales", "Engineering", "Engineering"}, nil))
	df.AddSeries(series.NewInt64Series("salary", memory.DefaultAllocator, []int64{50000, 60000, 80000, 90000}, nil))

	result, err := SQL("SELECT department, MIN(salary), MAX(salary) FROM employees GROUP BY department", df)
	if err != nil {
		t.Fatalf("SQL failed: %v", err)
	}

	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.NumRows())
	}
}

func TestSQLDistinct(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewInt64Series("age", memory.DefaultAllocator, []int64{25, 30, 25, 35}, nil))
	df.AddSeries(series.NewStringSeries("name", memory.DefaultAllocator, []string{"Alice", "Bob", "Alice", "Charlie"}, nil))

	result, err := SQL("SELECT DISTINCT name FROM users", df)
	if err != nil {
		t.Fatalf("SQL failed: %v", err)
	}

	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}
