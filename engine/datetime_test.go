package engine

import (
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func TestApplyYear(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("date", memory.DefaultAllocator, []string{"2024-01-15", "2023-06-20", "2025-12-31"}, nil))

	result, err := applyYear(df, expr.YearExpr{Expr: expr.Col("date")}, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("applyYear failed: %v", err)
	}
	if result.Name() != "date_year" {
		t.Errorf("Expected name date_year, got %s", result.Name())
	}
	intResult := result.(*series.Int64Series)
	if intResult.Value(0) != 2024 {
		t.Errorf("Expected 2024, got %d", intResult.Value(0))
	}
	if intResult.Value(1) != 2023 {
		t.Errorf("Expected 2023, got %d", intResult.Value(1))
	}
	if intResult.Value(2) != 2025 {
		t.Errorf("Expected 2025, got %d", intResult.Value(2))
	}
}

func TestApplyMonth(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("date", memory.DefaultAllocator, []string{"2024-01-15", "2024-06-20", "2024-12-31"}, nil))

	result, err := applyMonth(df, expr.MonthExpr{Expr: expr.Col("date")}, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("applyMonth failed: %v", err)
	}
	intResult := result.(*series.Int64Series)
	if intResult.Value(0) != 1 {
		t.Errorf("Expected month 1, got %d", intResult.Value(0))
	}
	if intResult.Value(1) != 6 {
		t.Errorf("Expected month 6, got %d", intResult.Value(1))
	}
	if intResult.Value(2) != 12 {
		t.Errorf("Expected month 12, got %d", intResult.Value(2))
	}
}

func TestApplyDay(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("date", memory.DefaultAllocator, []string{"2024-01-15", "2024-06-20", "2024-12-31"}, nil))

	result, err := applyDay(df, expr.DayExpr{Expr: expr.Col("date")}, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("applyDay failed: %v", err)
	}
	intResult := result.(*series.Int64Series)
	if intResult.Value(0) != 15 {
		t.Errorf("Expected day 15, got %d", intResult.Value(0))
	}
	if intResult.Value(1) != 20 {
		t.Errorf("Expected day 20, got %d", intResult.Value(1))
	}
	if intResult.Value(2) != 31 {
		t.Errorf("Expected day 31, got %d", intResult.Value(2))
	}
}

func TestApplyHour(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("time", memory.DefaultAllocator, []string{"2024-01-15T10:30", "2024-06-20T14:45", "2024-12-31T23:59"}, nil))

	result, err := applyHour(df, expr.HourExpr{Expr: expr.Col("time")}, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("applyHour failed: %v", err)
	}
	intResult := result.(*series.Int64Series)
	if intResult.Value(0) != 10 {
		t.Errorf("Expected hour 10, got %d", intResult.Value(0))
	}
	if intResult.Value(1) != 14 {
		t.Errorf("Expected hour 14, got %d", intResult.Value(1))
	}
	if intResult.Value(2) != 23 {
		t.Errorf("Expected hour 23, got %d", intResult.Value(2))
	}
}

func TestApplyMinute(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("time", memory.DefaultAllocator, []string{"2024-01-15T10:30", "2024-06-20T14:45", "2024-12-31T23:59"}, nil))

	result, err := applyMinute(df, expr.MinuteExpr{Expr: expr.Col("time")}, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("applyMinute failed: %v", err)
	}
	intResult := result.(*series.Int64Series)
	if intResult.Value(0) != 30 {
		t.Errorf("Expected minute 30, got %d", intResult.Value(0))
	}
	if intResult.Value(1) != 45 {
		t.Errorf("Expected minute 45, got %d", intResult.Value(1))
	}
	if intResult.Value(2) != 59 {
		t.Errorf("Expected minute 59, got %d", intResult.Value(2))
	}
}

func TestApplySecond(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("time", memory.DefaultAllocator, []string{"2024-01-15T10:30:45", "2024-06-20T14:45:15", "2024-12-31T23:59:30"}, nil))

	result, err := applySecond(df, expr.SecondExpr{Expr: expr.Col("time")}, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("applySecond failed: %v", err)
	}
	intResult := result.(*series.Int64Series)
	if intResult.Value(0) != 45 {
		t.Errorf("Expected second 45, got %d", intResult.Value(0))
	}
	if intResult.Value(1) != 15 {
		t.Errorf("Expected second 15, got %d", intResult.Value(1))
	}
	if intResult.Value(2) != 30 {
		t.Errorf("Expected second 30, got %d", intResult.Value(2))
	}
}

func TestApplyWeekday(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("date", memory.DefaultAllocator, []string{"2024-01-15", "2024-01-16", "2024-01-17"}, nil))

	result, err := applyWeekday(df, expr.WeekdayExpr{Expr: expr.Col("date")}, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("applyWeekday failed: %v", err)
	}
	intResult := result.(*series.Int64Series)
	if intResult.Value(0) != 1 {
		t.Errorf("Expected weekday 1 (Monday), got %d", intResult.Value(0))
	}
}

func TestWithColumnsYear(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("date", memory.DefaultAllocator, []string{"2024-01-15", "2023-06-20"}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Year(expr.Col("date")).Alias("year"),
	})
	if err != nil {
		t.Fatalf("applyWithColumns failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
	yearCol, _ := result.ColByName("year")
	intResult := yearCol.(*series.Int64Series)
	if intResult.Value(0) != 2024 {
		t.Errorf("Expected 2024, got %d", intResult.Value(0))
	}
}

func TestWithColumnsTruncate(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("timestamp", memory.DefaultAllocator, []string{"2024-01-15T14:30:00", "2024-01-15T15:45:00"}, nil))

	result, err := applyWithColumns(df, []expr.Expr{
		expr.Truncate(expr.Col("timestamp"), "1h").Alias("truncated"),
	})
	if err != nil {
		t.Fatalf("applyWithColumns with Truncate failed: %v", err)
	}
	if result.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.NumCols())
	}
	truncCol, _ := result.ColByName("truncated")
	strResult := truncCol.(*series.StringSeries)
	if strResult.Len() != 2 {
		t.Errorf("Expected 2 elements, got %d", strResult.Len())
	}
}

func TestTruncateHour(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("timestamp", memory.DefaultAllocator, []string{"2024-01-15T14:30:00", "2024-01-15T14:45:00", "2024-01-15T15:15:00"}, nil))

	result, err := applyTruncate(df, expr.Truncate(expr.Col("timestamp"), "1h"), memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("applyTruncate failed: %v", err)
	}

	strResult := result.(*series.StringSeries)
	if strResult.Len() != 3 {
		t.Errorf("Expected 3 elements, got %d", strResult.Len())
	}
}

func TestTruncateDay(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("date", memory.DefaultAllocator, []string{"2024-01-15T14:30:00", "2024-01-16T08:00:00", "2024-01-17T23:59:59"}, nil))

	result, err := applyTruncate(df, expr.Truncate(expr.Col("date"), "1d"), memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("applyTruncate failed: %v", err)
	}

	strResult := result.(*series.StringSeries)
	if strResult.Len() != 3 {
		t.Errorf("Expected 3 elements, got %d", strResult.Len())
	}
}

func TestTruncateInvalidPeriod(t *testing.T) {
	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("date", memory.DefaultAllocator, []string{"2024-01-15"}, nil))

	_, err := applyTruncate(df, expr.Truncate(expr.Col("date"), "invalid"), memory.DefaultAllocator)
	if err == nil {
		t.Error("Expected error for invalid period")
	}
}
