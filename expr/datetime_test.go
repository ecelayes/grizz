package expr

import "testing"

func TestYear(t *testing.T) {
	result := Year(Col("date_col"))
	expected := "year(date_col)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestYearExprString(t *testing.T) {
	result := Year(Column{Name: "timestamp"})
	expected := "year(timestamp)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestYearExprStruct(t *testing.T) {
	expr := YearExpr{Expr: Column{Name: "date"}}
	if expr.Expr.String() != "date" {
		t.Errorf("Expected expr 'date', got %s", expr.Expr.String())
	}
}

func TestMonth(t *testing.T) {
	result := Month(Col("date_col"))
	expected := "month(date_col)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestMonthExprString(t *testing.T) {
	result := Month(Column{Name: "timestamp"})
	expected := "month(timestamp)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestDay(t *testing.T) {
	result := Day(Col("date_col"))
	expected := "day(date_col)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestDayExprString(t *testing.T) {
	result := Day(Column{Name: "timestamp"})
	expected := "day(timestamp)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestHour(t *testing.T) {
	result := Hour(Col("time_col"))
	expected := "hour(time_col)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestHourExprString(t *testing.T) {
	result := Hour(Column{Name: "timestamp"})
	expected := "hour(timestamp)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestMinute(t *testing.T) {
	result := Minute(Col("time_col"))
	expected := "minute(time_col)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestMinuteExprString(t *testing.T) {
	result := Minute(Column{Name: "timestamp"})
	expected := "minute(timestamp)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestSecond(t *testing.T) {
	result := Second(Col("time_col"))
	expected := "second(time_col)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestSecondExprString(t *testing.T) {
	result := Second(Column{Name: "timestamp"})
	expected := "second(timestamp)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestWeekday(t *testing.T) {
	result := Weekday(Col("date_col"))
	expected := "weekday(date_col)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestWeekdayExprString(t *testing.T) {
	result := Weekday(Column{Name: "timestamp"})
	expected := "weekday(timestamp)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}
