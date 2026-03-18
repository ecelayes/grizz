package expr

import (
	"testing"
)

func TestDiffExpr(t *testing.T) {
	e := Diff(Col("value"))
	if e.Periods != 1 {
		t.Errorf("Expected periods 1, got %d", e.Periods)
	}
}

func TestDiffPeriodsExpr(t *testing.T) {
	e := DiffPeriods(Col("value"), 3)
	if e.Periods != 3 {
		t.Errorf("Expected periods 3, got %d", e.Periods)
	}
}

func TestDiffString(t *testing.T) {
	e := Diff(Col("x"))
	expected := "diff(x)"
	if e.String() != expected {
		t.Errorf("Expected %s, got %s", expected, e.String())
	}
}

func TestDiffPeriodsString(t *testing.T) {
	e := DiffPeriods(Col("x"), 2)
	expected := "diff(x, 2)"
	if e.String() != expected {
		t.Errorf("Expected %s, got %s", expected, e.String())
	}
}

func TestPctChangeExpr(t *testing.T) {
	e := PctChange(Col("value"))
	if e.Periods != 1 {
		t.Errorf("Expected periods 1, got %d", e.Periods)
	}
}

func TestPctChangePeriodsExpr(t *testing.T) {
	e := PctChangePeriods(Col("value"), 3)
	if e.Periods != 3 {
		t.Errorf("Expected periods 3, got %d", e.Periods)
	}
}

func TestPctChangeString(t *testing.T) {
	e := PctChange(Col("x"))
	expected := "pct_change(x)"
	if e.String() != expected {
		t.Errorf("Expected %s, got %s", expected, e.String())
	}
}

func TestPctChangePeriodsString(t *testing.T) {
	e := PctChangePeriods(Col("x"), 2)
	expected := "pct_change(x, 2)"
	if e.String() != expected {
		t.Errorf("Expected %s, got %s", expected, e.String())
	}
}

func TestDiffAlias(t *testing.T) {
	col := Col("value")
	e := Diff(col)
	aliasExpr := Alias(e, "diff_value")
	expected := "diff(value) AS diff_value"
	if aliasExpr.String() != expected {
		t.Errorf("Expected %s, got %s", expected, aliasExpr.String())
	}
}
