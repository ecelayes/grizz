package expr

import (
	"testing"
)

func TestEwmMeanExpr(t *testing.T) {
	e := EwmMean(Col("value"))
	if e.Alpha != 0.5 {
		t.Errorf("Expected alpha 0.5, got %f", e.Alpha)
	}
	if !e.Adjust {
		t.Error("Expected Adjust to be true by default")
	}
	if e.MinPeriods != 1 {
		t.Errorf("Expected MinPeriods 1, got %d", e.MinPeriods)
	}
}

func TestEwmMeanAlpha(t *testing.T) {
	e := EwmMeanAlpha(Col("value"), 0.3)
	if e.Alpha != 0.3 {
		t.Errorf("Expected alpha 0.3, got %f", e.Alpha)
	}
}

func TestEwmMeanAlphaMinPeriods(t *testing.T) {
	e := EwmMeanAlphaMinPeriods(Col("value"), 0.7, 3)
	if e.Alpha != 0.7 {
		t.Errorf("Expected alpha 0.7, got %f", e.Alpha)
	}
	if e.MinPeriods != 3 {
		t.Errorf("Expected MinPeriods 3, got %d", e.MinPeriods)
	}
}

func TestEwmMeanString(t *testing.T) {
	e := EwmMean(Col("x"))
	expected := "ewm_mean(x, alpha=0.50)"
	if e.String() != expected {
		t.Errorf("Expected %s, got %s", expected, e.String())
	}
}

func TestEwmMeanAlphaString(t *testing.T) {
	e := EwmMeanAlpha(Col("x"), 0.3)
	expected := "ewm_mean(x, alpha=0.30)"
	if e.String() != expected {
		t.Errorf("Expected %s, got %s", expected, e.String())
	}
}

func TestEwmMeanAlias(t *testing.T) {
	e := EwmMean(Col("value"))
	aliasExpr := Alias(e, "ewm")
	expected := "ewm_mean(value, alpha=0.50) AS ewm"
	if aliasExpr.String() != expected {
		t.Errorf("Expected %s, got %s", expected, aliasExpr.String())
	}
}
