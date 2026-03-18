package expr

import (
	"testing"
)

func TestTruncateExpr(t *testing.T) {
	e := Truncate(Col("timestamp"), "1h")
	if e.Period != "1h" {
		t.Errorf("Expected period 1h, got %s", e.Period)
	}
}

func TestTruncateString(t *testing.T) {
	e := Truncate(Col("ts"), "1d")
	expected := "truncate(ts, '1d')"
	if e.String() != expected {
		t.Errorf("Expected %s, got %s", expected, e.String())
	}
}

func TestTruncateAlias(t *testing.T) {
	e := Truncate(Col("ts"), "1h")
	aliasExpr := Alias(e, "truncated")
	expected := "truncate(ts, '1h') AS truncated"
	if aliasExpr.String() != expected {
		t.Errorf("Expected %s, got %s", expected, aliasExpr.String())
	}
}
