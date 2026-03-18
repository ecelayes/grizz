package expr

import "testing"

func TestRollingSum(t *testing.T) {
	result := RollingSum(Col("a"), 7, 3)
	expected := "rolling_sum(a, 7, 3)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestRollingMean(t *testing.T) {
	result := RollingMean(Col("b"), 5, 0)
	expected := "rolling_mean(b, 5, 5)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestRollingMin(t *testing.T) {
	result := RollingMin(Col("c"), 3, 2)
	expected := "rolling_min(c, 3, 2)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestRollingMax(t *testing.T) {
	result := RollingMax(Col("d"), 10, 5)
	expected := "rolling_max(d, 10, 5)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestRollingMinDefaultMinPeriods(t *testing.T) {
	result := RollingMin(Col("x"), 4, 0)
	expected := "rolling_min(x, 4, 4)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestRollingMaxDefaultMinPeriods(t *testing.T) {
	result := RollingMax(Col("y"), 6, 0)
	expected := "rolling_max(y, 6, 6)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestRollingExprAlias(t *testing.T) {
	result := RollingSum(Col("a"), 7, 3).Alias("rolling")
	expected := "rolling_sum(a, 7, 3) AS rolling"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}
