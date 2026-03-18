package expr

import "testing"

func TestCumSum(t *testing.T) {
	result := CumSum(Col("a"))
	expected := "cum_sum(a)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestCumProd(t *testing.T) {
	result := CumProd(Col("b"))
	expected := "cum_prod(b)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestCumMin(t *testing.T) {
	result := CumMin(Col("c"))
	expected := "cum_min(c)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestCumMax(t *testing.T) {
	result := CumMax(Col("d"))
	expected := "cum_max(d)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestCumSumExprString(t *testing.T) {
	result := CumSum(Column{Name: "value"})
	expected := "cum_sum(value)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestCumSumAlias(t *testing.T) {
	result := CumSum(Col("a")).Alias("cum_a")
	expected := "cum_sum(a) AS cum_a"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}
