package expr

import "testing"

func TestAlias(t *testing.T) {
	result := Alias(Column{Name: "age"}, "Age")
	expected := "age AS Age"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestAliasExprString(t *testing.T) {
	result := Alias(Col("name"), "full_name")
	expected := "name AS full_name"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestAliasExprStruct(t *testing.T) {
	alias := AliasExpr{
		Expr:  Column{Name: "value"},
		Alias: "result",
	}
	if alias.Alias != "result" {
		t.Errorf("Expected alias 'result', got %s", alias.Alias)
	}
}

func TestAliasWithExpression(t *testing.T) {
	result := Alias(Col("a").Add(Lit(10)), "sum_a")
	expected := "(a + 10) AS sum_a"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}
