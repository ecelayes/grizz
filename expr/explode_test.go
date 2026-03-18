package expr

import "testing"

func TestExplode(t *testing.T) {
	result := Explode(Col("tags"), ",")
	expected := "explode(tags, ',')"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestExplodeExprString(t *testing.T) {
	result := Explode(Column{Name: "values"}, ";")
	expected := "explode(values, ';')"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestExplodeExprStruct(t *testing.T) {
	expr := ExplodeExpr{
		Expr:      Column{Name: "data"},
		Delimiter: ",",
	}
	if expr.Delimiter != "," {
		t.Errorf("Expected delimiter ',', got %s", expr.Delimiter)
	}
}

func TestExplodeWithSpaceDelimiter(t *testing.T) {
	result := Explode(Col("words"), " ")
	expected := "explode(words, ' ')"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}
