package expr

import (
	"testing"
)

func TestSelectorAll(t *testing.T) {
	sel := All()
	if sel.Type != SelectorAll {
		t.Errorf("Expected SelectorAll, got %v", sel.Type)
	}
	if sel.String() != "cs.All()" {
		t.Errorf("Expected cs.All(), got %s", sel.String())
	}
}

func TestSelectorString(t *testing.T) {
	sel := String()
	if sel.Type != SelectorString {
		t.Errorf("Expected SelectorString, got %v", sel.Type)
	}
	if sel.String() != "cs.String()" {
		t.Errorf("Expected cs.String(), got %s", sel.String())
	}
}

func TestSelectorNumeric(t *testing.T) {
	sel := Numeric()
	if sel.Type != SelectorNumeric {
		t.Errorf("Expected SelectorNumeric, got %v", sel.Type)
	}
	if sel.String() != "cs.Numeric()" {
		t.Errorf("Expected cs.Numeric(), got %s", sel.String())
	}
}

func TestSelectorBoolean(t *testing.T) {
	sel := Boolean()
	if sel.Type != SelectorBoolean {
		t.Errorf("Expected SelectorBoolean, got %v", sel.Type)
	}
	if sel.String() != "cs.Boolean()" {
		t.Errorf("Expected cs.Boolean(), got %s", sel.String())
	}
}

func TestSelectorByName(t *testing.T) {
	sel := ByName("col_*")
	if sel.Type != SelectorByName {
		t.Errorf("Expected SelectorByName, got %v", sel.Type)
	}
	if sel.Pattern != "col_*" {
		t.Errorf("Expected col_*, got %s", sel.Pattern)
	}
	if sel.String() != `cs.ByName("col_*")` {
		t.Errorf("Expected cs.ByName(\"col_*\"), got %s", sel.String())
	}
}

func TestSelectorContains(t *testing.T) {
	sel := NameContains("test")
	if sel.Type != SelectorContains {
		t.Errorf("Expected SelectorContains, got %v", sel.Type)
	}
	if sel.Pattern != "test" {
		t.Errorf("Expected test, got %s", sel.Pattern)
	}
	if sel.String() != `cs.Contains("test")` {
		t.Errorf("Expected cs.Contains(\"test\"), got %s", sel.String())
	}
}

func TestSelectorStartsWith(t *testing.T) {
	sel := StartsWith("prefix_")
	if sel.Type != SelectorStartsWith {
		t.Errorf("Expected SelectorStartsWith, got %v", sel.Type)
	}
	if sel.Pattern != "prefix_" {
		t.Errorf("Expected prefix_, got %s", sel.Pattern)
	}
	if sel.String() != `cs.StartsWith("prefix_")` {
		t.Errorf("Expected cs.StartsWith(\"prefix_\"), got %s", sel.String())
	}
}

func TestSelectorEndsWith(t *testing.T) {
	sel := EndsWith("_suffix")
	if sel.Type != SelectorEndsWith {
		t.Errorf("Expected SelectorEndsWith, got %v", sel.Type)
	}
	if sel.Pattern != "_suffix" {
		t.Errorf("Expected _suffix, got %s", sel.Pattern)
	}
	if sel.String() != `cs.EndsWith("_suffix")` {
		t.Errorf("Expected cs.EndsWith(\"_suffix\"), got %s", sel.String())
	}
}

func TestSelectorAPI(t *testing.T) {
	tests := []struct {
		name     string
		got      string
		expected string
	}{
		{"cs.All", cs.All().String(), "cs.All()"},
		{"cs.String", cs.String().String(), "cs.String()"},
		{"cs.Numeric", cs.Numeric().String(), "cs.Numeric()"},
		{"cs.Boolean", cs.Boolean().String(), "cs.Boolean()"},
		{"cs.ByName", cs.ByName("col_*").String(), `cs.ByName("col_*")`},
		{"cs.Contains", cs.Contains("test").String(), `cs.Contains("test")`},
		{"cs.StartsWith", cs.StartsWith("pre").String(), `cs.StartsWith("pre")`},
		{"cs.EndsWith", cs.EndsWith("suf").String(), `cs.EndsWith("suf")`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, tt.got)
			}
		})
	}
}

func TestResolveSelectorAll(t *testing.T) {
	colNames := []string{"a", "b", "c"}
	colTypes := []string{"int64", "string", "float64"}

	result := ResolveSelector(All(), colNames, colTypes)

	if len(result) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(result))
	}
}

func TestResolveSelectorString(t *testing.T) {
	colNames := []string{"name", "age", "city", "score"}
	colTypes := []string{"string", "int64", "string", "float64"}

	result := ResolveSelector(String(), colNames, colTypes)

	if len(result) != 2 {
		t.Errorf("Expected 2 string columns, got %d", len(result))
	}
	if result[0] != "name" || result[1] != "city" {
		t.Errorf("Expected [name city], got %v", result)
	}
}

func TestResolveSelectorNumeric(t *testing.T) {
	colNames := []string{"name", "age", "city", "score", "flag"}
	colTypes := []string{"string", "int64", "string", "float64", "bool"}

	result := ResolveSelector(Numeric(), colNames, colTypes)

	if len(result) != 2 {
		t.Errorf("Expected 2 numeric columns, got %d", len(result))
	}
	if result[0] != "age" || result[1] != "score" {
		t.Errorf("Expected [age score], got %v", result)
	}
}

func TestResolveSelectorBoolean(t *testing.T) {
	colNames := []string{"name", "flag", "city", "active"}
	colTypes := []string{"string", "bool", "string", "boolean"}

	result := ResolveSelector(Boolean(), colNames, colTypes)

	if len(result) != 2 {
		t.Errorf("Expected 2 boolean columns, got %d", len(result))
	}
	if result[0] != "flag" || result[1] != "active" {
		t.Errorf("Expected [flag active], got %v", result)
	}
}

func TestResolveSelectorByName(t *testing.T) {
	colNames := []string{"col_a", "col_b", "other", "col_c"}

	result := ResolveSelector(ByName("col_*"), colNames, nil)

	if len(result) != 3 {
		t.Errorf("Expected 3 columns matching col_*, got %d", len(result))
	}
}

func TestResolveSelectorContains(t *testing.T) {
	colNames := []string{"first_name", "last_name", "age", "city_name"}

	result := ResolveSelector(NameContains("name"), colNames, nil)

	if len(result) != 3 {
		t.Errorf("Expected 3 columns containing 'name', got %d", len(result))
	}
}

func TestResolveSelectorStartsWith(t *testing.T) {
	colNames := []string{"first_name", "last_name", "age", "score_value"}

	result := ResolveSelector(StartsWith("first"), colNames, nil)

	if len(result) != 1 {
		t.Errorf("Expected 1 column starting with 'first', got %d", len(result))
	}
	if result[0] != "first_name" {
		t.Errorf("Expected first_name, got %s", result[0])
	}
}

func TestResolveSelectorEndsWith(t *testing.T) {
	colNames := []string{"first_name", "last_name", "age", "score_value"}

	result := ResolveSelector(EndsWith("_name"), colNames, nil)

	if len(result) != 2 {
		t.Errorf("Expected 2 columns ending with '_name', got %d", len(result))
	}
}

func TestResolveSelectorByNameExactMatch(t *testing.T) {
	colNames := []string{"exact", "other", "exact_match"}

	result := ResolveSelector(ByName("exact"), colNames, nil)

	if len(result) != 1 {
		t.Errorf("Expected 1 column named 'exact', got %d", len(result))
	}
	if result[0] != "exact" {
		t.Errorf("Expected exact, got %s", result[0])
	}
}

func TestIsNumericType(t *testing.T) {
	numericTypes := []string{
		"int", "int8", "int16", "int32", "int64",
		"uint", "uint8", "uint16", "uint32", "uint64",
		"float", "float32", "float64",
		"Int8", "Int16", "Int32", "Int64",
		"UInt8", "UInt16", "UInt32", "UInt64",
		"Float32", "Float64",
	}

	for _, typ := range numericTypes {
		if !isNumericType(typ) {
			t.Errorf("Expected %s to be numeric", typ)
		}
	}

	nonNumericTypes := []string{"string", "bool", "boolean", "binary", "date"}
	for _, typ := range nonNumericTypes {
		if isNumericType(typ) {
			t.Errorf("Expected %s to NOT be numeric", typ)
		}
	}
}

func TestMatchPattern(t *testing.T) {
	tests := []struct {
		input    string
		pattern  string
		expected bool
	}{
		{"col_a", "col_a", true},
		{"col_a", "col_b", false},
		{"col_a", "col_*", true},
		{"col_abc_def", "col_*", true},
		{"first_name", "*_name", true},
		{"a123b", "a*b", true},
		{"mytestvalue", "*test*", true},
		{"other", "col_*", false},
		{"anything", "*", true},
	}

	for _, tt := range tests {
		t.Run(tt.input+"_"+tt.pattern, func(t *testing.T) {
			result := matchPattern(tt.input, tt.pattern)
			if result != tt.expected {
				t.Errorf("matchPattern(%s, %s) = %v, expected %v", tt.input, tt.pattern, result, tt.expected)
			}
		})
	}
}

func TestSelectorExprString(t *testing.T) {
	selExpr := SelectorExpr{Selector: All()}
	if selExpr.String() != "cs.All()" {
		t.Errorf("Expected cs.All(), got %s", selExpr.String())
	}
}

func TestSelectorExprAlias(t *testing.T) {
	selExpr := SelectorExpr{Selector: All()}
	aliasExpr := selExpr.Alias("new_name")

	if aliasExpr.Alias != "new_name" {
		t.Errorf("Expected alias 'new_name', got %s", aliasExpr.Alias)
	}
}
