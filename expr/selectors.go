package expr

import (
	"fmt"
	"path/filepath"
	"strings"
)

type SelectorType int

const (
	SelectorAll SelectorType = iota
	SelectorString
	SelectorNumeric
	SelectorBoolean
	SelectorByName
	SelectorContains
	SelectorStartsWith
	SelectorEndsWith
)

type Selector struct {
	Type    SelectorType
	Pattern string
}

func (s Selector) String() string {
	switch s.Type {
	case SelectorAll:
		return "cs.All()"
	case SelectorString:
		return "cs.String()"
	case SelectorNumeric:
		return "cs.Numeric()"
	case SelectorBoolean:
		return "cs.Boolean()"
	case SelectorByName:
		return fmt.Sprintf("cs.ByName(%q)", s.Pattern)
	case SelectorContains:
		return fmt.Sprintf("cs.Contains(%q)", s.Pattern)
	case SelectorStartsWith:
		return fmt.Sprintf("cs.StartsWith(%q)", s.Pattern)
	case SelectorEndsWith:
		return fmt.Sprintf("cs.EndsWith(%q)", s.Pattern)
	default:
		return "Selector{}"
	}
}

func All() Selector {
	return Selector{Type: SelectorAll}
}

func String() Selector {
	return Selector{Type: SelectorString}
}

func Numeric() Selector {
	return Selector{Type: SelectorNumeric}
}

func Boolean() Selector {
	return Selector{Type: SelectorBoolean}
}

func ByName(pattern string) Selector {
	return Selector{Type: SelectorByName, Pattern: pattern}
}

func NameContains(substr string) Selector {
	return Selector{Type: SelectorContains, Pattern: substr}
}

func StartsWith(prefix string) Selector {
	return Selector{Type: SelectorStartsWith, Pattern: prefix}
}

func EndsWith(suffix string) Selector {
	return Selector{Type: SelectorEndsWith, Pattern: suffix}
}

type SelectorExpr struct {
	Selector Selector
}

func (s SelectorExpr) String() string {
	return s.Selector.String()
}

func (s SelectorExpr) Alias(name string) AliasExpr {
	return AliasExpr{Expr: s, Alias: name}
}

var cs selectorAPI

type selectorAPI struct{}

func (selectorAPI) All() Selector {
	return All()
}

func (selectorAPI) String() Selector {
	return String()
}

func (selectorAPI) Numeric() Selector {
	return Numeric()
}

func (selectorAPI) Boolean() Selector {
	return Boolean()
}

func (selectorAPI) ByName(pattern string) Selector {
	return ByName(pattern)
}

func (selectorAPI) Contains(substr string) Selector {
	return Selector{Type: SelectorContains, Pattern: substr}
}

func (selectorAPI) StartsWith(prefix string) Selector {
	return StartsWith(prefix)
}

func (selectorAPI) EndsWith(suffix string) Selector {
	return EndsWith(suffix)
}

func ResolveSelector(selector Selector, columnNames []string, columnTypes []string) []string {
	var result []string

	for i, colName := range columnNames {
		colType := ""
		if i < len(columnTypes) {
			colType = columnTypes[i]
		}

		switch selector.Type {
		case SelectorAll:
			result = append(result, colName)

		case SelectorString:
			if colType == "string" || colType == "String" {
				result = append(result, colName)
			}

		case SelectorNumeric:
			if isNumericType(colType) {
				result = append(result, colName)
			}

		case SelectorBoolean:
			if colType == "bool" || colType == "boolean" || colType == "Boolean" {
				result = append(result, colName)
			}

		case SelectorByName:
			if matchPattern(colName, selector.Pattern) {
				result = append(result, colName)
			}

		case SelectorContains:
			if strings.Contains(colName, selector.Pattern) {
				result = append(result, colName)
			}

		case SelectorStartsWith:
			if strings.HasPrefix(colName, selector.Pattern) {
				result = append(result, colName)
			}

		case SelectorEndsWith:
			if strings.HasSuffix(colName, selector.Pattern) {
				result = append(result, colName)
			}
		}
	}

	return result
}

func isNumericType(typeName string) bool {
	numericTypes := map[string]bool{
		"int": true, "int8": true, "int16": true, "int32": true, "int64": true,
		"uint": true, "uint8": true, "uint16": true, "uint32": true, "uint64": true,
		"float": true, "float32": true, "float64": true,
		"Int8": true, "Int16": true, "Int32": true, "Int64": true,
		"UInt8": true, "UInt16": true, "UInt32": true, "UInt64": true,
		"Float32": true, "Float64": true,
	}
	return numericTypes[typeName]
}

func matchPattern(name, pattern string) bool {
	escaped := strings.ReplaceAll(pattern, ".", "\\.")
	escaped = strings.ReplaceAll(escaped, "[", "\\[")
	escaped = strings.ReplaceAll(escaped, "]", "\\]")

	parts := strings.Split(escaped, "*")
	if len(parts) == 1 {
		return name == pattern
	}

	var regexPattern string
	for i, part := range parts {
		if i > 0 {
			regexPattern += ".*"
		}
		regexPattern += part
	}

	matched, err := filepath.Match(pattern, name)
	if err != nil {
		return false
	}
	return matched
}
