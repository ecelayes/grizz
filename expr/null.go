package expr

import "fmt"

type IsNullExpr struct {
	Expr Expr
}

func (e IsNullExpr) String() string {
	return fmt.Sprintf("IsNull(%s)", e.Expr.String())
}

type IsNotNullExpr struct {
	Expr Expr
}

func (e IsNotNullExpr) String() string {
	return fmt.Sprintf("IsNotNull(%s)", e.Expr.String())
}

type FillNullExpr struct {
	Expr  Expr
	Value Expr
}

func (e FillNullExpr) String() string {
	return fmt.Sprintf("FillNull(%s, %s)", e.Expr.String(), e.Value.String())
}

type CoalesceExpr struct {
	Exprs []Expr
}

func (e CoalesceExpr) String() string {
	var strs []string
	for _, expr := range e.Exprs {
		strs = append(strs, expr.String())
	}
	return fmt.Sprintf("Coalesce(%s)", joinStrings(strs, ", "))
}

func joinStrings(strs []string, sep string) string {
	result := ""
	for i, s := range strs {
		if i > 0 {
			result += sep
		}
		result += s
	}
	return result
}

func IsNull(e Expr) IsNullExpr {
	return IsNullExpr{Expr: e}
}

func IsNotNull(e Expr) IsNotNullExpr {
	return IsNotNullExpr{Expr: e}
}

func FillNull(e Expr, value Expr) FillNullExpr {
	return FillNullExpr{Expr: e, Value: value}
}

func Coalesce(exprs ...Expr) CoalesceExpr {
	return CoalesceExpr{Exprs: exprs}
}
