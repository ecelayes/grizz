package expr

import "fmt"

type TruncateExpr struct {
	Expr   Expr
	Period string
}

func (e TruncateExpr) String() string {
	return fmt.Sprintf("truncate(%s, '%s')", e.Expr.String(), e.Period)
}

func (e TruncateExpr) Alias(name string) AliasExpr {
	return AliasExpr{Expr: e, Alias: name}
}

func Truncate(e Expr, period string) TruncateExpr {
	return TruncateExpr{Expr: e, Period: period}
}
