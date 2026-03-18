package expr

import "fmt"

type AliasExpr struct {
	Expr  Expr
	Alias string
}

func (a AliasExpr) String() string {
	return fmt.Sprintf("%s AS %s", a.Expr.String(), a.Alias)
}

func Alias(expr Expr, name string) AliasExpr {
	return AliasExpr{Expr: expr, Alias: name}
}
