package expr

import "fmt"

type ExplodeExpr struct {
	Expr      Expr
	Delimiter string
}

func (e ExplodeExpr) String() string {
	return fmt.Sprintf("explode(%s, '%s')", e.Expr.String(), e.Delimiter)
}

func (e ExplodeExpr) Alias(name string) AliasExpr {
	return AliasExpr{Expr: e, Alias: name}
}

func Explode(e Expr, delimiter string) ExplodeExpr {
	return ExplodeExpr{Expr: e, Delimiter: delimiter}
}
