package expr

import "fmt"

type WhenExpr struct {
	Condition Expr
}

type ThenExpr struct {
	WhenExpr WhenExpr
	Value    Expr
}

type OtherwiseExpr struct {
	ThenExpr  ThenExpr
	Otherwise Expr
}

func (e WhenExpr) String() string {
	return fmt.Sprintf("When(%s)", e.Condition.String())
}

func (e ThenExpr) String() string {
	return fmt.Sprintf("Then(%s)", e.Value.String())
}

func (e ThenExpr) Alias(name string) AliasExpr {
	return AliasExpr{Expr: e, Alias: name}
}

func (e OtherwiseExpr) String() string {
	return fmt.Sprintf("When(%s).Then(%s).Otherwise(%s)",
		e.ThenExpr.WhenExpr.Condition.String(),
		e.ThenExpr.Value.String(),
		e.Otherwise.String())
}

func (e OtherwiseExpr) Alias(name string) AliasExpr {
	return AliasExpr{Expr: e, Alias: name}
}

func When(condition Expr) WhenExpr {
	return WhenExpr{Condition: condition}
}

func (w WhenExpr) Then(value Expr) ThenExpr {
	return ThenExpr{WhenExpr: w, Value: value}
}

func (t ThenExpr) Otherwise(otherwise Expr) OtherwiseExpr {
	return OtherwiseExpr{ThenExpr: t, Otherwise: otherwise}
}
