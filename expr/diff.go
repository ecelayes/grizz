package expr

import "fmt"

type DiffExpr struct {
	Expr    Expr
	Periods int
}

func (e DiffExpr) String() string {
	if e.Periods == 1 {
		return fmt.Sprintf("diff(%s)", e.Expr.String())
	}
	return fmt.Sprintf("diff(%s, %d)", e.Expr.String(), e.Periods)
}

func (e DiffExpr) Alias(name string) AliasExpr {
	return AliasExpr{Expr: e, Alias: name}
}

type PctChangeExpr struct {
	Expr    Expr
	Periods int
}

func (e PctChangeExpr) String() string {
	if e.Periods == 1 {
		return fmt.Sprintf("pct_change(%s)", e.Expr.String())
	}
	return fmt.Sprintf("pct_change(%s, %d)", e.Expr.String(), e.Periods)
}

func (e PctChangeExpr) Alias(name string) AliasExpr {
	return AliasExpr{Expr: e, Alias: name}
}

func Diff(e Expr) DiffExpr {
	return DiffExpr{Expr: e, Periods: 1}
}

func DiffPeriods(e Expr, periods int) DiffExpr {
	return DiffExpr{Expr: e, Periods: periods}
}

func PctChange(e Expr) PctChangeExpr {
	return PctChangeExpr{Expr: e, Periods: 1}
}

func PctChangePeriods(e Expr, periods int) PctChangeExpr {
	return PctChangeExpr{Expr: e, Periods: periods}
}
