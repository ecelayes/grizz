package expr

import "fmt"

type EwmMeanExpr struct {
	Expr       Expr
	Alpha      float64
	Adjust     bool
	MinPeriods int
}

func (e EwmMeanExpr) String() string {
	return fmt.Sprintf("ewm_mean(%s, alpha=%.2f)", e.Expr.String(), e.Alpha)
}

func (e EwmMeanExpr) Alias(name string) AliasExpr {
	return AliasExpr{Expr: e, Alias: name}
}

func EwmMean(e Expr) EwmMeanExpr {
	return EwmMeanExpr{Expr: e, Alpha: 0.5, Adjust: true, MinPeriods: 1}
}

func EwmMeanAlpha(e Expr, alpha float64) EwmMeanExpr {
	return EwmMeanExpr{Expr: e, Alpha: alpha, Adjust: true, MinPeriods: 1}
}

func EwmMeanAlphaMinPeriods(e Expr, alpha float64, minPeriods int) EwmMeanExpr {
	return EwmMeanExpr{Expr: e, Alpha: alpha, Adjust: true, MinPeriods: minPeriods}
}
