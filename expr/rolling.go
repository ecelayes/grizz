package expr

import "fmt"

type RollingSumExpr struct {
	Expr       Expr
	WindowSize int
	MinPeriods int
}

func (e RollingSumExpr) String() string {
	return fmt.Sprintf("rolling_sum(%s, %d, %d)", e.Expr.String(), e.WindowSize, e.MinPeriods)
}

func (e RollingSumExpr) Alias(name string) AliasExpr {
	return AliasExpr{Expr: e, Alias: name}
}

type RollingMeanExpr struct {
	Expr       Expr
	WindowSize int
	MinPeriods int
}

func (e RollingMeanExpr) String() string {
	return fmt.Sprintf("rolling_mean(%s, %d, %d)", e.Expr.String(), e.WindowSize, e.MinPeriods)
}

func (e RollingMeanExpr) Alias(name string) AliasExpr {
	return AliasExpr{Expr: e, Alias: name}
}

type RollingMinExpr struct {
	Expr       Expr
	WindowSize int
	MinPeriods int
}

func (e RollingMinExpr) String() string {
	return fmt.Sprintf("rolling_min(%s, %d, %d)", e.Expr.String(), e.WindowSize, e.MinPeriods)
}

func (e RollingMinExpr) Alias(name string) AliasExpr {
	return AliasExpr{Expr: e, Alias: name}
}

type RollingMaxExpr struct {
	Expr       Expr
	WindowSize int
	MinPeriods int
}

func (e RollingMaxExpr) String() string {
	return fmt.Sprintf("rolling_max(%s, %d, %d)", e.Expr.String(), e.WindowSize, e.MinPeriods)
}

func (e RollingMaxExpr) Alias(name string) AliasExpr {
	return AliasExpr{Expr: e, Alias: name}
}

func RollingSum(e Expr, windowSize int, minPeriods int) RollingSumExpr {
	if minPeriods == 0 {
		minPeriods = windowSize
	}
	return RollingSumExpr{Expr: e, WindowSize: windowSize, MinPeriods: minPeriods}
}

func RollingMean(e Expr, windowSize int, minPeriods int) RollingMeanExpr {
	if minPeriods == 0 {
		minPeriods = windowSize
	}
	return RollingMeanExpr{Expr: e, WindowSize: windowSize, MinPeriods: minPeriods}
}

func RollingMin(e Expr, windowSize int, minPeriods int) RollingMinExpr {
	if minPeriods == 0 {
		minPeriods = windowSize
	}
	return RollingMinExpr{Expr: e, WindowSize: windowSize, MinPeriods: minPeriods}
}

func RollingMax(e Expr, windowSize int, minPeriods int) RollingMaxExpr {
	if minPeriods == 0 {
		minPeriods = windowSize
	}
	return RollingMaxExpr{Expr: e, WindowSize: windowSize, MinPeriods: minPeriods}
}
