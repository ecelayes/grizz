package expr

import "fmt"

type CumSumExpr struct {
	Expr Expr
}

func (e CumSumExpr) String() string {
	return fmt.Sprintf("cum_sum(%s)", e.Expr.String())
}

func (e CumSumExpr) Alias(name string) AliasExpr {
	return AliasExpr{Expr: e, Alias: name}
}

type CumProdExpr struct {
	Expr Expr
}

func (e CumProdExpr) String() string {
	return fmt.Sprintf("cum_prod(%s)", e.Expr.String())
}

func (e CumProdExpr) Alias(name string) AliasExpr {
	return AliasExpr{Expr: e, Alias: name}
}

type CumMinExpr struct {
	Expr Expr
}

func (e CumMinExpr) String() string {
	return fmt.Sprintf("cum_min(%s)", e.Expr.String())
}

func (e CumMinExpr) Alias(name string) AliasExpr {
	return AliasExpr{Expr: e, Alias: name}
}

type CumMaxExpr struct {
	Expr Expr
}

func (e CumMaxExpr) String() string {
	return fmt.Sprintf("cum_max(%s)", e.Expr.String())
}

func (e CumMaxExpr) Alias(name string) AliasExpr {
	return AliasExpr{Expr: e, Alias: name}
}

func CumSum(e Expr) CumSumExpr {
	return CumSumExpr{Expr: e}
}

func CumProd(e Expr) CumProdExpr {
	return CumProdExpr{Expr: e}
}

func CumMin(e Expr) CumMinExpr {
	return CumMinExpr{Expr: e}
}

func CumMax(e Expr) CumMaxExpr {
	return CumMaxExpr{Expr: e}
}
