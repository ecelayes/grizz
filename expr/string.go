package expr

import "fmt"

type ContainsExpr struct {
	Expr   Expr
	Substr Expr
}

func (e ContainsExpr) String() string {
	return fmt.Sprintf("Contains(%s, %s)", e.Expr.String(), e.Substr.String())
}

type ReplaceExpr struct {
	Expr Expr
	Old  Expr
	New  Expr
}

func (e ReplaceExpr) String() string {
	return fmt.Sprintf("Replace(%s, %s, %s)", e.Expr.String(), e.Old.String(), e.New.String())
}

type UpperExpr struct {
	Expr Expr
}

func (e UpperExpr) String() string {
	return fmt.Sprintf("Upper(%s)", e.Expr.String())
}

type LowerExpr struct {
	Expr Expr
}

func (e LowerExpr) String() string {
	return fmt.Sprintf("Lower(%s)", e.Expr.String())
}

type StripExpr struct {
	Expr Expr
}

func (e StripExpr) String() string {
	return fmt.Sprintf("Strip(%s)", e.Expr.String())
}

func Contains(expr Expr, substr Expr) ContainsExpr {
	return ContainsExpr{Expr: expr, Substr: substr}
}

func Replace(expr Expr, old Expr, new Expr) ReplaceExpr {
	return ReplaceExpr{Expr: expr, Old: old, New: new}
}

func Upper(expr Expr) UpperExpr {
	return UpperExpr{Expr: expr}
}

func Lower(expr Expr) LowerExpr {
	return LowerExpr{Expr: expr}
}

func Strip(expr Expr) StripExpr {
	return StripExpr{Expr: expr}
}
