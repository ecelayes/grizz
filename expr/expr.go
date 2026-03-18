package expr

import "fmt"

type Expr interface {
	String() string
}

type Column struct {
	Name string
}

func (c Column) String() string {
	return c.Name
}

func (c Column) Eq(other Expr) BinaryOp {
	return BinaryOp{Left: c, Op: "==", Right: other}
}

func (c Column) Gt(other Expr) BinaryOp {
	return BinaryOp{Left: c, Op: ">", Right: other}
}

func (c Column) Lt(other Expr) BinaryOp {
	return BinaryOp{Left: c, Op: "<", Right: other}
}

func (c Column) LtEq(other Expr) BinaryOp {
	return BinaryOp{Left: c, Op: "<=", Right: other}
}

func (c Column) GtEq(other Expr) BinaryOp {
	return BinaryOp{Left: c, Op: ">=", Right: other}
}

func (c Column) Ne(other Expr) BinaryOp {
	return BinaryOp{Left: c, Op: "!=", Right: other}
}

func (c Column) And(other Expr) LogicalOp {
	return LogicalOp{Left: c, Op: "And", Right: other}
}

func (c Column) Or(other Expr) LogicalOp {
	return LogicalOp{Left: c, Op: "Or", Right: other}
}

func (c Column) IsIn(values []any) IsInExpr {
	literals := make([]Literal, len(values))
	for i, v := range values {
		literals[i] = Literal{Value: v}
	}
	return IsInExpr{Expr: c, Values: literals}
}

func (c Column) Between(lower, upper any) BetweenExpr {
	return BetweenExpr{
		Expr:  c,
		Lower: Literal{Value: lower},
		Upper: Literal{Value: upper},
	}
}

type IsInExpr struct {
	Expr   Expr
	Values []Literal
}

func (e IsInExpr) String() string {
	return "is_in"
}

type BetweenExpr struct {
	Expr  Expr
	Lower Literal
	Upper Literal
}

func (e BetweenExpr) String() string {
	return fmt.Sprintf("between(%s, %s)", e.Lower.String(), e.Upper.String())
}

func (e BetweenExpr) Alias(name string) AliasExpr {
	return AliasExpr{Expr: e, Alias: name}
}

type Literal struct {
	Value any
}

func (l Literal) String() string {
	switch v := l.Value.(type) {
	case string:
		return fmt.Sprintf(`"%s"`, v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

type BinaryOp struct {
	Left  Expr
	Op    string
	Right Expr
}

func (b BinaryOp) String() string {
	return fmt.Sprintf("(%s %s %s)", b.Left.String(), b.Op, b.Right.String())
}

func (b BinaryOp) And(other Expr) LogicalOp {
	return LogicalOp{Left: b, Op: "And", Right: other}
}

func (b BinaryOp) Or(other Expr) LogicalOp {
	return LogicalOp{Left: b, Op: "Or", Right: other}
}

type LogicalOp struct {
	Left  Expr
	Op    string
	Right Expr
}

func (l LogicalOp) String() string {
	return fmt.Sprintf("(%s %s %s)", l.Left.String(), l.Op, l.Right.String())
}

type NotOp struct {
	Expr Expr
}

func (n NotOp) String() string {
	return fmt.Sprintf("Not(%s)", n.Expr.String())
}

func And(left, right Expr) LogicalOp {
	return LogicalOp{Left: left, Op: "And", Right: right}
}

func Or(left, right Expr) LogicalOp {
	return LogicalOp{Left: left, Op: "Or", Right: right}
}

func Not(e Expr) NotOp {
	return NotOp{Expr: e}
}

func Col(name string) Column {
	return Column{Name: name}
}

func Lit(value any) Literal {
	return Literal{Value: value}
}

type ArithmeticOp struct {
	Left  Expr
	Op    string
	Right Expr
}

func (a ArithmeticOp) String() string {
	return fmt.Sprintf("(%s %s %s)", a.Left.String(), a.Op, a.Right.String())
}

func (c Column) Add(other Expr) ArithmeticOp {
	return ArithmeticOp{Left: c, Op: "+", Right: other}
}

func (c Column) Sub(other Expr) ArithmeticOp {
	return ArithmeticOp{Left: c, Op: "-", Right: other}
}

func (c Column) Mul(other Expr) ArithmeticOp {
	return ArithmeticOp{Left: c, Op: "*", Right: other}
}

func (c Column) Div(other Expr) ArithmeticOp {
	return ArithmeticOp{Left: c, Op: "/", Right: other}
}

func (c Column) Alias(name string) AliasExpr {
	return AliasExpr{Expr: c, Alias: name}
}

func (a ArithmeticOp) Add(other Expr) ArithmeticOp {
	return ArithmeticOp{Left: a, Op: "+", Right: other}
}

func (a ArithmeticOp) Sub(other Expr) ArithmeticOp {
	return ArithmeticOp{Left: a, Op: "-", Right: other}
}

func (a ArithmeticOp) Mul(other Expr) ArithmeticOp {
	return ArithmeticOp{Left: a, Op: "*", Right: other}
}

func (a ArithmeticOp) Div(other Expr) ArithmeticOp {
	return ArithmeticOp{Left: a, Op: "/", Right: other}
}

func (a ArithmeticOp) Alias(name string) AliasExpr {
	return AliasExpr{Expr: a, Alias: name}
}

func (b BinaryOp) Alias(name string) AliasExpr {
	return AliasExpr{Expr: b, Alias: name}
}
