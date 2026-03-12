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

type YearExpr struct {
	Expr Expr
}

func (e YearExpr) String() string {
	return fmt.Sprintf("year(%s)", e.Expr.String())
}

func (e YearExpr) Alias(name string) AliasExpr {
	return AliasExpr{Expr: e, Alias: name}
}

type MonthExpr struct {
	Expr Expr
}

func (e MonthExpr) String() string {
	return fmt.Sprintf("month(%s)", e.Expr.String())
}

func (e MonthExpr) Alias(name string) AliasExpr {
	return AliasExpr{Expr: e, Alias: name}
}

type DayExpr struct {
	Expr Expr
}

func (e DayExpr) String() string {
	return fmt.Sprintf("day(%s)", e.Expr.String())
}

func (e DayExpr) Alias(name string) AliasExpr {
	return AliasExpr{Expr: e, Alias: name}
}

type HourExpr struct {
	Expr Expr
}

func (e HourExpr) String() string {
	return fmt.Sprintf("hour(%s)", e.Expr.String())
}

func (e HourExpr) Alias(name string) AliasExpr {
	return AliasExpr{Expr: e, Alias: name}
}

type MinuteExpr struct {
	Expr Expr
}

func (e MinuteExpr) String() string {
	return fmt.Sprintf("minute(%s)", e.Expr.String())
}

func (e MinuteExpr) Alias(name string) AliasExpr {
	return AliasExpr{Expr: e, Alias: name}
}

type SecondExpr struct {
	Expr Expr
}

func (e SecondExpr) String() string {
	return fmt.Sprintf("second(%s)", e.Expr.String())
}

func (e SecondExpr) Alias(name string) AliasExpr {
	return AliasExpr{Expr: e, Alias: name}
}

type WeekdayExpr struct {
	Expr Expr
}

func (e WeekdayExpr) String() string {
	return fmt.Sprintf("weekday(%s)", e.Expr.String())
}

func (e WeekdayExpr) Alias(name string) AliasExpr {
	return AliasExpr{Expr: e, Alias: name}
}

func Year(e Expr) YearExpr {
	return YearExpr{Expr: e}
}

func Month(e Expr) MonthExpr {
	return MonthExpr{Expr: e}
}

func Day(e Expr) DayExpr {
	return DayExpr{Expr: e}
}

func Hour(e Expr) HourExpr {
	return HourExpr{Expr: e}
}

func Minute(e Expr) MinuteExpr {
	return MinuteExpr{Expr: e}
}

func Second(e Expr) SecondExpr {
	return SecondExpr{Expr: e}
}

func Weekday(e Expr) WeekdayExpr {
	return WeekdayExpr{Expr: e}
}

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
