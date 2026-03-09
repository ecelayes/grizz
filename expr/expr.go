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

func Col(name string) Column {
	return Column{Name: name}
}

func Lit(value any) Literal {
	return Literal{Value: value}
}
