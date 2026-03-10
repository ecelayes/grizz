package expr

import "fmt"

type WindowFunc string

const (
	FuncRowNumber WindowFunc = "row_number"
	FuncRank      WindowFunc = "rank"
	FuncLead      WindowFunc = "lead"
	FuncLag       WindowFunc = "lag"
)

type WindowExpr struct {
	Func    WindowFunc
	Expr    Expr
	Offset  int
	PartBy  []Expr
	OrderBy []Expr
}

func (e WindowExpr) String() string {
	offsetStr := ""
	if e.Offset > 0 {
		offsetStr = fmt.Sprintf(", %d", e.Offset)
	}
	exprStr := ""
	if e.Expr != nil {
		exprStr = e.Expr.String()
	}
	return fmt.Sprintf("%s(%s%s)", e.Func, exprStr, offsetStr)
}

func RowNumber() WindowExpr {
	return WindowExpr{Func: FuncRowNumber}
}

func Rank() WindowExpr {
	return WindowExpr{Func: FuncRank}
}

func Lead(expr Expr, offset int) WindowExpr {
	return WindowExpr{Func: FuncLead, Expr: expr, Offset: offset}
}

func Lag(expr Expr, offset int) WindowExpr {
	return WindowExpr{Func: FuncLag, Expr: expr, Offset: offset}
}
