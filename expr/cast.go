package expr

import (
	"fmt"

	grizzarrows "github.com/ecelayes/grizz/internal/arrow"
)

type CastExpr struct {
	Expr  Expr
	Dtype grizzarrows.DataType
}

func (e CastExpr) String() string {
	return fmt.Sprintf("Cast(%s, %s)", e.Expr.String(), e.Dtype.Name())
}

func Cast(expr Expr, dtype grizzarrows.DataType) CastExpr {
	return CastExpr{Expr: expr, Dtype: dtype}
}
