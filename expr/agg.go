package expr

import "fmt"

type AggFunc string

const (
	SumAgg   AggFunc = "Sum"
	MeanAgg  AggFunc = "Mean"
	CountAgg AggFunc = "Count"
)

type Aggregation struct {
	Func AggFunc
	Expr Expr
}

func (a Aggregation) String() string {
	return fmt.Sprintf("%s(%s)", a.Func, a.Expr.String())
}

func Sum(colName string) Aggregation {
	return Aggregation{Func: SumAgg, Expr: Col(colName)}
}

func Mean(colName string) Aggregation {
	return Aggregation{Func: MeanAgg, Expr: Col(colName)}
}

func Count(colName string) Aggregation {
	return Aggregation{Func: CountAgg, Expr: Col(colName)}
}
