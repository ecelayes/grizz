package expr

import "fmt"

type AggFunc string

const (
	SumAgg      AggFunc = "Sum"
	MeanAgg     AggFunc = "Mean"
	CountAgg    AggFunc = "Count"
	MinAgg      AggFunc = "Min"
	MaxAgg      AggFunc = "Max"
	StdAgg      AggFunc = "Std"
	VarAgg      AggFunc = "Var"
	MedianAgg   AggFunc = "Median"
	QuantileAgg AggFunc = "Quantile"
	NUniqueAgg  AggFunc = "NUnique"
	FirstAgg    AggFunc = "First"
	LastAgg     AggFunc = "Last"
	ArgMinAgg   AggFunc = "ArgMin"
	ArgMaxAgg   AggFunc = "ArgMax"
)

type Aggregation struct {
	Func  AggFunc
	Expr  Expr
	Param float64
}

func (a Aggregation) String() string {
	if a.Func == QuantileAgg {
		return fmt.Sprintf("%s(%s, %.2f)", a.Func, a.Expr.String(), a.Param)
	}
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

func Min(colName string) Aggregation {
	return Aggregation{Func: MinAgg, Expr: Col(colName)}
}

func Max(colName string) Aggregation {
	return Aggregation{Func: MaxAgg, Expr: Col(colName)}
}

func Std(colName string) Aggregation {
	return Aggregation{Func: StdAgg, Expr: Col(colName)}
}

func Var(colName string) Aggregation {
	return Aggregation{Func: VarAgg, Expr: Col(colName)}
}

func Median(colName string) Aggregation {
	return Aggregation{Func: MedianAgg, Expr: Col(colName)}
}

func Quantile(colName string, q float64) Aggregation {
	return Aggregation{Func: QuantileAgg, Expr: Col(colName), Param: q}
}

func NUnique(colName string) Aggregation {
	return Aggregation{Func: NUniqueAgg, Expr: Col(colName)}
}

func First(colName string) Aggregation {
	return Aggregation{Func: FirstAgg, Expr: Col(colName)}
}

func Last(colName string) Aggregation {
	return Aggregation{Func: LastAgg, Expr: Col(colName)}
}

func ArgMin(colName string) Aggregation {
	return Aggregation{Func: ArgMinAgg, Expr: Col(colName)}
}

func ArgMax(colName string) Aggregation {
	return Aggregation{Func: ArgMaxAgg, Expr: Col(colName)}
}
