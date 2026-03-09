package engine

import (
	"errors"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func Execute(plan dataframe.LogicalPlan) (*dataframe.DataFrame, error) {
	switch p := plan.(type) {
	case dataframe.ScanPlan:
		return p.DataFrame, nil

	case dataframe.FilterPlan:
		inputDF, err := Execute(p.Input)
		if err != nil {
			return nil, err
		}
		
		mask, err := evaluateCondition(inputDF, p.Condition)
		if err != nil {
			return nil, err
		}

		return applyMask(inputDF, mask)

	case dataframe.SelectPlan:
		inputDF, err := Execute(p.Input)
		if err != nil {
			return nil, err
		}
		return applyProjection(inputDF, p.Columns)

	case dataframe.GroupByPlan:
		inputDF, err := Execute(p.Input)
		if err != nil {
			return nil, err
		}
		return applyGroupBy(inputDF, p.Keys, p.Aggs)

	case dataframe.OrderByPlan:
		inputDF, err := Execute(p.Input)
		if err != nil {
			return nil, err
		}
		return applyOrderBy(inputDF, p.Column, p.Descending)

	case dataframe.LimitPlan:
		inputDF, err := Execute(p.Input)
		if err != nil {
			return nil, err
		}
		return applyLimit(inputDF, p.Limit)

	case dataframe.JoinPlan:
		leftDF, err := Execute(p.Left)
		if err != nil {
			return nil, err
		}
		rightDF, err := Execute(p.Right)
		if err != nil {
			return nil, err
		}
		return applyJoin(leftDF, rightDF, p.On, p.How)

	default:
		return nil, errors.New("unknown logical plan node")
	}
}

func evaluateCondition(df *dataframe.DataFrame, condition expr.Expr) ([]bool, error) {
	binOp, ok := condition.(expr.BinaryOp)
	if !ok {
		return nil, errors.New("only binary operations are supported currently")
	}

	colExpr, ok1 := binOp.Left.(expr.Column)
	litExpr, ok2 := binOp.Right.(expr.Literal)

	if !ok1 || !ok2 {
		return nil, errors.New("unsupported expression format: expected Column OP Literal")
	}

	col, err := df.ColByName(colExpr.Name)
	if err != nil {
		return nil, err
	}

	mask := make([]bool, col.Len())

	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			mask[i] = false
			continue
		}

		switch binOp.Op {
		case "==":
			if strCol, ok := col.(*series.StringSeries); ok {
				mask[i] = strCol.Value(i) == litExpr.Value.(string)
			} else if intCol, ok := col.(*series.Int64Series); ok {
				var target int64
				switch v := litExpr.Value.(type) {
				case int:
					target = int64(v)
				case int64:
					target = v
				}
				mask[i] = intCol.Value(i) == target
			}
		case ">":
			if floatCol, ok := col.(*series.Float64Series); ok {
				mask[i] = floatCol.Value(i) > litExpr.Value.(float64)
			} else if intCol, ok := col.(*series.Int64Series); ok {
				var target int64
				switch v := litExpr.Value.(type) {
				case int:
					target = int64(v)
				case int64:
					target = v
				}
				mask[i] = intCol.Value(i) > target
			}
		default:
			return nil, errors.New("unsupported operator")
		}
	}

	return mask, nil
}

func applyMask(df *dataframe.DataFrame, mask []bool) (*dataframe.DataFrame, error) {
	result := dataframe.New()
	alloc := memory.DefaultAllocator()

	for i := 0; i < df.NumCols(); i++ {
		col, _ := df.Col(i)
		
		switch typedCol := col.(type) {
		case *series.StringSeries:
			var filtered []string
			for j, keep := range mask {
				if keep {
					filtered = append(filtered, typedCol.Value(j))
				}
			}
			newCol := series.NewStringSeries(typedCol.Name(), alloc, filtered, nil)
			result.AddSeries(newCol)

		case *series.Int64Series:
			var filtered []int64
			for j, keep := range mask {
				if keep {
					filtered = append(filtered, typedCol.Value(j))
				}
			}
			result.AddSeries(series.NewInt64Series(typedCol.Name(), alloc, filtered, nil))
			
		case *series.Float64Series:
			var filtered []float64
			for j, keep := range mask {
				if keep {
					filtered = append(filtered, typedCol.Value(j))
				}
			}
			newCol := series.NewFloat64Series(typedCol.Name(), alloc, filtered, nil)
			result.AddSeries(newCol)

		case *series.BooleanSeries:
			var filtered []bool
			for j, keep := range mask {
				if keep {
					filtered = append(filtered, typedCol.Value(j))
				}
			}
			newCol := series.NewBooleanSeries(typedCol.Name(), alloc, filtered, nil)
			result.AddSeries(newCol)
		}
	}

	return result, nil
}

func applyProjection(df *dataframe.DataFrame, columns []expr.Expr) (*dataframe.DataFrame, error) {
	result := dataframe.New()
	alloc := memory.DefaultAllocator()

	for _, exprCol := range columns {
		colExpr, ok := exprCol.(expr.Column)
		if !ok {
			return nil, errors.New("select only supports column expressions currently")
		}

		originalCol, err := df.ColByName(colExpr.Name)
		if err != nil {
			return nil, err
		}

		switch typedCol := originalCol.(type) {
		case *series.StringSeries:
			var copied []string
			for j := 0; j < typedCol.Len(); j++ {
				copied = append(copied, typedCol.Value(j))
			}
			newCol := series.NewStringSeries(typedCol.Name(), alloc, copied, nil)
			result.AddSeries(newCol)

		case *series.Int64Series:
			var copied []int64
			for j := 0; j < typedCol.Len(); j++ {
				copied = append(copied, typedCol.Value(j))
			}
			result.AddSeries(series.NewInt64Series(typedCol.Name(), alloc, copied, nil))

		case *series.Float64Series:
			var copied []float64
			for j := 0; j < typedCol.Len(); j++ {
				copied = append(copied, typedCol.Value(j))
			}
			newCol := series.NewFloat64Series(typedCol.Name(), alloc, copied, nil)
			result.AddSeries(newCol)

		case *series.BooleanSeries:
			var copied []bool
			for j := 0; j < typedCol.Len(); j++ {
				copied = append(copied, typedCol.Value(j))
			}
			newCol := series.NewBooleanSeries(typedCol.Name(), alloc, copied, nil)
			result.AddSeries(newCol)
		}
	}

	return result, nil
}
