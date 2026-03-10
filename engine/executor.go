package engine

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

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

	case dataframe.WithColumnsPlan:
		inputDF, err := Execute(p.Input)
		if err != nil {
			return nil, err
		}
		return applyWithColumns(inputDF, p.Columns)

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

	case dataframe.TailPlan:
		inputDF, err := Execute(p.Input)
		if err != nil {
			return nil, err
		}
		return applyTail(inputDF, p.N)

	case dataframe.SamplePlan:
		inputDF, err := Execute(p.Input)
		if err != nil {
			return nil, err
		}
		return applySample(inputDF, p.N, p.Frac, p.Replace)

	case dataframe.JoinPlan:
		leftDF, err := Execute(p.Left)
		if err != nil {
			return nil, err
		}
		rightDF, err := Execute(p.Right)
		if err != nil {
			return nil, err
		}
		onCol := p.On
		if len(p.OnCols) > 0 {
			onCol = p.OnCols[0]
		}
		return applyJoin(leftDF, rightDF, onCol, p.How)

	case dataframe.DropNullsPlan:
		inputDF, err := Execute(p.Input)
		if err != nil {
			return nil, err
		}
		return applyDropNulls(inputDF)

	case dataframe.DistinctPlan:
		inputDF, err := Execute(p.Input)
		if err != nil {
			return nil, err
		}
		return applyDistinct(inputDF)

	case dataframe.WindowPlan:
		inputDF, err := Execute(p.Input)
		if err != nil {
			return nil, err
		}
		return applyWindow(inputDF, p.Func, p.PartBy, p.OrderBy)

	default:
		return nil, errors.New("unknown logical plan node")
	}
}

func evaluateCondition(df *dataframe.DataFrame, condition expr.Expr) ([]bool, error) {
	switch cond := condition.(type) {
	case expr.IsNullExpr:
		colExpr, ok := cond.Expr.(expr.Column)
		if !ok {
			return nil, errors.New("IsNull only supports column expressions")
		}
		col, err := df.ColByName(colExpr.Name)
		if err != nil {
			return nil, err
		}
		mask := make([]bool, col.Len())
		for i := 0; i < col.Len(); i++ {
			mask[i] = col.IsNull(i)
		}
		return mask, nil

	case expr.IsNotNullExpr:
		colExpr, ok := cond.Expr.(expr.Column)
		if !ok {
			return nil, errors.New("IsNotNull only supports column expressions")
		}
		col, err := df.ColByName(colExpr.Name)
		if err != nil {
			return nil, err
		}
		mask := make([]bool, col.Len())
		for i := 0; i < col.Len(); i++ {
			mask[i] = !col.IsNull(i)
		}
		return mask, nil

	case expr.LogicalOp:
		leftMask, err := evaluateCondition(df, cond.Left)
		if err != nil {
			return nil, err
		}
		rightMask, err := evaluateCondition(df, cond.Right)
		if err != nil {
			return nil, err
		}

		result := make([]bool, len(leftMask))
		for i := range result {
			if cond.Op == "And" {
				result[i] = leftMask[i] && rightMask[i]
			} else if cond.Op == "Or" {
				result[i] = leftMask[i] || rightMask[i]
			}
		}
		return result, nil

	case expr.NotOp:
		mask, err := evaluateCondition(df, cond.Expr)
		if err != nil {
			return nil, err
		}
		result := make([]bool, len(mask))
		for i := range result {
			result[i] = !mask[i]
		}
		return result, nil

	case expr.BinaryOp:
		binOp := cond
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
				} else if boolCol, ok := col.(*series.BooleanSeries); ok {
					target, ok := litExpr.Value.(bool)
					if !ok {
						return nil, errors.New("boolean column requires boolean literal for > operator")
					}
					mask[i] = boolCol.Value(i) && !target
				}
			case "<":
				if floatCol, ok := col.(*series.Float64Series); ok {
					mask[i] = floatCol.Value(i) < litExpr.Value.(float64)
				} else if intCol, ok := col.(*series.Int64Series); ok {
					var target int64
					switch v := litExpr.Value.(type) {
					case int:
						target = int64(v)
					case int64:
						target = v
					}
					mask[i] = intCol.Value(i) < target
				} else if boolCol, ok := col.(*series.BooleanSeries); ok {
					target, ok := litExpr.Value.(bool)
					if !ok {
						return nil, errors.New("boolean column requires boolean literal for < operator")
					}
					mask[i] = !boolCol.Value(i) && target
				}
			case "<=":
				if floatCol, ok := col.(*series.Float64Series); ok {
					mask[i] = floatCol.Value(i) <= litExpr.Value.(float64)
				} else if intCol, ok := col.(*series.Int64Series); ok {
					var target int64
					switch v := litExpr.Value.(type) {
					case int:
						target = int64(v)
					case int64:
						target = v
					}
					mask[i] = intCol.Value(i) <= target
				} else if boolCol, ok := col.(*series.BooleanSeries); ok {
					target, ok := litExpr.Value.(bool)
					if !ok {
						return nil, errors.New("boolean column requires boolean literal for <= operator")
					}
					mask[i] = boolCol.Value(i) == target || !target
				}
			case ">=":
				if floatCol, ok := col.(*series.Float64Series); ok {
					mask[i] = floatCol.Value(i) >= litExpr.Value.(float64)
				} else if intCol, ok := col.(*series.Int64Series); ok {
					var target int64
					switch v := litExpr.Value.(type) {
					case int:
						target = int64(v)
					case int64:
						target = v
					}
					mask[i] = intCol.Value(i) >= target
				} else if boolCol, ok := col.(*series.BooleanSeries); ok {
					target, ok := litExpr.Value.(bool)
					if !ok {
						return nil, errors.New("boolean column requires boolean literal for >= operator")
					}
					mask[i] = boolCol.Value(i) == target || target
				}
			case "!=":
				if strCol, ok := col.(*series.StringSeries); ok {
					mask[i] = strCol.Value(i) != litExpr.Value.(string)
				} else if intCol, ok := col.(*series.Int64Series); ok {
					var target int64
					switch v := litExpr.Value.(type) {
					case int:
						target = int64(v)
					case int64:
						target = v
					}
					mask[i] = intCol.Value(i) != target
				} else if floatCol, ok := col.(*series.Float64Series); ok {
					mask[i] = floatCol.Value(i) != litExpr.Value.(float64)
				}
			default:
				return nil, errors.New("unsupported operator")
			}
		}

		return mask, nil

	default:
		return nil, errors.New("only binary and logical operations are supported currently")
	}
}

func applyMask(df *dataframe.DataFrame, mask []bool) (*dataframe.DataFrame, error) {
	result := dataframe.New()
	alloc := memory.DefaultAllocator

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
	alloc := memory.DefaultAllocator

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

func applyDropNulls(df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
	mask := make([]bool, df.NumRows())
	for i := 0; i < df.NumRows(); i++ {
		mask[i] = true
	}

	for colIdx := 0; colIdx < df.NumCols(); colIdx++ {
		col, _ := df.Col(colIdx)
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				mask[i] = false
			}
		}
	}

	return applyMask(df, mask)
}

func applyWithColumns(df *dataframe.DataFrame, columns []expr.Expr) (*dataframe.DataFrame, error) {
	result := dataframe.New()
	alloc := memory.DefaultAllocator

	for i := 0; i < df.NumCols(); i++ {
		col, _ := df.Col(i)
		result.AddSeries(col)
	}

	for _, colExpr := range columns {
		switch e := colExpr.(type) {
		case expr.FillNullExpr:
			col, err := df.ColByName(e.Expr.(expr.Column).Name)
			if err != nil {
				return nil, err
			}
			fillValue := e.Value.(expr.Literal).Value

			switch typedCol := col.(type) {
			case *series.Int64Series:
				var resultVals []int64
				var valid []bool
				for j := 0; j < typedCol.Len(); j++ {
					if typedCol.IsNull(j) {
						var fv int64
						switch v := fillValue.(type) {
						case int:
							fv = int64(v)
						case int64:
							fv = v
						}
						resultVals = append(resultVals, fv)
						valid = append(valid, true)
					} else {
						resultVals = append(resultVals, typedCol.Value(j))
						valid = append(valid, true)
					}
				}
				newCol := series.NewInt64Series(typedCol.Name(), alloc, resultVals, valid)
				result.AddSeries(newCol)

			case *series.Float64Series:
				var resultVals []float64
				var valid []bool
				for j := 0; j < typedCol.Len(); j++ {
					if typedCol.IsNull(j) {
						var fv float64
						switch v := fillValue.(type) {
						case float64:
							fv = v
						case int:
							fv = float64(v)
						case int64:
							fv = float64(v)
						}
						resultVals = append(resultVals, fv)
						valid = append(valid, true)
					} else {
						resultVals = append(resultVals, typedCol.Value(j))
						valid = append(valid, true)
					}
				}
				newCol := series.NewFloat64Series(typedCol.Name(), alloc, resultVals, valid)
				result.AddSeries(newCol)

			case *series.StringSeries:
				var resultVals []string
				var valid []bool
				for j := 0; j < typedCol.Len(); j++ {
					if typedCol.IsNull(j) {
						var fv string
						switch v := fillValue.(type) {
						case string:
							fv = v
						}
						resultVals = append(resultVals, fv)
						valid = append(valid, true)
					} else {
						resultVals = append(resultVals, typedCol.Value(j))
						valid = append(valid, true)
					}
				}
				newCol := series.NewStringSeries(typedCol.Name(), alloc, resultVals, valid)
				result.AddSeries(newCol)

			case *series.BooleanSeries:
				var resultVals []bool
				var valid []bool
				for j := 0; j < typedCol.Len(); j++ {
					if typedCol.IsNull(j) {
						var fv bool
						switch v := fillValue.(type) {
						case bool:
							fv = v
						}
						resultVals = append(resultVals, fv)
						valid = append(valid, true)
					} else {
						resultVals = append(resultVals, typedCol.Value(j))
						valid = append(valid, true)
					}
				}
				newCol := series.NewBooleanSeries(typedCol.Name(), alloc, resultVals, valid)
				result.AddSeries(newCol)
			}

		case expr.FillNullForwardExpr:
			col, err := df.ColByName(e.Expr.(expr.Column).Name)
			if err != nil {
				return nil, err
			}

			switch typedCol := col.(type) {
			case *series.Int64Series:
				var resultVals []int64
				var valid []bool
				var lastValid int64
				var hasLast bool
				for j := 0; j < typedCol.Len(); j++ {
					if !typedCol.IsNull(j) {
						resultVals = append(resultVals, typedCol.Value(j))
						valid = append(valid, true)
						lastValid = typedCol.Value(j)
						hasLast = true
					} else if hasLast {
						resultVals = append(resultVals, lastValid)
						valid = append(valid, true)
					} else {
						resultVals = append(resultVals, 0)
						valid = append(valid, false)
					}
				}
				newCol := series.NewInt64Series(typedCol.Name(), alloc, resultVals, valid)
				result.AddSeries(newCol)

			case *series.Float64Series:
				var resultVals []float64
				var valid []bool
				var lastValid float64
				var hasLast bool
				for j := 0; j < typedCol.Len(); j++ {
					if !typedCol.IsNull(j) {
						resultVals = append(resultVals, typedCol.Value(j))
						valid = append(valid, true)
						lastValid = typedCol.Value(j)
						hasLast = true
					} else if hasLast {
						resultVals = append(resultVals, lastValid)
						valid = append(valid, true)
					} else {
						resultVals = append(resultVals, 0.0)
						valid = append(valid, false)
					}
				}
				newCol := series.NewFloat64Series(typedCol.Name(), alloc, resultVals, valid)
				result.AddSeries(newCol)

			case *series.StringSeries:
				var resultVals []string
				var valid []bool
				var lastValid string
				var hasLast bool
				for j := 0; j < typedCol.Len(); j++ {
					if !typedCol.IsNull(j) {
						resultVals = append(resultVals, typedCol.Value(j))
						valid = append(valid, true)
						lastValid = typedCol.Value(j)
						hasLast = true
					} else if hasLast {
						resultVals = append(resultVals, lastValid)
						valid = append(valid, true)
					} else {
						resultVals = append(resultVals, "")
						valid = append(valid, false)
					}
				}
				newCol := series.NewStringSeries(typedCol.Name(), alloc, resultVals, valid)
				result.AddSeries(newCol)

			case *series.BooleanSeries:
				var resultVals []bool
				var valid []bool
				var lastValid bool
				var hasLast bool
				for j := 0; j < typedCol.Len(); j++ {
					if !typedCol.IsNull(j) {
						resultVals = append(resultVals, typedCol.Value(j))
						valid = append(valid, true)
						lastValid = typedCol.Value(j)
						hasLast = true
					} else if hasLast {
						resultVals = append(resultVals, lastValid)
						valid = append(valid, true)
					} else {
						resultVals = append(resultVals, false)
						valid = append(valid, false)
					}
				}
				newCol := series.NewBooleanSeries(typedCol.Name(), alloc, resultVals, valid)
				result.AddSeries(newCol)
			}

		case expr.FillNullBackwardExpr:
			col, err := df.ColByName(e.Expr.(expr.Column).Name)
			if err != nil {
				return nil, err
			}

			switch typedCol := col.(type) {
			case *series.Int64Series:
				n := typedCol.Len()
				resultVals := make([]int64, n)
				valid := make([]bool, n)
				var nextValid int64
				var hasNext bool
				for j := n - 1; j >= 0; j-- {
					if !typedCol.IsNull(j) {
						resultVals[j] = typedCol.Value(j)
						valid[j] = true
						nextValid = typedCol.Value(j)
						hasNext = true
					} else if hasNext {
						resultVals[j] = nextValid
						valid[j] = true
					} else {
						resultVals[j] = 0
						valid[j] = false
					}
				}
				newCol := series.NewInt64Series(typedCol.Name(), alloc, resultVals, valid)
				result.AddSeries(newCol)

			case *series.Float64Series:
				n := typedCol.Len()
				resultVals := make([]float64, n)
				valid := make([]bool, n)
				var nextValid float64
				var hasNext bool
				for j := n - 1; j >= 0; j-- {
					if !typedCol.IsNull(j) {
						resultVals[j] = typedCol.Value(j)
						valid[j] = true
						nextValid = typedCol.Value(j)
						hasNext = true
					} else if hasNext {
						resultVals[j] = nextValid
						valid[j] = true
					} else {
						resultVals[j] = 0.0
						valid[j] = false
					}
				}
				newCol := series.NewFloat64Series(typedCol.Name(), alloc, resultVals, valid)
				result.AddSeries(newCol)

			case *series.StringSeries:
				n := typedCol.Len()
				resultVals := make([]string, n)
				valid := make([]bool, n)
				var nextValid string
				var hasNext bool
				for j := n - 1; j >= 0; j-- {
					if !typedCol.IsNull(j) {
						resultVals[j] = typedCol.Value(j)
						valid[j] = true
						nextValid = typedCol.Value(j)
						hasNext = true
					} else if hasNext {
						resultVals[j] = nextValid
						valid[j] = true
					} else {
						resultVals[j] = ""
						valid[j] = false
					}
				}
				newCol := series.NewStringSeries(typedCol.Name(), alloc, resultVals, valid)
				result.AddSeries(newCol)

			case *series.BooleanSeries:
				n := typedCol.Len()
				resultVals := make([]bool, n)
				valid := make([]bool, n)
				var nextValid bool
				var hasNext bool
				for j := n - 1; j >= 0; j-- {
					if !typedCol.IsNull(j) {
						resultVals[j] = typedCol.Value(j)
						valid[j] = true
						nextValid = typedCol.Value(j)
						hasNext = true
					} else if hasNext {
						resultVals[j] = nextValid
						valid[j] = true
					} else {
						resultVals[j] = false
						valid[j] = false
					}
				}
				newCol := series.NewBooleanSeries(typedCol.Name(), alloc, resultVals, valid)
				result.AddSeries(newCol)
			}

		case expr.CoalesceExpr:
			colName := e.Exprs[0].(expr.Column).Name
			col, err := df.ColByName(colName)
			if err != nil {
				return nil, err
			}

			otherCols := make([]series.Series, len(e.Exprs)-1)
			for i := 1; i < len(e.Exprs); i++ {
				otherCols[i-1], _ = df.ColByName(e.Exprs[i].(expr.Column).Name)
			}

			switch typedCol := col.(type) {
			case *series.Int64Series:
				var resultVals []int64
				var valid []bool
				for j := 0; j < typedCol.Len(); j++ {
					if !typedCol.IsNull(j) {
						resultVals = append(resultVals, typedCol.Value(j))
						valid = append(valid, true)
					} else {
						found := false
						for _, otherCol := range otherCols {
							if otherColInt, ok := otherCol.(*series.Int64Series); ok {
								if !otherColInt.IsNull(j) {
									resultVals = append(resultVals, otherColInt.Value(j))
									valid = append(valid, true)
									found = true
									break
								}
							}
						}
						if !found {
							resultVals = append(resultVals, 0)
							valid = append(valid, false)
						}
					}
				}
				newCol := series.NewInt64Series(colName, alloc, resultVals, valid)
				result.AddSeries(newCol)

			case *series.Float64Series:
				var resultVals []float64
				var valid []bool
				for j := 0; j < typedCol.Len(); j++ {
					if !typedCol.IsNull(j) {
						resultVals = append(resultVals, typedCol.Value(j))
						valid = append(valid, true)
					} else {
						found := false
						for _, otherCol := range otherCols {
							if otherColFloat, ok := otherCol.(*series.Float64Series); ok {
								if !otherColFloat.IsNull(j) {
									resultVals = append(resultVals, otherColFloat.Value(j))
									valid = append(valid, true)
									found = true
									break
								}
							}
						}
						if !found {
							resultVals = append(resultVals, 0.0)
							valid = append(valid, false)
						}
					}
				}
				newCol := series.NewFloat64Series(colName, alloc, resultVals, valid)
				result.AddSeries(newCol)

			case *series.StringSeries:
				var resultVals []string
				var valid []bool
				for j := 0; j < typedCol.Len(); j++ {
					if !typedCol.IsNull(j) {
						resultVals = append(resultVals, typedCol.Value(j))
						valid = append(valid, true)
					} else {
						found := false
						for _, otherCol := range otherCols {
							if otherColStr, ok := otherCol.(*series.StringSeries); ok {
								if !otherColStr.IsNull(j) {
									resultVals = append(resultVals, otherColStr.Value(j))
									valid = append(valid, true)
									found = true
									break
								}
							}
						}
						if !found {
							resultVals = append(resultVals, "")
							valid = append(valid, false)
						}
					}
				}
				newCol := series.NewStringSeries(colName, alloc, resultVals, valid)
				result.AddSeries(newCol)

			case *series.BooleanSeries:
				var resultVals []bool
				var valid []bool
				for j := 0; j < typedCol.Len(); j++ {
					if !typedCol.IsNull(j) {
						resultVals = append(resultVals, typedCol.Value(j))
						valid = append(valid, true)
					} else {
						found := false
						for _, otherCol := range otherCols {
							if otherColBool, ok := otherCol.(*series.BooleanSeries); ok {
								if !otherColBool.IsNull(j) {
									resultVals = append(resultVals, otherColBool.Value(j))
									valid = append(valid, true)
									found = true
									break
								}
							}
						}
						if !found {
							resultVals = append(resultVals, false)
							valid = append(valid, false)
						}
					}
				}
				newCol := series.NewBooleanSeries(colName, alloc, resultVals, valid)
				result.AddSeries(newCol)
			}

		case expr.ContainsExpr:
			col, err := df.ColByName(e.Expr.(expr.Column).Name)
			if err != nil {
				return nil, err
			}
			strCol, ok := col.(*series.StringSeries)
			if !ok {
				return nil, errors.New("Contains requires string column")
			}
			substr := e.Substr.(expr.Literal).Value.(string)
			var resultVals []string
			var valid []bool
			for j := 0; j < strCol.Len(); j++ {
				if strCol.IsNull(j) {
					resultVals = append(resultVals, "")
					valid = append(valid, false)
				} else {
					resultVals = append(resultVals, strconv.FormatBool(strings.Contains(strCol.Value(j), substr)))
					valid = append(valid, true)
				}
			}
			newCol := series.NewStringSeries("contains_result", alloc, resultVals, valid)
			result.AddSeries(newCol)

		case expr.ReplaceExpr:
			col, err := df.ColByName(e.Expr.(expr.Column).Name)
			if err != nil {
				return nil, err
			}
			strCol, ok := col.(*series.StringSeries)
			if !ok {
				return nil, errors.New("Replace requires string column")
			}
			oldStr := e.Old.(expr.Literal).Value.(string)
			newStr := e.New.(expr.Literal).Value.(string)
			var resultVals []string
			var valid []bool
			for j := 0; j < strCol.Len(); j++ {
				if strCol.IsNull(j) {
					resultVals = append(resultVals, "")
					valid = append(valid, false)
				} else {
					resultVals = append(resultVals, strings.Replace(strCol.Value(j), oldStr, newStr, -1))
					valid = append(valid, true)
				}
			}
			newCol := series.NewStringSeries(strCol.Name(), alloc, resultVals, valid)
			result.AddSeries(newCol)

		case expr.UpperExpr:
			col, err := df.ColByName(e.Expr.(expr.Column).Name)
			if err != nil {
				return nil, err
			}
			strCol, ok := col.(*series.StringSeries)
			if !ok {
				return nil, errors.New("Upper requires string column")
			}
			var resultVals []string
			var valid []bool
			for j := 0; j < strCol.Len(); j++ {
				if strCol.IsNull(j) {
					resultVals = append(resultVals, "")
					valid = append(valid, false)
				} else {
					resultVals = append(resultVals, strings.ToUpper(strCol.Value(j)))
					valid = append(valid, true)
				}
			}
			newCol := series.NewStringSeries(strCol.Name(), alloc, resultVals, valid)
			result.AddSeries(newCol)

		case expr.LowerExpr:
			col, err := df.ColByName(e.Expr.(expr.Column).Name)
			if err != nil {
				return nil, err
			}
			strCol, ok := col.(*series.StringSeries)
			if !ok {
				return nil, errors.New("Lower requires string column")
			}
			var resultVals []string
			var valid []bool
			for j := 0; j < strCol.Len(); j++ {
				if strCol.IsNull(j) {
					resultVals = append(resultVals, "")
					valid = append(valid, false)
				} else {
					resultVals = append(resultVals, strings.ToLower(strCol.Value(j)))
					valid = append(valid, true)
				}
			}
			newCol := series.NewStringSeries(strCol.Name(), alloc, resultVals, valid)
			result.AddSeries(newCol)

		case expr.StripExpr:
			col, err := df.ColByName(e.Expr.(expr.Column).Name)
			if err != nil {
				return nil, err
			}
			strCol, ok := col.(*series.StringSeries)
			if !ok {
				return nil, errors.New("Strip requires string column")
			}
			var resultVals []string
			var valid []bool
			for j := 0; j < strCol.Len(); j++ {
				if strCol.IsNull(j) {
					resultVals = append(resultVals, "")
					valid = append(valid, false)
				} else {
					resultVals = append(resultVals, strings.TrimSpace(strCol.Value(j)))
					valid = append(valid, true)
				}
			}
			newCol := series.NewStringSeries(strCol.Name(), alloc, resultVals, valid)
			result.AddSeries(newCol)

		case expr.LengthExpr:
			col, err := df.ColByName(e.Expr.(expr.Column).Name)
			if err != nil {
				return nil, err
			}
			strCol, ok := col.(*series.StringSeries)
			if !ok {
				return nil, errors.New("Length requires string column")
			}
			var lenVals []int64
			var lenValid []bool
			for j := 0; j < strCol.Len(); j++ {
				if strCol.IsNull(j) {
					lenVals = append(lenVals, 0)
					lenValid = append(lenValid, false)
				} else {
					lenVals = append(lenVals, int64(len(strCol.Value(j))))
					lenValid = append(lenValid, true)
				}
			}
			lenCol := series.NewInt64Series(strCol.Name(), alloc, lenVals, lenValid)
			result.AddSeries(lenCol)

		case expr.TrimExpr:
			col, err := df.ColByName(e.Expr.(expr.Column).Name)
			if err != nil {
				return nil, err
			}
			strCol, ok := col.(*series.StringSeries)
			if !ok {
				return nil, errors.New("Trim requires string column")
			}
			var trimVals []string
			var trimValid []bool
			for j := 0; j < strCol.Len(); j++ {
				if strCol.IsNull(j) {
					trimVals = append(trimVals, "")
					trimValid = append(trimValid, false)
				} else {
					trimVals = append(trimVals, strings.Trim(strCol.Value(j), " \t\n"))
					trimValid = append(trimValid, true)
				}
			}
			trimCol := series.NewStringSeries(strCol.Name(), alloc, trimVals, trimValid)
			result.AddSeries(trimCol)

		case expr.LPadExpr:
			col, err := df.ColByName(e.Expr.(expr.Column).Name)
			if err != nil {
				return nil, err
			}
			strCol, ok := col.(*series.StringSeries)
			if !ok {
				return nil, errors.New("LPad requires string column")
			}
			targetLen := e.Length.(expr.Literal).Value.(int)
			var lpadVals []string
			var lpadValid []bool
			for j := 0; j < strCol.Len(); j++ {
				if strCol.IsNull(j) {
					lpadVals = append(lpadVals, "")
					lpadValid = append(lpadValid, false)
				} else {
					s := strCol.Value(j)
					if len(s) < targetLen {
						s = strings.Repeat(" ", targetLen-len(s)) + s
					}
					lpadVals = append(lpadVals, s)
					lpadValid = append(lpadValid, true)
				}
			}
			lpadCol := series.NewStringSeries(strCol.Name(), alloc, lpadVals, lpadValid)
			result.AddSeries(lpadCol)

		case expr.RPadExpr:
			col, err := df.ColByName(e.Expr.(expr.Column).Name)
			if err != nil {
				return nil, err
			}
			strCol, ok := col.(*series.StringSeries)
			if !ok {
				return nil, errors.New("RPad requires string column")
			}
			targetLen := e.Length.(expr.Literal).Value.(int)
			var rpadVals []string
			var rpadValid []bool
			for j := 0; j < strCol.Len(); j++ {
				if strCol.IsNull(j) {
					rpadVals = append(rpadVals, "")
					rpadValid = append(rpadValid, false)
				} else {
					s := strCol.Value(j)
					if len(s) < targetLen {
						s = s + strings.Repeat(" ", targetLen-len(s))
					}
					rpadVals = append(rpadVals, s)
					rpadValid = append(rpadValid, true)
				}
			}
			rpadCol := series.NewStringSeries(strCol.Name(), alloc, rpadVals, rpadValid)
			result.AddSeries(rpadCol)

		case expr.ContainsRegexExpr:
			col, err := df.ColByName(e.Expr.(expr.Column).Name)
			if err != nil {
				return nil, err
			}
			strCol, ok := col.(*series.StringSeries)
			if !ok {
				return nil, errors.New("ContainsRegex requires string column")
			}
			pattern := e.Pattern.(expr.Literal).Value.(string)
			re, err := regexp.Compile(pattern)
			if err != nil {
				return nil, err
			}
			var regexVals []string
			var regexValid []bool
			for j := 0; j < strCol.Len(); j++ {
				if strCol.IsNull(j) {
					regexVals = append(regexVals, "")
					regexValid = append(regexValid, false)
				} else {
					regexVals = append(regexVals, strconv.FormatBool(re.MatchString(strCol.Value(j))))
					regexValid = append(regexValid, true)
				}
			}
			regexCol := series.NewStringSeries("contains_regex", alloc, regexVals, regexValid)
			result.AddSeries(regexCol)

		case expr.SliceExpr:
			col, err := df.ColByName(e.Expr.(expr.Column).Name)
			if err != nil {
				return nil, err
			}
			strCol, ok := col.(*series.StringSeries)
			if !ok {
				return nil, errors.New("Slice requires string column")
			}
			start := e.Start.(expr.Literal).Value.(int)
			length := e.Length.(expr.Literal).Value.(int)
			var sliceVals []string
			var sliceValid []bool
			for j := 0; j < strCol.Len(); j++ {
				if strCol.IsNull(j) {
					sliceVals = append(sliceVals, "")
					sliceValid = append(sliceValid, false)
				} else {
					s := strCol.Value(j)
					if start < len(s) {
						end := start + length
						if end > len(s) {
							end = len(s)
						}
						sliceVals = append(sliceVals, s[start:end])
						sliceValid = append(sliceValid, true)
					} else {
						sliceVals = append(sliceVals, "")
						sliceValid = append(sliceValid, false)
					}
				}
			}
			sliceCol := series.NewStringSeries(strCol.Name(), alloc, sliceVals, sliceValid)
			result.AddSeries(sliceCol)

		case expr.SplitExpr:
			col, err := df.ColByName(e.Expr.(expr.Column).Name)
			if err != nil {
				return nil, err
			}
			strCol, ok := col.(*series.StringSeries)
			if !ok {
				return nil, errors.New("Split requires string column")
			}
			delim := e.Delim.(expr.Literal).Value.(string)
			var splitVals []string
			var splitValid []bool
			for j := 0; j < strCol.Len(); j++ {
				if strCol.IsNull(j) {
					splitVals = append(splitVals, "")
					splitValid = append(splitValid, false)
				} else {
					parts := strings.Split(strCol.Value(j), delim)
					splitVals = append(splitVals, strings.Join(parts, "|"))
					splitValid = append(splitValid, true)
				}
			}
			splitCol := series.NewStringSeries(strCol.Name(), alloc, splitVals, splitValid)
			result.AddSeries(splitCol)

		case expr.CastExpr:
			col, err := df.ColByName(e.Expr.(expr.Column).Name)
			if err != nil {
				return nil, err
			}
			targetType := e.Dtype.Name()

			switch targetType {
			case "int64":
				switch typedCol := col.(type) {
				case *series.Float64Series:
					var resultVals []int64
					var valid []bool
					for j := 0; j < typedCol.Len(); j++ {
						if typedCol.IsNull(j) {
							resultVals = append(resultVals, 0)
							valid = append(valid, false)
						} else {
							resultVals = append(resultVals, int64(typedCol.Value(j)))
							valid = append(valid, true)
						}
					}
					newCol := series.NewInt64Series(typedCol.Name(), alloc, resultVals, valid)
					result.AddSeries(newCol)
				case *series.StringSeries:
					var resultVals []int64
					var valid []bool
					for j := 0; j < typedCol.Len(); j++ {
						if typedCol.IsNull(j) {
							resultVals = append(resultVals, 0)
							valid = append(valid, false)
						} else {
							val, _ := strconv.ParseInt(typedCol.Value(j), 10, 64)
							resultVals = append(resultVals, val)
							valid = append(valid, true)
						}
					}
					newCol := series.NewInt64Series(typedCol.Name(), alloc, resultVals, valid)
					result.AddSeries(newCol)
				}
			case "float64":
				switch typedCol := col.(type) {
				case *series.Int64Series:
					var resultVals []float64
					var valid []bool
					for j := 0; j < typedCol.Len(); j++ {
						if typedCol.IsNull(j) {
							resultVals = append(resultVals, 0)
							valid = append(valid, false)
						} else {
							resultVals = append(resultVals, float64(typedCol.Value(j)))
							valid = append(valid, true)
						}
					}
					newCol := series.NewFloat64Series(typedCol.Name(), alloc, resultVals, valid)
					result.AddSeries(newCol)
				case *series.StringSeries:
					var resultVals []float64
					var valid []bool
					for j := 0; j < typedCol.Len(); j++ {
						if typedCol.IsNull(j) {
							resultVals = append(resultVals, 0)
							valid = append(valid, false)
						} else {
							val, _ := strconv.ParseFloat(typedCol.Value(j), 64)
							resultVals = append(resultVals, val)
							valid = append(valid, true)
						}
					}
					newCol := series.NewFloat64Series(typedCol.Name(), alloc, resultVals, valid)
					result.AddSeries(newCol)
				}
			case "utf8":
				switch typedCol := col.(type) {
				case *series.Int64Series:
					var resultVals []string
					var valid []bool
					for j := 0; j < typedCol.Len(); j++ {
						if typedCol.IsNull(j) {
							resultVals = append(resultVals, "")
							valid = append(valid, false)
						} else {
							resultVals = append(resultVals, strconv.FormatInt(typedCol.Value(j), 10))
							valid = append(valid, true)
						}
					}
					newCol := series.NewStringSeries(typedCol.Name(), alloc, resultVals, valid)
					result.AddSeries(newCol)
				case *series.Float64Series:
					var resultVals []string
					var valid []bool
					for j := 0; j < typedCol.Len(); j++ {
						if typedCol.IsNull(j) {
							resultVals = append(resultVals, "")
							valid = append(valid, false)
						} else {
							resultVals = append(resultVals, strconv.FormatFloat(typedCol.Value(j), 'f', -1, 64))
							valid = append(valid, true)
						}
					}
					newCol := series.NewStringSeries(typedCol.Name(), alloc, resultVals, valid)
					result.AddSeries(newCol)
				}
			}

		case expr.OtherwiseExpr:
			mask, err := evaluateCondition(df, e.ThenExpr.WhenExpr.Condition)
			if err != nil {
				return nil, err
			}
			thenVal := e.ThenExpr.Value.(expr.Literal).Value
			elseVal := e.Otherwise.(expr.Literal).Value

			col, err := df.Col(0)
			if err != nil {
				return nil, err
			}

			switch typedCol := col.(type) {
			case *series.Int64Series:
				var resultVals []int64
				var valid []bool
				var thenInt, elseInt int64
				switch v := thenVal.(type) {
				case int:
					thenInt = int64(v)
				case int64:
					thenInt = v
				}
				switch v := elseVal.(type) {
				case int:
					elseInt = int64(v)
				case int64:
					elseInt = v
				}
				for j := 0; j < typedCol.Len(); j++ {
					if mask[j] {
						resultVals = append(resultVals, thenInt)
					} else {
						resultVals = append(resultVals, elseInt)
					}
					valid = append(valid, true)
				}
				newCol := series.NewInt64Series(typedCol.Name(), alloc, resultVals, valid)
				result.AddSeries(newCol)

			case *series.Float64Series:
				var resultVals []float64
				var valid []bool
				var thenFloat, elseFloat float64
				switch v := thenVal.(type) {
				case float64:
					thenFloat = v
				case int:
					thenFloat = float64(v)
				case int64:
					thenFloat = float64(v)
				}
				switch v := elseVal.(type) {
				case float64:
					elseFloat = v
				case int:
					elseFloat = float64(v)
				case int64:
					elseFloat = float64(v)
				}
				for j := 0; j < typedCol.Len(); j++ {
					if mask[j] {
						resultVals = append(resultVals, thenFloat)
					} else {
						resultVals = append(resultVals, elseFloat)
					}
					valid = append(valid, true)
				}
				newCol := series.NewFloat64Series(typedCol.Name(), alloc, resultVals, valid)
				result.AddSeries(newCol)

			case *series.StringSeries:
				var resultVals []string
				var valid []bool
				thenStr, _ := thenVal.(string)
				elseStr, _ := elseVal.(string)
				for j := 0; j < typedCol.Len(); j++ {
					if mask[j] {
						resultVals = append(resultVals, thenStr)
					} else {
						resultVals = append(resultVals, elseStr)
					}
					valid = append(valid, true)
				}
				newCol := series.NewStringSeries(typedCol.Name(), alloc, resultVals, valid)
				result.AddSeries(newCol)

			case *series.BooleanSeries:
				var resultVals []bool
				var valid []bool
				var thenBool, elseBool bool
				switch v := thenVal.(type) {
				case bool:
					thenBool = v
				}
				switch v := elseVal.(type) {
				case bool:
					elseBool = v
				}
				for j := 0; j < typedCol.Len(); j++ {
					if mask[j] {
						resultVals = append(resultVals, thenBool)
					} else {
						resultVals = append(resultVals, elseBool)
					}
					valid = append(valid, true)
				}
				newCol := series.NewBooleanSeries(typedCol.Name(), alloc, resultVals, valid)
				result.AddSeries(newCol)
			}

		default:
			return nil, errors.New("unsupported expression in WithColumns")
		}
	}

	return result, nil
}

func applyDistinct(df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
	seen := make(map[string]bool)
	var keepIndices []int

	for i := 0; i < df.NumRows(); i++ {
		key := ""
		for j := 0; j < df.NumCols(); j++ {
			col, _ := df.Col(j)
			if j > 0 {
				key += "|"
			}
			if col.IsNull(i) {
				key += "NULL"
			} else {
				switch c := col.(type) {
				case *series.StringSeries:
					key += c.Value(i)
				case *series.Int64Series:
					key += fmt.Sprintf("%d", c.Value(i))
				case *series.Float64Series:
					key += fmt.Sprintf("%f", c.Value(i))
				case *series.BooleanSeries:
					if c.Value(i) {
						key += "true"
					} else {
						key += "false"
					}
				}
			}
		}
		if !seen[key] {
			seen[key] = true
			keepIndices = append(keepIndices, i)
		}
	}

	result := dataframe.New()
	alloc := memory.DefaultAllocator

	for i := 0; i < df.NumCols(); i++ {
		col, _ := df.Col(i)
		newCol := copySeriesByIndices(col, keepIndices, alloc)
		result.AddSeries(newCol)
	}

	return result, nil
}

func applyWindow(df *dataframe.DataFrame, windowFunc expr.WindowExpr, partBy []string, orderBy []string) (*dataframe.DataFrame, error) {
	result := dataframe.New()
	alloc := memory.DefaultAllocator

	for i := 0; i < df.NumCols(); i++ {
		col, _ := df.Col(i)
		result.AddSeries(col)
	}

	var resultCol series.Series

	switch windowFunc.Func {
	case expr.FuncRowNumber:
		rowNumbers := make([]int64, df.NumRows())
		for i := 0; i < df.NumRows(); i++ {
			rowNumbers[i] = int64(i + 1)
		}
		resultCol = series.NewInt64Series("row_number", alloc, rowNumbers, nil)

	case expr.FuncRank:
		ranks := make([]int64, df.NumRows())
		for i := 0; i < df.NumRows(); i++ {
			ranks[i] = int64(i + 1)
		}
		resultCol = series.NewInt64Series("rank", alloc, ranks, nil)

	case expr.FuncLag:
		if windowFunc.Expr != nil {
			if colExpr, ok := windowFunc.Expr.(expr.Column); ok {
				col, err := df.ColByName(colExpr.Name)
				if err != nil {
					return nil, err
				}
				offset := windowFunc.Offset
				if offset == 0 {
					offset = 1
				}

				switch typedCol := col.(type) {
				case *series.Int64Series:
					values := make([]int64, df.NumRows())
					valid := make([]bool, df.NumRows())
					for i := 0; i < df.NumRows(); i++ {
						if i >= offset {
							values[i] = typedCol.Value(i - offset)
							valid[i] = !typedCol.IsNull(i - offset)
						} else {
							values[i] = 0
							valid[i] = false
						}
					}
					resultCol = series.NewInt64Series("lag", alloc, values, valid)

				case *series.Float64Series:
					values := make([]float64, df.NumRows())
					valid := make([]bool, df.NumRows())
					for i := 0; i < df.NumRows(); i++ {
						if i >= offset {
							values[i] = typedCol.Value(i - offset)
							valid[i] = !typedCol.IsNull(i - offset)
						} else {
							values[i] = 0
							valid[i] = false
						}
					}
					resultCol = series.NewFloat64Series("lag", alloc, values, valid)

				case *series.StringSeries:
					values := make([]string, df.NumRows())
					valid := make([]bool, df.NumRows())
					for i := 0; i < df.NumRows(); i++ {
						if i >= offset {
							values[i] = typedCol.Value(i - offset)
							valid[i] = !typedCol.IsNull(i - offset)
						} else {
							values[i] = ""
							valid[i] = false
						}
					}
					resultCol = series.NewStringSeries("lag", alloc, values, valid)
				}
			}
		}

	case expr.FuncLead:
		if windowFunc.Expr != nil {
			if colExpr, ok := windowFunc.Expr.(expr.Column); ok {
				col, err := df.ColByName(colExpr.Name)
				if err != nil {
					return nil, err
				}
				offset := windowFunc.Offset
				if offset == 0 {
					offset = 1
				}

				switch typedCol := col.(type) {
				case *series.Int64Series:
					values := make([]int64, df.NumRows())
					valid := make([]bool, df.NumRows())
					for i := 0; i < df.NumRows(); i++ {
						if i+offset < df.NumRows() {
							values[i] = typedCol.Value(i + offset)
							valid[i] = !typedCol.IsNull(i + offset)
						} else {
							values[i] = 0
							valid[i] = false
						}
					}
					resultCol = series.NewInt64Series("lead", alloc, values, valid)

				case *series.Float64Series:
					values := make([]float64, df.NumRows())
					valid := make([]bool, df.NumRows())
					for i := 0; i < df.NumRows(); i++ {
						if i+offset < df.NumRows() {
							values[i] = typedCol.Value(i + offset)
							valid[i] = !typedCol.IsNull(i + offset)
						} else {
							values[i] = 0
							valid[i] = false
						}
					}
					resultCol = series.NewFloat64Series("lead", alloc, values, valid)

				case *series.StringSeries:
					values := make([]string, df.NumRows())
					valid := make([]bool, df.NumRows())
					for i := 0; i < df.NumRows(); i++ {
						if i+offset < df.NumRows() {
							values[i] = typedCol.Value(i + offset)
							valid[i] = !typedCol.IsNull(i + offset)
						} else {
							values[i] = ""
							valid[i] = false
						}
					}
					resultCol = series.NewStringSeries("lead", alloc, values, valid)
				}
			}
		}
	}

	if resultCol != nil {
		result.AddSeries(resultCol)
	}

	return result, nil
}
