package engine

import (
	"errors"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

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

	case expr.IsInExpr:
		colExpr, ok := cond.Expr.(expr.Column)
		if !ok {
			return nil, errors.New("IsIn only supports column expressions")
		}
		col, err := df.ColByName(colExpr.Name)
		if err != nil {
			return nil, err
		}
		valueSet := make(map[any]bool)
		for _, lit := range cond.Values {
			val := lit.Value
			if intVal, ok := val.(int); ok {
				val = int64(intVal)
			}
			valueSet[val] = true
		}
		mask := make([]bool, col.Len())
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				mask[i] = false
				continue
			}
			var val any
			switch c := col.(type) {
			case *series.Int64Series:
				val = c.Value(i)
			case *series.Float64Series:
				val = c.Value(i)
			case *series.StringSeries:
				val = c.Value(i)
			case *series.BooleanSeries:
				val = c.Value(i)
			}
			mask[i] = valueSet[val]
		}
		return mask, nil

	case expr.BetweenExpr:
		colExpr, ok := cond.Expr.(expr.Column)
		if !ok {
			return nil, errors.New("Between only supports column expressions")
		}
		col, err := df.ColByName(colExpr.Name)
		if err != nil {
			return nil, err
		}

		lower := cond.Lower.Value
		upper := cond.Upper.Value

		if intVal, ok := lower.(int); ok {
			lower = int64(intVal)
		}
		if intVal, ok := upper.(int); ok {
			upper = int64(intVal)
		}

		mask := make([]bool, col.Len())
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				mask[i] = false
				continue
			}
			switch c := col.(type) {
			case *series.Int64Series:
				val := c.Value(i)
				lowerVal, lowerOk := lower.(int64)
				upperVal, upperOk := upper.(int64)
				if !lowerOk || !upperOk {
					mask[i] = false
					continue
				}
				mask[i] = val >= lowerVal && val <= upperVal
			case *series.Float64Series:
				val := c.Value(i)
				var lowerVal, upperVal float64
				switch l := lower.(type) {
				case float64:
					lowerVal = l
				case int64:
					lowerVal = float64(l)
				default:
					mask[i] = false
					continue
				}
				switch u := upper.(type) {
				case float64:
					upperVal = u
				case int64:
					upperVal = float64(u)
				default:
					mask[i] = false
					continue
				}
				mask[i] = val >= lowerVal && val <= upperVal
			default:
				return nil, errors.New("Between only supports numeric columns")
			}
		}
		return mask, nil

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
