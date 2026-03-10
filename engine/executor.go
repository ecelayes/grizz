package engine

import (
	"errors"
	"fmt"
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
						resultVals = append(resultVals, fillValue.(float64))
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
						resultVals = append(resultVals, fillValue.(string))
						valid = append(valid, true)
					} else {
						resultVals = append(resultVals, typedCol.Value(j))
						valid = append(valid, true)
					}
				}
				newCol := series.NewStringSeries(typedCol.Name(), alloc, resultVals, valid)
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
			col, err := df.ColByName(e.ThenExpr.WhenExpr.Condition.(expr.Column).Name)
			if err != nil {
				return nil, err
			}
			thenVal := e.ThenExpr.Value.(expr.Literal).Value
			elseVal := e.Otherwise.(expr.Literal).Value

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
					if typedCol.IsNull(j) {
						resultVals = append(resultVals, elseInt)
						valid = append(valid, true)
					} else {
						resultVals = append(resultVals, thenInt)
						valid = append(valid, true)
					}
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
					if typedCol.IsNull(j) {
						resultVals = append(resultVals, elseFloat)
						valid = append(valid, true)
					} else {
						resultVals = append(resultVals, thenFloat)
						valid = append(valid, true)
					}
				}
				newCol := series.NewFloat64Series(typedCol.Name(), alloc, resultVals, valid)
				result.AddSeries(newCol)

			case *series.StringSeries:
				var resultVals []string
				var valid []bool
				thenStr, _ := thenVal.(string)
				elseStr, _ := elseVal.(string)
				for j := 0; j < typedCol.Len(); j++ {
					if typedCol.IsNull(j) {
						resultVals = append(resultVals, elseStr)
						valid = append(valid, true)
					} else {
						resultVals = append(resultVals, thenStr)
						valid = append(valid, true)
					}
				}
				newCol := series.NewStringSeries(typedCol.Name(), alloc, resultVals, valid)
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
