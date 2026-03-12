package engine

import (
	"errors"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func applyArithmetic(df *dataframe.DataFrame, arith expr.ArithmeticOp, alloc memory.Allocator) (series.Series, error) {
	leftCol, leftIsColumn := arith.Left.(expr.Column)
	rightCol, rightIsColumn := arith.Right.(expr.Column)

	if !leftIsColumn || !rightIsColumn {
		return nil, errors.New("Arithmetic between columns requires both operands to be columns")
	}

	col1, err := df.ColByName(leftCol.Name)
	if err != nil {
		return nil, err
	}
	col2, err := df.ColByName(rightCol.Name)
	if err != nil {
		return nil, err
	}

	return applyArithmeticSeries(col1, col2, arith.Op, "result", alloc)
}

func applyArithmeticSeries(col1, col2 series.Series, op string, name string, alloc memory.Allocator) (series.Series, error) {
	valid := make([]bool, col1.Len())
	for i := 0; i < col1.Len(); i++ {
		valid[i] = !col1.IsNull(i) && !col2.IsNull(i)
	}

	switch c1 := col1.(type) {
	case *series.Int64Series:
		c2, ok := col2.(*series.Int64Series)
		if !ok {
			return nil, errors.New("cannot perform arithmetic between different types")
		}
		result := make([]int64, c1.Len())
		for i := 0; i < c1.Len(); i++ {
			if !valid[i] {
				continue
			}
			switch op {
			case "+":
				result[i] = c1.Value(i) + c2.Value(i)
			case "-":
				result[i] = c1.Value(i) - c2.Value(i)
			case "*":
				result[i] = c1.Value(i) * c2.Value(i)
			case "/":
				if c2.Value(i) != 0 {
					result[i] = c1.Value(i) / c2.Value(i)
				}
			}
		}
		return series.NewInt64Series(name, alloc, result, valid), nil

	case *series.Float64Series:
		switch c2 := col2.(type) {
		case *series.Float64Series:
			result := make([]float64, c1.Len())
			for i := 0; i < c1.Len(); i++ {
				if !valid[i] {
					continue
				}
				switch op {
				case "+":
					result[i] = c1.Value(i) + c2.Value(i)
				case "-":
					result[i] = c1.Value(i) - c2.Value(i)
				case "*":
					result[i] = c1.Value(i) * c2.Value(i)
				case "/":
					if c2.Value(i) != 0 {
						result[i] = c1.Value(i) / c2.Value(i)
					}
				}
			}
			return series.NewFloat64Series(name, alloc, result, valid), nil

		case *series.Int64Series:
			result := make([]float64, c1.Len())
			for i := 0; i < c1.Len(); i++ {
				if !valid[i] {
					continue
				}
				switch op {
				case "+":
					result[i] = c1.Value(i) + float64(c2.Value(i))
				case "-":
					result[i] = c1.Value(i) - float64(c2.Value(i))
				case "*":
					result[i] = c1.Value(i) * float64(c2.Value(i))
				case "/":
					if c2.Value(i) != 0 {
						result[i] = c1.Value(i) / float64(c2.Value(i))
					}
				}
			}
			return series.NewFloat64Series(name, alloc, result, valid), nil

		default:
			return nil, errors.New("cannot perform arithmetic between different types")
		}

	default:
		return nil, errors.New("arithmetic not supported for this column type")
	}
}
