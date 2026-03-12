package engine

import (
	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func applyFillNull(df *dataframe.DataFrame, e expr.FillNullExpr, alloc memory.Allocator) (series.Series, error) {
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
		return series.NewInt64Series(typedCol.Name(), alloc, resultVals, valid), nil

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
		return series.NewFloat64Series(typedCol.Name(), alloc, resultVals, valid), nil

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
		return series.NewStringSeries(typedCol.Name(), alloc, resultVals, valid), nil

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
		return series.NewBooleanSeries(typedCol.Name(), alloc, resultVals, valid), nil
	}
	return nil, nil
}

func applyFillNullForward(df *dataframe.DataFrame, e expr.FillNullForwardExpr, alloc memory.Allocator) (series.Series, error) {
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
		return series.NewInt64Series(typedCol.Name(), alloc, resultVals, valid), nil

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
		return series.NewFloat64Series(typedCol.Name(), alloc, resultVals, valid), nil

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
		return series.NewStringSeries(typedCol.Name(), alloc, resultVals, valid), nil

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
		return series.NewBooleanSeries(typedCol.Name(), alloc, resultVals, valid), nil
	}
	return nil, nil
}

func applyFillNullBackward(df *dataframe.DataFrame, e expr.FillNullBackwardExpr, alloc memory.Allocator) (series.Series, error) {
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
		return series.NewInt64Series(typedCol.Name(), alloc, resultVals, valid), nil

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
		return series.NewFloat64Series(typedCol.Name(), alloc, resultVals, valid), nil

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
		return series.NewStringSeries(typedCol.Name(), alloc, resultVals, valid), nil

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
		return series.NewBooleanSeries(typedCol.Name(), alloc, resultVals, valid), nil
	}
	return nil, nil
}
