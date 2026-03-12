package engine

import (
	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

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
