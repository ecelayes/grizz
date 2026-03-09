package engine

import (
	"errors"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func applyGroupBy(df *dataframe.DataFrame, keys []string, aggs []expr.Aggregation) (*dataframe.DataFrame, error) {
	if len(keys) != 1 {
		return nil, errors.New("only single-key groupby is currently supported")
	}

	keyCol, err := df.ColByName(keys[0])
	if err != nil {
		return nil, err
	}

	strKeyCol, ok := keyCol.(*series.StringSeries)
	if !ok {
		return nil, errors.New("groupby key must be a string column")
	}

	groups := make(map[string][]int)
	for i := 0; i < strKeyCol.Len(); i++ {
		if !strKeyCol.IsNull(i) {
			val := strKeyCol.Value(i)
			groups[val] = append(groups[val], i)
		}
	}

	alloc := memory.DefaultAllocator()
	result := dataframe.New()

	var outKeys []string
	for k := range groups {
		outKeys = append(outKeys, k)
	}
	result.AddSeries(series.NewStringSeries(keys[0], alloc, outKeys, nil))

	for _, agg := range aggs {
		colExpr, ok := agg.Expr.(expr.Column)
		if !ok {
			return nil, errors.New("aggregation only supports direct columns")
		}

		aggCol, err := df.ColByName(colExpr.Name)
		if err != nil {
			return nil, err
		}

		aggName := string(agg.Func) + "_" + colExpr.Name

		switch typedAggCol := aggCol.(type) {
		case *series.Float64Series:
			var outVals []float64
			for _, key := range outKeys {
				indices := groups[key]
				val := calculateAggFloat(typedAggCol, indices, agg.Func)
				outVals = append(outVals, val)
			}
			result.AddSeries(series.NewFloat64Series(aggName, alloc, outVals, nil))

		case *series.Int64Series:
			if agg.Func == expr.MeanAgg {
				var outVals []float64
				for _, key := range outKeys {
					indices := groups[key]
					val := calculateAggIntToFloat(typedAggCol, indices, agg.Func)
					outVals = append(outVals, val)
				}
				result.AddSeries(series.NewFloat64Series(aggName, alloc, outVals, nil))
			} else {
				var outVals []int64
				for _, key := range outKeys {
					indices := groups[key]
					val := calculateAggInt(typedAggCol, indices, agg.Func)
					outVals = append(outVals, val)
				}
				result.AddSeries(series.NewInt64Series(aggName, alloc, outVals, nil))
			}
		default:
			return nil, errors.New("aggregations currently only support numeric columns")
		}
	}

	return result, nil
}

func calculateAggFloat(col *series.Float64Series, indices []int, aggFunc expr.AggFunc) float64 {
	switch aggFunc {
	case expr.SumAgg:
		var sum float64
		for _, idx := range indices {
			sum += col.Value(idx)
		}
		return sum
	case expr.MeanAgg:
		var sum float64
		for _, idx := range indices {
			sum += col.Value(idx)
		}
		if len(indices) == 0 {
			return 0
		}
		return sum / float64(len(indices))
	case expr.CountAgg:
		return float64(len(indices))
	}
	return 0
}

func calculateAggInt(col *series.Int64Series, indices []int, aggFunc expr.AggFunc) int64 {
	switch aggFunc {
	case expr.SumAgg:
		var sum int64
		for _, idx := range indices {
			sum += col.Value(idx)
		}
		return sum
	case expr.CountAgg:
		return int64(len(indices))
	}
	return 0
}

func calculateAggIntToFloat(col *series.Int64Series, indices []int, aggFunc expr.AggFunc) float64 {
	if aggFunc == expr.MeanAgg {
		var sum float64
		for _, idx := range indices {
			sum += float64(col.Value(idx))
		}
		if len(indices) == 0 {
			return 0
		}
		return sum / float64(len(indices))
	}
	return 0
}
