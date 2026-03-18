package engine

import (
	"errors"
	"math"
	"sort"

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

	alloc := memory.DefaultAllocator
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
			outVals := make([]float64, len(outKeys))
			for i, key := range outKeys {
				indices := groups[key]
				outVals[i] = calculateAggFloat(typedAggCol, indices, agg.Func, agg.Param)
			}
			result.AddSeries(series.NewFloat64Series(aggName, alloc, outVals, nil))

		case *series.Int64Series:
			if agg.Func == expr.MeanAgg || agg.Func == expr.StdAgg || agg.Func == expr.VarAgg ||
				agg.Func == expr.MedianAgg || agg.Func == expr.QuantileAgg {
				outVals := make([]float64, len(outKeys))
				for i, key := range outKeys {
					indices := groups[key]
					outVals[i] = calculateAggIntToFloat(typedAggCol, indices, agg.Func, agg.Param)
				}
				result.AddSeries(series.NewFloat64Series(aggName, alloc, outVals, nil))
			} else {
				outVals := make([]int64, len(outKeys))
				for i, key := range outKeys {
					indices := groups[key]
					outVals[i] = calculateAggInt(typedAggCol, indices, agg.Func)
				}
				result.AddSeries(series.NewInt64Series(aggName, alloc, outVals, nil))
			}
		default:
			return nil, errors.New("aggregations currently only support numeric columns")
		}
	}

	return result, nil
}

func calculateAggFloat(col *series.Float64Series, indices []int, aggFunc expr.AggFunc, param float64) float64 {
	if len(indices) == 0 {
		return 0
	}

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
		return sum / float64(len(indices))
	case expr.CountAgg:
		return float64(len(indices))
	case expr.MinAgg:
		minVal := col.Value(indices[0])
		for _, idx := range indices[1:] {
			v := col.Value(idx)
			if v < minVal {
				minVal = v
			}
		}
		return minVal
	case expr.MaxAgg:
		maxVal := col.Value(indices[0])
		for _, idx := range indices[1:] {
			v := col.Value(idx)
			if v > maxVal {
				maxVal = v
			}
		}
		return maxVal
	case expr.StdAgg:
		return math.Sqrt(popVarianceFloat(col, indices))
	case expr.VarAgg:
		return popVarianceFloat(col, indices)
	case expr.MedianAgg:
		return quantileFloat(col, indices, 0.5)
	case expr.QuantileAgg:
		return quantileFloat(col, indices, param)
	case expr.NUniqueAgg:
		return nuniqueFloat(col, indices)
	case expr.FirstAgg:
		return firstFloat(col, indices)
	case expr.LastAgg:
		return lastFloat(col, indices)
	case expr.ArgMinAgg:
		return argminFloat(col, indices)
	case expr.ArgMaxAgg:
		return argmaxFloat(col, indices)
	}
	return 0
}

func popVarianceFloat(col *series.Float64Series, indices []int) float64 {
	if len(indices) == 0 {
		return 0
	}
	var sum float64
	for _, idx := range indices {
		sum += col.Value(idx)
	}
	mean := sum / float64(len(indices))

	var variance float64
	for _, idx := range indices {
		diff := col.Value(idx) - mean
		variance += diff * diff
	}
	return variance / float64(len(indices))
}

func quantileFloat(col *series.Float64Series, indices []int, q float64) float64 {
	if len(indices) == 0 {
		return 0
	}
	values := make([]float64, len(indices))
	for i, idx := range indices {
		values[i] = col.Value(idx)
	}
	sort.Float64s(values)

	pos := q * float64(len(values)-1)
	lower := int(pos)
	upper := lower + 1
	if upper >= len(values) {
		return values[lower]
	}
	weight := pos - float64(lower)
	return values[lower]*(1-weight) + values[upper]*weight
}

func sqrt(x float64) float64 {
	return math.Sqrt(x)
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
	case expr.MinAgg:
		if len(indices) == 0 {
			return 0
		}
		minVal := col.Value(indices[0])
		for _, idx := range indices[1:] {
			v := col.Value(idx)
			if v < minVal {
				minVal = v
			}
		}
		return minVal
	case expr.MaxAgg:
		if len(indices) == 0 {
			return 0
		}
		maxVal := col.Value(indices[0])
		for _, idx := range indices[1:] {
			v := col.Value(idx)
			if v > maxVal {
				maxVal = v
			}
		}
		return maxVal
	}
	return 0
}

func calculateAggIntToFloat(col *series.Int64Series, indices []int, aggFunc expr.AggFunc, param float64) float64 {
	if len(indices) == 0 {
		return 0
	}

	switch aggFunc {
	case expr.MeanAgg:
		var sum float64
		for _, idx := range indices {
			sum += float64(col.Value(idx))
		}
		return sum / float64(len(indices))
	case expr.StdAgg:
		return math.Sqrt(popVarianceInt(col, indices))
	case expr.VarAgg:
		return popVarianceInt(col, indices)
	case expr.MedianAgg:
		return quantileInt(col, indices, 0.5)
	case expr.QuantileAgg:
		return quantileInt(col, indices, param)
	}
	return 0
}

func popVarianceInt(col *series.Int64Series, indices []int) float64 {
	if len(indices) == 0 {
		return 0
	}
	var sum float64
	for _, idx := range indices {
		sum += float64(col.Value(idx))
	}
	mean := sum / float64(len(indices))

	var variance float64
	for _, idx := range indices {
		diff := float64(col.Value(idx)) - mean
		variance += diff * diff
	}
	return variance / float64(len(indices))
}

func quantileInt(col *series.Int64Series, indices []int, q float64) float64 {
	if len(indices) == 0 {
		return 0
	}
	values := make([]float64, len(indices))
	for i, idx := range indices {
		values[i] = float64(col.Value(idx))
	}
	sort.Float64s(values)

	pos := q * float64(len(values)-1)
	lower := int(pos)
	upper := lower + 1
	if upper >= len(values) {
		return values[lower]
	}
	weight := pos - float64(lower)
	return values[lower]*(1-weight) + values[upper]*weight
}

func nuniqueFloat(col *series.Float64Series, indices []int) float64 {
	if len(indices) == 0 {
		return 0
	}
	unique := make(map[float64]bool)
	for _, idx := range indices {
		unique[col.Value(idx)] = true
	}
	return float64(len(unique))
}

func firstFloat(col *series.Float64Series, indices []int) float64 {
	if len(indices) == 0 {
		return 0
	}
	return col.Value(indices[0])
}

func lastFloat(col *series.Float64Series, indices []int) float64 {
	if len(indices) == 0 {
		return 0
	}
	return col.Value(indices[len(indices)-1])
}

func argminFloat(col *series.Float64Series, indices []int) float64 {
	if len(indices) == 0 {
		return 0
	}
	minVal := col.Value(indices[0])
	minIdx := indices[0]
	for _, idx := range indices {
		if col.Value(idx) < minVal {
			minVal = col.Value(idx)
			minIdx = idx
		}
	}
	return float64(minIdx)
}

func argmaxFloat(col *series.Float64Series, indices []int) float64 {
	if len(indices) == 0 {
		return 0
	}
	maxVal := col.Value(indices[0])
	maxIdx := indices[0]
	for _, idx := range indices {
		if col.Value(idx) > maxVal {
			maxVal = col.Value(idx)
			maxIdx = idx
		}
	}
	return float64(maxIdx)
}

func buildGroups(df *dataframe.DataFrame, keys []string) (map[string][]int, error) {
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

	return groups, nil
}

func subsetSeriesByIndices(col series.Series, indices []int) series.Series {
	switch c := col.(type) {
	case *series.Int64Series:
		values := make([]int64, len(indices))
		valid := make([]bool, len(indices))
		for i, idx := range indices {
			values[i] = c.Value(idx)
			valid[i] = !c.IsNull(idx)
		}
		return series.NewInt64Series(c.Name(), memory.DefaultAllocator, values, valid)
	case *series.Float64Series:
		values := make([]float64, len(indices))
		valid := make([]bool, len(indices))
		for i, idx := range indices {
			values[i] = c.Value(idx)
			valid[i] = !c.IsNull(idx)
		}
		return series.NewFloat64Series(c.Name(), memory.DefaultAllocator, values, valid)
	case *series.StringSeries:
		values := make([]string, len(indices))
		valid := make([]bool, len(indices))
		for i, idx := range indices {
			values[i] = c.Value(idx)
			valid[i] = !c.IsNull(idx)
		}
		return series.NewStringSeries(c.Name(), memory.DefaultAllocator, values, valid)
	case *series.BooleanSeries:
		values := make([]bool, len(indices))
		valid := make([]bool, len(indices))
		for i, idx := range indices {
			values[i] = c.Value(idx)
			valid[i] = !c.IsNull(idx)
		}
		return series.NewBooleanSeries(c.Name(), memory.DefaultAllocator, values, valid)
	default:
		return col
	}
}

func applyGroupByHead(df *dataframe.DataFrame, keys []string, n int) (*dataframe.DataFrame, error) {
	groups, err := buildGroups(df, keys)
	if err != nil {
		return nil, err
	}

	result := dataframe.New()

	var sortedKeys []string
	for k := range groups {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	allIndices := make([]int, 0)
	for _, key := range sortedKeys {
		indices := groups[key]
		count := n
		if count > len(indices) {
			count = len(indices)
		}
		allIndices = append(allIndices, indices[:count]...)
	}

	for i := 0; i < df.NumCols(); i++ {
		col, err := df.Col(i)
		if err != nil {
			return nil, err
		}
		subCol := subsetSeriesByIndices(col, allIndices)
		result.AddSeries(subCol)
	}

	return result, nil
}

func applyGroupByTail(df *dataframe.DataFrame, keys []string, n int) (*dataframe.DataFrame, error) {
	groups, err := buildGroups(df, keys)
	if err != nil {
		return nil, err
	}

	result := dataframe.New()

	var sortedKeys []string
	for k := range groups {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	allIndices := make([]int, 0)
	for _, key := range sortedKeys {
		indices := groups[key]
		count := n
		if count > len(indices) {
			count = len(indices)
		}
		start := len(indices) - count
		allIndices = append(allIndices, indices[start:]...)
	}

	for i := 0; i < df.NumCols(); i++ {
		col, err := df.Col(i)
		if err != nil {
			return nil, err
		}
		subCol := subsetSeriesByIndices(col, allIndices)
		result.AddSeries(subCol)
	}

	return result, nil
}

func applyGroupByGroups(df *dataframe.DataFrame, keys []string) (*dataframe.DataFrame, error) {
	groups, err := buildGroups(df, keys)
	if err != nil {
		return nil, err
	}

	result := dataframe.New()

	var sortedKeys []string
	for k := range groups {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	result.AddSeries(series.NewStringSeries(keys[0], memory.DefaultAllocator, sortedKeys, nil))

	rowCounts := make([]int64, len(sortedKeys))
	for i, key := range sortedKeys {
		rowCounts[i] = int64(len(groups[key]))
	}
	result.AddSeries(series.NewInt64Series("__row_count", memory.DefaultAllocator, rowCounts, nil))

	return result, nil
}
