package engine

import (
	"runtime"
	"sync"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

var numWorkers = runtime.NumCPU()

func ExecuteParallel(plan dataframe.LogicalPlan) (*dataframe.DataFrame, error) {
	return executeParallel(plan, numWorkers)
}

func executeParallel(plan dataframe.LogicalPlan, workers int) (*dataframe.DataFrame, error) {
	switch p := plan.(type) {
	case dataframe.ScanPlan:
		return p.DataFrame, nil

	case dataframe.FilterPlan:
		inputDF, err := executeParallel(p.Input, workers)
		if err != nil {
			return nil, err
		}

		mask, err := evaluateCondition(inputDF, p.Condition)
		if err != nil {
			return nil, err
		}

		return applyMaskParallel(inputDF, mask, workers)

	case dataframe.SelectPlan:
		inputDF, err := executeParallel(p.Input, workers)
		if err != nil {
			return nil, err
		}
		return applyProjection(inputDF, p.Columns)

	case dataframe.WithColumnsPlan:
		inputDF, err := executeParallel(p.Input, workers)
		if err != nil {
			return nil, err
		}
		return applyWithColumns(inputDF, p.Columns)

	case dataframe.GroupByPlan:
		inputDF, err := executeParallel(p.Input, workers)
		if err != nil {
			return nil, err
		}
		return applyGroupBy(inputDF, p.Keys, p.Aggs)

	case dataframe.OrderByPlan:
		inputDF, err := executeParallel(p.Input, workers)
		if err != nil {
			return nil, err
		}
		return applyOrderBy(inputDF, p.Column, p.Descending)

	case dataframe.LimitPlan:
		inputDF, err := executeParallel(p.Input, workers)
		if err != nil {
			return nil, err
		}
		return applyLimit(inputDF, p.Limit)

	case dataframe.TailPlan:
		inputDF, err := executeParallel(p.Input, workers)
		if err != nil {
			return nil, err
		}
		return applyTail(inputDF, p.N)

	case dataframe.SamplePlan:
		inputDF, err := executeParallel(p.Input, workers)
		if err != nil {
			return nil, err
		}
		return applySample(inputDF, p.N, p.Frac, p.Replace)

	case dataframe.JoinPlan:
		leftDF, err := executeParallel(p.Left, workers)
		if err != nil {
			return nil, err
		}
		rightDF, err := executeParallel(p.Right, workers)
		if err != nil {
			return nil, err
		}
		return applyJoin(leftDF, rightDF, p.On, p.How)

	case dataframe.DistinctPlan:
		inputDF, err := executeParallel(p.Input, workers)
		if err != nil {
			return nil, err
		}
		return applyDistinct(inputDF)

	case dataframe.DropNullsPlan:
		inputDF, err := executeParallel(p.Input, workers)
		if err != nil {
			return nil, err
		}
		return applyDropNulls(inputDF)

	case dataframe.WindowPlan:
		inputDF, err := executeParallel(p.Input, workers)
		if err != nil {
			return nil, err
		}
		return applyWindow(inputDF, p.Func, p.PartBy, p.OrderBy)

	default:
		return nil, nil
	}
}

func applyMaskParallel(df *dataframe.DataFrame, mask []bool, workers int) (*dataframe.DataFrame, error) {
	result := dataframe.New()

	type colResult struct {
		index int
		col   series.Series
	}

	inputCh := make(chan int, df.NumCols())
	outputCh := make(chan colResult, df.NumCols())

	var wg sync.WaitGroup

	for i := 0; i < workers && i < df.NumCols(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for colIdx := range inputCh {
				col, _ := df.Col(colIdx)
				filteredCol := filterColumnParallel(col, mask)
				outputCh <- colResult{index: colIdx, col: filteredCol}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(outputCh)
	}()

	go func() {
		for i := 0; i < df.NumCols(); i++ {
			inputCh <- i
		}
		close(inputCh)
	}()

	cols := make([]series.Series, df.NumCols())
	for result := range outputCh {
		cols[result.index] = result.col
	}

	for _, col := range cols {
		if col != nil {
			result.AddSeries(col)
		}
	}

	return result, nil
}

func filterColumnParallel(col series.Series, mask []bool) series.Series {
	alloc := memory.DefaultAllocator

	count := 0
	for _, v := range mask {
		if v {
			count++
		}
	}

	switch typedCol := col.(type) {
	case *series.StringSeries:
		filtered := make([]string, count)
		valid := make([]bool, count)
		idx := 0
		for j, keep := range mask {
			if keep {
				filtered[idx] = typedCol.Value(j)
				valid[idx] = !typedCol.IsNull(j)
				idx++
			}
		}
		return series.NewStringSeries(typedCol.Name(), alloc, filtered, valid)

	case *series.Int64Series:
		filtered := make([]int64, count)
		valid := make([]bool, count)
		idx := 0
		for j, keep := range mask {
			if keep {
				filtered[idx] = typedCol.Value(j)
				valid[idx] = !typedCol.IsNull(j)
				idx++
			}
		}
		return series.NewInt64Series(typedCol.Name(), alloc, filtered, valid)

	case *series.Float64Series:
		filtered := make([]float64, count)
		valid := make([]bool, count)
		idx := 0
		for j, keep := range mask {
			if keep {
				filtered[idx] = typedCol.Value(j)
				valid[idx] = !typedCol.IsNull(j)
				idx++
			}
		}
		return series.NewFloat64Series(typedCol.Name(), alloc, filtered, valid)

	case *series.BooleanSeries:
		filtered := make([]bool, count)
		valid := make([]bool, count)
		idx := 0
		for j, keep := range mask {
			if keep {
				filtered[idx] = typedCol.Value(j)
				valid[idx] = !typedCol.IsNull(j)
				idx++
			}
		}
		return series.NewBooleanSeries(typedCol.Name(), alloc, filtered, valid)

	default:
		return col
	}
}
