package engine

import (
	"runtime"
	"sync"

	"github.com/ecelayes/grizz/dataframe"
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
	return col
}
