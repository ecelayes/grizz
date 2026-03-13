package engine

import (
	"errors"

	"github.com/ecelayes/grizz/dataframe"
)

func Execute(plan dataframe.LogicalPlan) (*dataframe.DataFrame, error) {
	optimizedPlan := Optimize(plan)
	return executePlan(optimizedPlan)
}

func executePlan(plan dataframe.LogicalPlan) (*dataframe.DataFrame, error) {
	switch p := plan.(type) {
	case dataframe.ScanPlan:
		df := p.DataFrame
		if p.NumRows > 0 && p.NumRows < df.NumRows() {
			return applyLimit(df, p.NumRows)
		}
		if len(p.Columns) > 0 {
			return applyProjectionAtScan(p.DataFrame, p.Columns), nil
		}
		return p.DataFrame, nil

	case dataframe.FilterPlan:
		inputDF, err := executePlan(p.Input)
		if err != nil {
			return nil, err
		}

		mask, err := evaluateCondition(inputDF, p.Condition)
		if err != nil {
			return nil, err
		}

		return applyMask(inputDF, mask)

	case dataframe.SelectPlan:
		inputDF, err := executePlan(p.Input)
		if err != nil {
			return nil, err
		}
		return applyProjection(inputDF, p.Columns)

	case dataframe.WithColumnsPlan:
		inputDF, err := executePlan(p.Input)
		if err != nil {
			return nil, err
		}
		return applyWithColumns(inputDF, p.Columns)

	case dataframe.GroupByPlan:
		inputDF, err := executePlan(p.Input)
		if err != nil {
			return nil, err
		}
		return applyGroupBy(inputDF, p.Keys, p.Aggs)

	case dataframe.GroupByHeadPlan:
		inputDF, err := executePlan(p.Input)
		if err != nil {
			return nil, err
		}
		return applyGroupByHead(inputDF, p.Keys, p.N)

	case dataframe.GroupByTailPlan:
		inputDF, err := executePlan(p.Input)
		if err != nil {
			return nil, err
		}
		return applyGroupByTail(inputDF, p.Keys, p.N)

	case dataframe.GroupByGroupsPlan:
		inputDF, err := executePlan(p.Input)
		if err != nil {
			return nil, err
		}
		return applyGroupByGroups(inputDF, p.Keys)

	case dataframe.OrderByPlan:
		inputDF, err := executePlan(p.Input)
		if err != nil {
			return nil, err
		}
		return applyOrderBy(inputDF, p.Column, p.Descending)

	case dataframe.LimitPlan:
		inputDF, err := executePlan(p.Input)
		if err != nil {
			return nil, err
		}
		return applyLimit(inputDF, p.Limit)

	case dataframe.TailPlan:
		inputDF, err := executePlan(p.Input)
		if err != nil {
			return nil, err
		}
		return applyTail(inputDF, p.N)

	case dataframe.SamplePlan:
		inputDF, err := executePlan(p.Input)
		if err != nil {
			return nil, err
		}
		return applySample(inputDF, p.N, p.Frac, p.Replace)

	case dataframe.JoinPlan:
		leftDF, err := executePlan(p.Left)
		if err != nil {
			return nil, err
		}
		rightDF, err := executePlan(p.Right)
		if err != nil {
			return nil, err
		}
		onCol := p.On
		if len(p.OnCols) > 0 {
			onCol = p.OnCols[0]
		}
		return applyJoin(leftDF, rightDF, onCol, p.How)

	case dataframe.DropNullsPlan:
		inputDF, err := executePlan(p.Input)
		if err != nil {
			return nil, err
		}
		return applyDropNulls(inputDF)

	case dataframe.DistinctPlan:
		inputDF, err := executePlan(p.Input)
		if err != nil {
			return nil, err
		}
		return applyDistinct(inputDF)

	case dataframe.WindowPlan:
		inputDF, err := executePlan(p.Input)
		if err != nil {
			return nil, err
		}
		return applyWindow(inputDF, p.Func, p.PartBy, p.OrderBy)

	case dataframe.MeltPlan:
		inputDF, err := executePlan(p.Input)
		if err != nil {
			return nil, err
		}
		return applyMelt(inputDF, p.IdVars, p.ValueVars)

	default:
		return nil, errors.New("unknown logical plan node")
	}
}

func Collect(lf *dataframe.LazyFrame) (*dataframe.DataFrame, error) {
	optimizedPlan := Optimize(lf.Plan())
	return executePlan(optimizedPlan)
}
