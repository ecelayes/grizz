package engine

import (
	"errors"

	"github.com/ecelayes/grizz/dataframe"
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

	case dataframe.TailPlan:
		inputDF, err := Execute(p.Input)
		if err != nil {
			return nil, err
		}
		return applyTail(inputDF, p.N)

	case dataframe.SamplePlan:
		inputDF, err := Execute(p.Input)
		if err != nil {
			return nil, err
		}
		return applySample(inputDF, p.N, p.Frac, p.Replace)

	case dataframe.JoinPlan:
		leftDF, err := Execute(p.Left)
		if err != nil {
			return nil, err
		}
		rightDF, err := Execute(p.Right)
		if err != nil {
			return nil, err
		}
		onCol := p.On
		if len(p.OnCols) > 0 {
			onCol = p.OnCols[0]
		}
		return applyJoin(leftDF, rightDF, onCol, p.How)

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

	case dataframe.WindowPlan:
		inputDF, err := Execute(p.Input)
		if err != nil {
			return nil, err
		}
		return applyWindow(inputDF, p.Func, p.PartBy, p.OrderBy)

	default:
		return nil, errors.New("unknown logical plan node")
	}
}
