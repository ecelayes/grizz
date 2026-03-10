package engine

import (
	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
)

type Optimizer struct{}

func NewOptimizer() *Optimizer {
	return &Optimizer{}
}

func Optimize(plan dataframe.LogicalPlan) dataframe.LogicalPlan {
	opt := NewOptimizer()
	return opt.optimize(plan)
}

func (o *Optimizer) optimize(plan dataframe.LogicalPlan) dataframe.LogicalPlan {
	if plan == nil {
		return plan
	}

	switch p := plan.(type) {
	case dataframe.FilterPlan:
		p.Input = o.optimize(p.Input)
		p = o.optimizeFilter(p)
		p = o.pushDownFilter(p)
		return p

	case dataframe.SelectPlan:
		p.Input = o.optimize(p.Input)
		p = o.optimizeSelect(p)
		return p

	case dataframe.WithColumnsPlan:
		p.Input = o.optimize(p.Input)
		return p

	case dataframe.JoinPlan:
		p.Left = o.optimize(p.Left)
		p.Right = o.optimize(p.Right)
		p = o.optimizeJoin(p)
		p = o.pushDownFilterToJoin(p)
		return p

	case dataframe.GroupByPlan:
		p.Input = o.optimize(p.Input)
		return p

	case dataframe.OrderByPlan:
		p.Input = o.optimize(p.Input)
		return p

	case dataframe.LimitPlan:
		p.Input = o.optimize(p.Input)
		p = o.optimizeLimit(p)
		return p

	case dataframe.TailPlan:
		p.Input = o.optimize(p.Input)
		return p

	case dataframe.SamplePlan:
		p.Input = o.optimize(p.Input)
		return p

	case dataframe.WindowPlan:
		p.Input = o.optimize(p.Input)
		return p

	case dataframe.DistinctPlan:
		p.Input = o.optimize(p.Input)
		return p

	case dataframe.DropNullsPlan:
		p.Input = o.optimize(p.Input)
		return p

	default:
		return p
	}
}

func (o *Optimizer) optimizeFilter(p dataframe.FilterPlan) dataframe.FilterPlan {
	input := p.Input

	if innerFilter, ok := input.(dataframe.FilterPlan); ok {
		combined := expr.And(innerFilter.Condition, p.Condition)
		return dataframe.FilterPlan{
			Input:     innerFilter.Input,
			Condition: combined,
		}
	}

	return p
}

func (o *Optimizer) optimizeSelect(p dataframe.SelectPlan) dataframe.SelectPlan {
	input := p.Input

	if innerSelect, ok := input.(dataframe.SelectPlan); ok {
		neededCols := make(map[string]bool)
		for _, col := range p.Columns {
			if c, ok := col.(expr.Column); ok {
				neededCols[c.Name] = true
			}
		}

		var newOuterCols []expr.Expr
		for _, col := range innerSelect.Columns {
			if c, ok := col.(expr.Column); ok {
				if neededCols[c.Name] {
					newOuterCols = append(newOuterCols, col)
				}
			}
		}

		if len(newOuterCols) > 0 {
			return dataframe.SelectPlan{
				Input:   innerSelect.Input,
				Columns: newOuterCols,
			}
		}
	}

	return p
}

func (o *Optimizer) optimizeJoin(p dataframe.JoinPlan) dataframe.JoinPlan {
	if selectPlan, ok := p.Left.(dataframe.SelectPlan); ok {
		p.Left = selectPlan.Input
	}

	if selectPlan, ok := p.Right.(dataframe.SelectPlan); ok {
		p.Right = selectPlan.Input
	}

	return p
}

func (o *Optimizer) optimizeLimit(p dataframe.LimitPlan) dataframe.LimitPlan {
	input := p.Input

	if innerLimit, ok := input.(dataframe.LimitPlan); ok {
		if p.Limit < innerLimit.Limit {
			return p
		}
		return dataframe.LimitPlan{
			Input: innerLimit.Input,
			Limit: innerLimit.Limit,
		}
	}

	return p
}

func (o *Optimizer) canPushDown(plan dataframe.LogicalPlan) bool {
	switch plan.(type) {
	case dataframe.ScanPlan:
		return true
	case dataframe.FilterPlan:
		return true
	default:
		return false
	}
}

func (o *Optimizer) pushDownFilter(p dataframe.FilterPlan) dataframe.FilterPlan {
	input := p.Input

	switch inp := input.(type) {
	case dataframe.SelectPlan:
		newFilter := dataframe.FilterPlan{
			Input:     inp.Input,
			Condition: p.Condition,
		}
		return dataframe.FilterPlan{
			Input:     newFilter,
			Condition: p.Condition,
		}

	case dataframe.WithColumnsPlan:
		newFilter := dataframe.FilterPlan{
			Input:     inp.Input,
			Condition: p.Condition,
		}
		return dataframe.FilterPlan{
			Input:     dataframe.WithColumnsPlan{Input: newFilter, Columns: inp.Columns},
			Condition: p.Condition,
		}

	case dataframe.LimitPlan:
		newFilter := dataframe.FilterPlan{
			Input:     inp.Input,
			Condition: p.Condition,
		}
		return dataframe.FilterPlan{
			Input:     dataframe.LimitPlan{Input: newFilter, Limit: inp.Limit},
			Condition: p.Condition,
		}
	}

	return p
}

func (o *Optimizer) pushDownFilterToJoin(p dataframe.JoinPlan) dataframe.JoinPlan {
	leftFilter, leftHasFilter := p.Left.(dataframe.FilterPlan)
	rightFilter, rightHasFilter := p.Right.(dataframe.FilterPlan)

	if leftHasFilter && !rightHasFilter {
		p.Right = dataframe.FilterPlan{
			Input:     p.Right,
			Condition: leftFilter.Condition,
		}
		p.Left = leftFilter.Input
		return p
	}

	if rightHasFilter && !leftHasFilter {
		p.Left = dataframe.FilterPlan{
			Input:     p.Left,
			Condition: rightFilter.Condition,
		}
		p.Right = rightFilter.Input
		return p
	}

	if leftHasFilter && rightHasFilter {
		p.Left = leftFilter.Input
		p.Right = rightFilter.Input
		return p
	}

	return p
}
