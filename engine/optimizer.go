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
