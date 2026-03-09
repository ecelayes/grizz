package dataframe

import (
	"fmt"
	"strings"

	"github.com/ecelayes/grizz/expr"
)

type LogicalPlan interface {
	Explain(indent int) string
}

type ScanPlan struct {
	DataFrame *DataFrame
}

func (s ScanPlan) Explain(indent int) string {
	pad := strings.Repeat("  ", indent)
	return fmt.Sprintf("%sScan DataFrame (Rows: %d, Cols: %d)", pad, s.DataFrame.NumRows(), s.DataFrame.NumCols())
}

type FilterPlan struct {
	Input     LogicalPlan
	Condition expr.Expr
}

func (f FilterPlan) Explain(indent int) string {
	pad := strings.Repeat("  ", indent)
	inputStr := f.Input.Explain(indent + 1)
	return fmt.Sprintf("%sFilter: %s\n%s", pad, f.Condition.String(), inputStr)
}

type SelectPlan struct {
	Input   LogicalPlan
	Columns []expr.Expr
}

func (s SelectPlan) Explain(indent int) string {
	pad := strings.Repeat("  ", indent)
	var cols []string
	for _, c := range s.Columns {
		cols = append(cols, c.String())
	}
	inputStr := s.Input.Explain(indent + 1)
	return fmt.Sprintf("%sSelect: %s\n%s", pad, strings.Join(cols, ", "), inputStr)
}

type LazyFrame struct {
	plan LogicalPlan
}

func (df *DataFrame) Lazy() *LazyFrame {
	return &LazyFrame{
		plan: ScanPlan{DataFrame: df},
	}
}

func (lf *LazyFrame) Filter(condition expr.Expr) *LazyFrame {
	return &LazyFrame{
		plan: FilterPlan{
			Input:     lf.plan,
			Condition: condition,
		},
	}
}

func (lf *LazyFrame) Select(columns ...expr.Expr) *LazyFrame {
	return &LazyFrame{
		plan: SelectPlan{
			Input:   lf.plan,
			Columns: columns,
		},
	}
}

func (lf *LazyFrame) Explain() string {
	return "Logical Plan:\n" + lf.plan.Explain(1)
}

func (lf *LazyFrame) Plan() LogicalPlan {
	return lf.plan
}

type GroupByPlan struct {
	Input LogicalPlan
	Keys  []string
	Aggs  []expr.Aggregation
}

func (g GroupByPlan) Explain(indent int) string {
	pad := strings.Repeat("  ", indent)
	var aggs []string
	for _, a := range g.Aggs {
		aggs = append(aggs, a.String())
	}
	inputStr := g.Input.Explain(indent + 1)
	return fmt.Sprintf("%sGroupBy: [%s] Agg: [%s]\n%s", pad, strings.Join(g.Keys, ", "), strings.Join(aggs, ", "), inputStr)
}

type LazyGroupBy struct {
	lf   *LazyFrame
	keys []string
}

func (lf *LazyFrame) GroupBy(keys ...string) *LazyGroupBy {
	return &LazyGroupBy{
		lf:   lf,
		keys: keys,
	}
}

func (lgb *LazyGroupBy) Agg(aggs ...expr.Aggregation) *LazyFrame {
	return &LazyFrame{
		plan: GroupByPlan{
			Input: lgb.lf.plan,
			Keys:  lgb.keys,
			Aggs:  aggs,
		},
	}
}

type OrderByPlan struct {
	Input      LogicalPlan
	Column     string
	Descending bool
}

func (o OrderByPlan) Explain(indent int) string {
	pad := strings.Repeat("  ", indent)
	dir := "ASC"
	if o.Descending {
		dir = "DESC"
	}
	inputStr := o.Input.Explain(indent + 1)
	return fmt.Sprintf("%sOrderBy: %s (%s)\n%s", pad, o.Column, dir, inputStr)
}

func (lf *LazyFrame) OrderBy(column string, descending bool) *LazyFrame {
	return &LazyFrame{
		plan: OrderByPlan{
			Input:      lf.plan,
			Column:     column,
			Descending: descending,
		},
	}
}

type LimitPlan struct {
	Input LogicalPlan
	Limit int
}

func (l LimitPlan) Explain(indent int) string {
	pad := strings.Repeat("  ", indent)
	inputStr := l.Input.Explain(indent + 1)
	return fmt.Sprintf("%sLimit: %d\n%s", pad, l.Limit, inputStr)
}

func (lf *LazyFrame) Limit(n int) *LazyFrame {
	return &LazyFrame{
		plan: LimitPlan{
			Input: lf.plan,
			Limit: n,
		},
	}
}

type JoinType string

const (
	Inner JoinType = "Inner"
	Left  JoinType = "Left"
)

type JoinPlan struct {
	Left  LogicalPlan
	Right LogicalPlan
	On    string
	How   JoinType
}

func (j JoinPlan) Explain(indent int) string {
	pad := strings.Repeat("  ", indent)
	leftStr := j.Left.Explain(indent + 1)
	rightStr := j.Right.Explain(indent + 1)
	return fmt.Sprintf("%sJoin: %s on '%s'\n%s%s", pad, j.How, j.On, leftStr, rightStr)
}

func (lf *LazyFrame) Join(other *LazyFrame, on string, how JoinType) *LazyFrame {
	return &LazyFrame{
		plan: JoinPlan{
			Left:  lf.plan,
			Right: other.plan,
			On:    on,
			How:   how,
		},
	}
}
