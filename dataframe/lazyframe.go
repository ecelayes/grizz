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
	Columns   []string
	NumRows   int
}

func (s ScanPlan) Explain(indent int) string {
	pad := strings.Repeat("  ", indent)
	colsInfo := ""
	if len(s.Columns) > 0 {
		colsInfo = " (projected: " + strings.Join(s.Columns, ", ") + ")"
	}
	rowsInfo := ""
	if s.NumRows > 0 {
		rowsInfo = fmt.Sprintf(" (limit: %d)", s.NumRows)
	}
	return fmt.Sprintf("%sScan DataFrame (Rows: %d, Cols: %d)%s%s", pad, s.DataFrame.NumRows(), s.DataFrame.NumCols(), colsInfo, rowsInfo)
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

func (lf *LazyFrame) Collect(execFn func(LogicalPlan) (*DataFrame, error)) (*DataFrame, error) {
	return execFn(lf.plan)
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

type GroupByHeadPlan struct {
	Input LogicalPlan
	Keys  []string
	N     int
}

func (g GroupByHeadPlan) Explain(indent int) string {
	pad := strings.Repeat("  ", indent)
	inputStr := g.Input.Explain(indent + 1)
	return fmt.Sprintf("%sGroupBy Head: [%s] n=%d\n%s", pad, strings.Join(g.Keys, ", "), g.N, inputStr)
}

func (lgb *LazyGroupBy) Head(n int) *LazyFrame {
	return &LazyFrame{
		plan: GroupByHeadPlan{
			Input: lgb.lf.plan,
			Keys:  lgb.keys,
			N:     n,
		},
	}
}

type GroupByTailPlan struct {
	Input LogicalPlan
	Keys  []string
	N     int
}

func (g GroupByTailPlan) Explain(indent int) string {
	pad := strings.Repeat("  ", indent)
	inputStr := g.Input.Explain(indent + 1)
	return fmt.Sprintf("%sGroupBy Tail: [%s] n=%d\n%s", pad, strings.Join(g.Keys, ", "), g.N, inputStr)
}

func (lgb *LazyGroupBy) Tail(n int) *LazyFrame {
	return &LazyFrame{
		plan: GroupByTailPlan{
			Input: lgb.lf.plan,
			Keys:  lgb.keys,
			N:     n,
		},
	}
}

type GroupByGroupsPlan struct {
	Input LogicalPlan
	Keys  []string
}

func (g GroupByGroupsPlan) Explain(indent int) string {
	pad := strings.Repeat("  ", indent)
	inputStr := g.Input.Explain(indent + 1)
	return fmt.Sprintf("%sGroupBy Groups: [%s]\n%s", pad, strings.Join(g.Keys, ", "), inputStr)
}

func (lgb *LazyGroupBy) Groups() *LazyFrame {
	return &LazyFrame{
		plan: GroupByGroupsPlan{
			Input: lgb.lf.plan,
			Keys:  lgb.keys,
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

func (lf *LazyFrame) Head(n int) *LazyFrame {
	return lf.Limit(n)
}

func (lf *LazyFrame) Tail(n int) *LazyFrame {
	return &LazyFrame{
		plan: TailPlan{
			Input: lf.plan,
			N:     n,
		},
	}
}

type TailPlan struct {
	Input LogicalPlan
	N     int
}

func (t TailPlan) Explain(indent int) string {
	pad := strings.Repeat("  ", indent)
	inputStr := t.Input.Explain(indent + 1)
	return fmt.Sprintf("%sTail: %d\n%s", pad, t.N, inputStr)
}

type SamplePlan struct {
	Input   LogicalPlan
	N       int
	Frac    float64
	Replace bool
}

func (s SamplePlan) Explain(indent int) string {
	pad := strings.Repeat("  ", indent)
	inputStr := s.Input.Explain(indent + 1)
	return fmt.Sprintf("%sSample: n=%d, frac=%.2f, replace=%t\n%s", pad, s.N, s.Frac, s.Replace, inputStr)
}

func (lf *LazyFrame) Sample(n int, replace bool) *LazyFrame {
	return &LazyFrame{
		plan: SamplePlan{
			Input:   lf.plan,
			N:       n,
			Replace: replace,
		},
	}
}

func (lf *LazyFrame) SampleFrac(frac float64, replace bool) *LazyFrame {
	return &LazyFrame{
		plan: SamplePlan{
			Input:   lf.plan,
			Frac:    frac,
			Replace: replace,
		},
	}
}

type JoinType string

const (
	Inner JoinType = "Inner"
	Left  JoinType = "Left"
	Right JoinType = "Right"
	Outer JoinType = "Outer"
	Cross JoinType = "Cross"
	Semi  JoinType = "Semi"
	Anti  JoinType = "Anti"
)

type JoinPlan struct {
	Left   LogicalPlan
	Right  LogicalPlan
	On     string
	OnCols []string
	How    JoinType
}

func (j JoinPlan) Explain(indent int) string {
	pad := strings.Repeat("  ", indent)
	leftStr := j.Left.Explain(indent + 1)
	rightStr := j.Right.Explain(indent + 1)
	onStr := j.On
	if len(j.OnCols) > 0 {
		onStr = "[" + strings.Join(j.OnCols, ", ") + "]"
	}
	return fmt.Sprintf("%sJoin: %s on '%s'\n%s%s", pad, j.How, onStr, leftStr, rightStr)
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

func (lf *LazyFrame) JoinOn(other *LazyFrame, onCols []string, how JoinType) *LazyFrame {
	return &LazyFrame{
		plan: JoinPlan{
			Left:   lf.plan,
			Right:  other.plan,
			OnCols: onCols,
			How:    how,
		},
	}
}

type WithColumnsPlan struct {
	Input   LogicalPlan
	Columns []expr.Expr
}

func (w WithColumnsPlan) Explain(indent int) string {
	pad := strings.Repeat("  ", indent)
	var cols []string
	for _, c := range w.Columns {
		cols = append(cols, c.String())
	}
	inputStr := w.Input.Explain(indent + 1)
	return fmt.Sprintf("%sWithColumns: %s\n%s", pad, strings.Join(cols, ", "), inputStr)
}

func (lf *LazyFrame) WithColumns(columns ...expr.Expr) *LazyFrame {
	return &LazyFrame{
		plan: WithColumnsPlan{
			Input:   lf.plan,
			Columns: columns,
		},
	}
}

type DropNullsPlan struct {
	Input LogicalPlan
}

func (d DropNullsPlan) Explain(indent int) string {
	pad := strings.Repeat("  ", indent)
	inputStr := d.Input.Explain(indent + 1)
	return fmt.Sprintf("%sDropNulls\n%s", pad, inputStr)
}

func (lf *LazyFrame) DropNulls() *LazyFrame {
	return &LazyFrame{
		plan: DropNullsPlan{
			Input: lf.plan,
		},
	}
}

type DistinctPlan struct {
	Input LogicalPlan
}

func (d DistinctPlan) Explain(indent int) string {
	pad := strings.Repeat("  ", indent)
	inputStr := d.Input.Explain(indent + 1)
	return fmt.Sprintf("%sDistinct\n%s", pad, inputStr)
}

func (lf *LazyFrame) Distinct() *LazyFrame {
	return &LazyFrame{
		plan: DistinctPlan{
			Input: lf.plan,
		},
	}
}

func (lf *LazyFrame) Unique() *LazyFrame {
	return lf.Distinct()
}

type WindowPlan struct {
	Input   LogicalPlan
	Func    expr.WindowExpr
	PartBy  []string
	OrderBy []string
}

func (w WindowPlan) Explain(indent int) string {
	pad := strings.Repeat("  ", indent)
	inputStr := w.Input.Explain(indent + 1)
	return fmt.Sprintf("%sWindow: %s\n%s", pad, w.Func.String(), inputStr)
}

func (lf *LazyFrame) WithWindow(funcExpr expr.WindowExpr, partBy []string, orderBy []string) *LazyFrame {
	return &LazyFrame{
		plan: WindowPlan{
			Input:   lf.plan,
			Func:    funcExpr,
			PartBy:  partBy,
			OrderBy: orderBy,
		},
	}
}

type MeltPlan struct {
	Input     LogicalPlan
	IdVars    []string
	ValueVars []string
}

func (m MeltPlan) Explain(indent int) string {
	pad := strings.Repeat("  ", indent)
	inputStr := m.Input.Explain(indent + 1)
	return fmt.Sprintf("%sMelt: id_vars=%v, value_vars=%v\n%s", pad, m.IdVars, m.ValueVars, inputStr)
}

func (lf *LazyFrame) Melt(idVars, valueVars []string) *LazyFrame {
	return &LazyFrame{
		plan: MeltPlan{
			Input:     lf.plan,
			IdVars:    idVars,
			ValueVars: valueVars,
		},
	}
}
