package sql

import (
	"fmt"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/engine"
	"github.com/ecelayes/grizz/expr"
)

type Engine struct {
	df *dataframe.DataFrame
}

func NewEngine(df *dataframe.DataFrame) *Engine {
	return &Engine{df: df}
}

func (e *Engine) Execute(stmt *SQLStatement) (*dataframe.DataFrame, error) {
	lf := e.df.Lazy()

	hasWhere := stmt.Where.Condition != nil
	hasSelect := len(stmt.Select.Columns) > 0
	hasGroupBy := len(stmt.GroupBy.Columns) > 0
	isSelectAll := false

	if hasSelect {
		if len(stmt.Select.Columns) == 1 {
			if colRef, ok := stmt.Select.Columns[0].Expr.(ColumnRef); ok && colRef.Name == "*" {
				isSelectAll = true
			}
		}
	}

	if hasGroupBy {
		lf, err := e.applyGroupBy(lf, stmt)
		if err != nil {
			return nil, err
		}
		result, err := lf.Collect(engine.Execute)
		if err != nil {
			return nil, err
		}
		return result, nil
	}

	if hasWhere && hasSelect && !isSelectAll {
		condition, err := e.convertCondition(stmt.Where.Condition)
		if err != nil {
			return nil, fmt.Errorf("where clause: %w", err)
		}
		lf = lf.Filter(condition)
		result, err := lf.Collect(engine.Execute)
		if err != nil {
			return nil, err
		}
		return e.applySelect(result, stmt.Select)
	}

	if hasWhere {
		condition, err := e.convertCondition(stmt.Where.Condition)
		if err != nil {
			return nil, fmt.Errorf("where clause: %w", err)
		}
		lf = lf.Filter(condition)
	}

	if hasSelect && !isSelectAll {
		selects, err := e.convertSelectColumns(stmt.Select)
		if err != nil {
			return nil, fmt.Errorf("select columns: %w", err)
		}
		lf = lf.Select(selects...)
	}

	if stmt.Having.Condition != nil {
		condition, err := e.convertCondition(stmt.Having.Condition)
		if err != nil {
			return nil, fmt.Errorf("having clause: %w", err)
		}
		lf = lf.Filter(condition)
	}

	if len(stmt.OrderBy.Columns) > 0 {
		for i := len(stmt.OrderBy.Columns) - 1; i >= 0; i-- {
			ob := stmt.OrderBy.Columns[i]
			colRef, ok := ob.Expr.(ColumnRef)
			if !ok {
				return nil, fmt.Errorf("order by only supports column references")
			}
			lf = lf.OrderBy(colRef.Name, ob.Descending)
		}
	}

	if stmt.Limit.Count > 0 {
		lf = lf.Limit(stmt.Limit.Count)
	}

	if stmt.Select.IsDistinct {
		lf = lf.Distinct()
	}

	result, err := lf.Collect(engine.Execute)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (e *Engine) applySelect(df *dataframe.DataFrame, sc SelectClause) (*dataframe.DataFrame, error) {
	if len(sc.Columns) == 1 {
		colRef, ok := sc.Columns[0].Expr.(ColumnRef)
		if ok && colRef.Name == "*" {
			return df, nil
		}
	}

	selects, err := e.convertSelectColumns(sc)
	if err != nil {
		return nil, err
	}

	result := dataframe.New()
	for _, sel := range selects {
		colExpr, ok := sel.(expr.Column)
		if !ok {
			return nil, fmt.Errorf("select supports only column references, got %T", sel)
		}
		col, err := df.ColByName(colExpr.Name)
		if err != nil {
			return nil, err
		}
		if err := result.AddSeries(col); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (e *Engine) convertSelectColumns(sc SelectClause) ([]expr.Expr, error) {
	var selects []expr.Expr

	for _, col := range sc.Columns {
		colExpr, err := e.convertExpression(col.Expr)
		if err != nil {
			return nil, err
		}

		if col.Alias != "" {
			colExpr = expr.Alias(colExpr, col.Alias)
		}

		selects = append(selects, colExpr)
	}

	return selects, nil
}

func (e *Engine) convertExpression(ex Expression) (expr.Expr, error) {
	switch sqlExpr := ex.(type) {
	case ColumnRef:
		if sqlExpr.Name == "*" {
			return nil, fmt.Errorf("cannot use * in expression context")
		}
		return expr.Col(sqlExpr.Name), nil
	case Literal:
		return expr.Lit(sqlExpr.Value), nil
	case BinaryExpr:
		return e.convertBinaryExpr(sqlExpr)
	case LogicalExpr:
		return e.convertLogicalExpr(sqlExpr)
	case UnaryExpr:
		return e.convertUnaryExpr(sqlExpr)
	case BetweenExpr:
		return e.convertBetweenExpr(sqlExpr)
	case InExpr:
		return e.convertInExpr(sqlExpr)
	case LikeExpr:
		return e.convertLikeExpr(sqlExpr)
	case FunctionCallExpr:
		return e.convertFunctionCall(sqlExpr)
	case AggExpr:
		return e.convertAggExpr(sqlExpr)
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", ex)
	}
}

func (e *Engine) convertBinaryExpr(b BinaryExpr) (expr.Expr, error) {
	left, err := e.convertExpression(b.Left)
	if err != nil {
		return nil, err
	}
	right, err := e.convertExpression(b.Right)
	if err != nil {
		return nil, err
	}

	leftCol, leftOk := left.(expr.Column)
	rightCol, rightOk := right.(expr.Column)
	leftLit, leftLitOk := left.(expr.Literal)

	switch b.Op {
	case "==", "=":
		if leftOk {
			return leftCol.Eq(right), nil
		}
		if rightOk {
			return rightCol.Eq(left), nil
		}
		return nil, fmt.Errorf("comparison requires at least one column")
	case "!=", "<>":
		if leftOk {
			return leftCol.Ne(right), nil
		}
		if rightOk {
			return rightCol.Ne(left), nil
		}
		return nil, fmt.Errorf("comparison requires at least one column")
	case "<":
		if leftOk {
			return leftCol.Lt(right), nil
		}
		if rightOk {
			return rightCol.Gt(left), nil
		}
		return nil, fmt.Errorf("comparison requires at least one column")
	case ">":
		if leftOk {
			return leftCol.Gt(right), nil
		}
		if rightOk {
			return rightCol.Lt(left), nil
		}
		return nil, fmt.Errorf("comparison requires at least one column")
	case "<=":
		if leftOk {
			return leftCol.LtEq(right), nil
		}
		if rightOk {
			return rightCol.GtEq(left), nil
		}
		return nil, fmt.Errorf("comparison requires at least one column")
	case ">=":
		if leftOk {
			return leftCol.GtEq(right), nil
		}
		if rightOk {
			return rightCol.LtEq(left), nil
		}
		return nil, fmt.Errorf("comparison requires at least one column")
	default:
		if leftLitOk {
			switch leftLit.Value.(type) {
			case int64, int, float64:
				return nil, fmt.Errorf("cannot use binary operator %s with literal on left without column", b.Op)
			}
		}
		return nil, fmt.Errorf("unsupported binary operator: %s", b.Op)
	}
}

func (e *Engine) convertLogicalExpr(l LogicalExpr) (expr.Expr, error) {
	left, err := e.convertExpression(l.Left)
	if err != nil {
		return nil, err
	}
	right, err := e.convertExpression(l.Right)
	if err != nil {
		return nil, err
	}

	switch l.Op {
	case "And":
		return expr.And(left, right), nil
	case "Or":
		return expr.Or(left, right), nil
	default:
		return nil, fmt.Errorf("unsupported logical operator: %s", l.Op)
	}
}

func (e *Engine) convertUnaryExpr(u UnaryExpr) (expr.Expr, error) {
	subExpr, err := e.convertExpression(u.Expr)
	if err != nil {
		return nil, err
	}

	switch u.Op {
	case "Not":
		return expr.Not(subExpr), nil
	case "-":
		subCol, ok := subExpr.(expr.Column)
		if !ok {
			return nil, fmt.Errorf("unary minus only supports column expressions")
		}
		return subCol.Mul(expr.Lit(-1)), nil
	default:
		return nil, fmt.Errorf("unsupported unary operator: %s", u.Op)
	}
}

func (e *Engine) convertBetweenExpr(b BetweenExpr) (expr.Expr, error) {
	expr_, err := e.convertExpression(b.Expr)
	if err != nil {
		return nil, err
	}
	lower, err := e.convertExpression(b.Lower)
	if err != nil {
		return nil, err
	}
	upper, err := e.convertExpression(b.Upper)
	if err != nil {
		return nil, err
	}

	col, ok := expr_.(expr.Column)
	if !ok {
		return nil, fmt.Errorf("between only supports column expressions")
	}

	lowerVal, ok := lower.(expr.Literal)
	if !ok {
		return nil, fmt.Errorf("between lower bound must be a literal")
	}
	upperVal, ok := upper.(expr.Literal)
	if !ok {
		return nil, fmt.Errorf("between upper bound must be a literal")
	}

	return col.Between(lowerVal.Value, upperVal.Value), nil
}

func (e *Engine) convertInExpr(i InExpr) (expr.Expr, error) {
	expr_, err := e.convertExpression(i.Expr)
	if err != nil {
		return nil, err
	}

	col, ok := expr_.(expr.Column)
	if !ok {
		return nil, fmt.Errorf("IN only supports column expressions")
	}

	values := make([]any, len(i.Values))
	for idx, v := range i.Values {
		lit, ok := v.(Literal)
		if !ok {
			return nil, fmt.Errorf("IN values must be literals")
		}
		values[idx] = lit.Value
	}

	return col.IsIn(values), nil
}

func (e *Engine) convertLikeExpr(l LikeExpr) (expr.Expr, error) {
	expr_, err := e.convertExpression(l.Expr)
	if err != nil {
		return nil, err
	}
	pattern, err := e.convertExpression(l.Pattern)
	if err != nil {
		return nil, err
	}

	col, ok := expr_.(expr.Column)
	if !ok {
		return nil, fmt.Errorf("LIKE only supports column expressions")
	}

	pat, ok := pattern.(expr.Literal)
	if !ok {
		return nil, fmt.Errorf("LIKE pattern must be a literal")
	}

	patStr, ok := pat.Value.(string)
	if !ok {
		return nil, fmt.Errorf("LIKE pattern must be a string")
	}

	return expr.Contains(col, expr.Lit(patStr)), nil
}

func (e *Engine) convertFunctionCall(f FunctionCallExpr) (expr.Expr, error) {
	if len(f.Args) == 0 {
		return nil, fmt.Errorf("function %s requires at least one argument", f.Name)
	}

	argExpr, err := e.convertExpression(f.Args[0])
	if err != nil {
		return nil, err
	}

	col, ok := argExpr.(expr.Column)
	if !ok {
		return nil, fmt.Errorf("function arguments must be column references")
	}

	upperName := f.Name
	switch upperName {
	case "COUNT":
		return expr.Count(col.Name), nil
	case "SUM":
		return expr.Sum(col.Name), nil
	case "MIN":
		return expr.Min(col.Name), nil
	case "MAX":
		return expr.Max(col.Name), nil
	case "MEAN":
		return expr.Mean(col.Name), nil
	default:
		return nil, fmt.Errorf("unsupported function: %s", f.Name)
	}
}

func (e *Engine) convertAggExpr(a AggExpr) (expr.Expr, error) {
	colRef, ok := a.Expr.(ColumnRef)
	if !ok {
		return nil, fmt.Errorf("aggregate expression must be a column reference")
	}

	switch a.Func {
	case "COUNT":
		return expr.Count(colRef.Name), nil
	case "SUM":
		return expr.Sum(colRef.Name), nil
	case "MIN":
		return expr.Min(colRef.Name), nil
	case "MAX":
		return expr.Max(colRef.Name), nil
	case "MEAN":
		return expr.Mean(colRef.Name), nil
	default:
		return nil, fmt.Errorf("unsupported aggregate function: %s", a.Func)
	}
}

func (e *Engine) convertCondition(cond Expression) (expr.Expr, error) {
	switch c := cond.(type) {
	case BinaryExpr:
		return e.convertBinaryExpr(c)
	case LogicalExpr:
		return e.convertLogicalExpr(c)
	case UnaryExpr:
		return e.convertUnaryExpr(c)
	case BetweenExpr:
		return e.convertBetweenExpr(c)
	case InExpr:
		return e.convertInExpr(c)
	case LikeExpr:
		return e.convertLikeExpr(c)
	default:
		return e.convertExpression(c)
	}
}

func (e *Engine) applyGroupBy(lf *dataframe.LazyFrame, stmt *SQLStatement) (*dataframe.LazyFrame, error) {
	groupCols := make([]string, len(stmt.GroupBy.Columns))
	for i, col := range stmt.GroupBy.Columns {
		colRef, ok := col.(ColumnRef)
		if !ok {
			return nil, fmt.Errorf("group by must use column references")
		}
		groupCols[i] = colRef.Name
	}

	var aggs []expr.Aggregation
	for _, selCol := range stmt.Select.Columns {
		if selCol.IsAgg {
			aggExpr, err := e.convertAggExpr(AggExpr{
				Func:  selCol.AggFunc,
				Expr:  selCol.Expr,
				Alias: selCol.Alias,
			})
			if err != nil {
				return nil, err
			}
			agg, ok := aggExpr.(expr.Aggregation)
			if !ok {
				return nil, fmt.Errorf("expected aggregation expression")
			}
			aggs = append(aggs, agg)
		}
	}

	if len(aggs) == 0 {
		return nil, fmt.Errorf("group by requires at least one aggregation")
	}

	return lf.GroupBy(groupCols...).Agg(aggs...), nil
}
