package engine

import (
	"errors"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func applyWithColumns(df *dataframe.DataFrame, columns []expr.Expr) (*dataframe.DataFrame, error) {
	result := dataframe.New()
	alloc := memory.DefaultAllocator

	for i := 0; i < df.NumCols(); i++ {
		col, _ := df.Col(i)
		result.AddSeries(col)
	}

	for _, colExpr := range columns {
		newCol, err := evaluateExpression(df, colExpr, alloc)
		if err != nil {
			return nil, err
		}
		if newCol != nil {
			result.AddSeries(newCol)
		}
	}

	return result, nil
}

func evaluateExpression(df *dataframe.DataFrame, colExpr expr.Expr, alloc memory.Allocator) (series.Series, error) {
	switch e := colExpr.(type) {
	case expr.AliasExpr:
		result, err := evaluateExpression(df, e.Expr, alloc)
		if err != nil {
			return nil, err
		}
		if result != nil {
			result.SetName(e.Alias)
		}
		return result, nil
	case expr.ArithmeticOp:
		return applyArithmetic(df, e, alloc)
	case expr.FillNullExpr:
		return applyFillNull(df, e, alloc)
	case expr.FillNullForwardExpr:
		return applyFillNullForward(df, e, alloc)
	case expr.FillNullBackwardExpr:
		return applyFillNullBackward(df, e, alloc)
	case expr.CoalesceExpr:
		return applyCoalesce(df, e, alloc)
	case expr.ContainsExpr:
		return applyContains(df, e, alloc)
	case expr.ReplaceExpr:
		return applyReplace(df, e, alloc)
	case expr.UpperExpr:
		return applyUpper(df, e, alloc)
	case expr.LowerExpr:
		return applyLower(df, e, alloc)
	case expr.StripExpr:
		return applyStrip(df, e, alloc)
	case expr.LengthExpr:
		return applyLength(df, e, alloc)
	case expr.TrimExpr:
		return applyTrim(df, e, alloc)
	case expr.LPadExpr:
		return applyLPad(df, e, alloc)
	case expr.RPadExpr:
		return applyRPad(df, e, alloc)
	case expr.ContainsRegexExpr:
		return applyContainsRegex(df, e, alloc)
	case expr.ExtractExpr:
		return applyExtract(df, e, alloc)
	case expr.FindExpr:
		return applyFind(df, e, alloc)
	case expr.SliceExpr:
		return applySlice(df, e, alloc)
	case expr.SplitExpr:
		return applySplit(df, e, alloc)
	case expr.CastExpr:
		return applyCast(df, e, alloc)
	case expr.OtherwiseExpr:
		return applyOtherwise(df, e, alloc)
	case expr.BetweenExpr:
		return applyBetween(df, e, alloc)
	case expr.YearExpr:
		return applyYear(df, e, alloc)
	case expr.MonthExpr:
		return applyMonth(df, e, alloc)
	case expr.DayExpr:
		return applyDay(df, e, alloc)
	case expr.HourExpr:
		return applyHour(df, e, alloc)
	case expr.MinuteExpr:
		return applyMinute(df, e, alloc)
	case expr.SecondExpr:
		return applySecond(df, e, alloc)
	case expr.WeekdayExpr:
		return applyWeekday(df, e, alloc)
	case expr.ExplodeExpr:
		return applyExplode(df, e, alloc)
	case expr.RollingSumExpr:
		return applyRollingSum(df, e, alloc)
	case expr.RollingMeanExpr:
		return applyRollingMean(df, e, alloc)
	case expr.RollingMinExpr:
		return applyRollingMin(df, e, alloc)
	case expr.RollingMaxExpr:
		return applyRollingMax(df, e, alloc)
	case expr.CumSumExpr:
		return applyCumSum(df, e, alloc)
	case expr.CumProdExpr:
		return applyCumProd(df, e, alloc)
	case expr.CumMinExpr:
		return applyCumMin(df, e, alloc)
	case expr.CumMaxExpr:
		return applyCumMax(df, e, alloc)
	default:
		return nil, errors.New("unsupported expression in WithColumns")
	}
}
