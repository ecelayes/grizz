package engine

import (
	"errors"
	"time"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

type datetimeExtractor func(time.Time) int64

func applyDatetime(df *dataframe.DataFrame, colName string, layout string, extract datetimeExtractor, resultName string, alloc memory.Allocator) (series.Series, error) {
	col, err := df.ColByName(colName)
	if err != nil {
		return nil, err
	}

	strCol, ok := col.(*series.StringSeries)
	if !ok {
		return nil, errors.New("datetime operations only support string columns")
	}

	result := make([]int64, strCol.Len())
	for i := 0; i < strCol.Len(); i++ {
		if strCol.IsNull(i) {
			result[i] = 0
			continue
		}
		t, err := time.Parse(layout, strCol.Value(i))
		if err != nil {
			result[i] = 0
		} else {
			result[i] = extract(t)
		}
	}

	return series.NewInt64Series(resultName, alloc, result, nil), nil
}

func applyYear(df *dataframe.DataFrame, ye expr.YearExpr, alloc memory.Allocator) (series.Series, error) {
	colExpr, ok := ye.Expr.(expr.Column)
	if !ok {
		return nil, errors.New("Year only supports column expressions")
	}
	return applyDatetime(df, colExpr.Name, "2006-01-02", func(t time.Time) int64 { return int64(t.Year()) }, colExpr.Name+"_year", alloc)
}

func applyMonth(df *dataframe.DataFrame, me expr.MonthExpr, alloc memory.Allocator) (series.Series, error) {
	colExpr, ok := me.Expr.(expr.Column)
	if !ok {
		return nil, errors.New("Month only supports column expressions")
	}
	return applyDatetime(df, colExpr.Name, "2006-01-02", func(t time.Time) int64 { return int64(t.Month()) }, colExpr.Name+"_month", alloc)
}

func applyDay(df *dataframe.DataFrame, de expr.DayExpr, alloc memory.Allocator) (series.Series, error) {
	colExpr, ok := de.Expr.(expr.Column)
	if !ok {
		return nil, errors.New("Day only supports column expressions")
	}
	return applyDatetime(df, colExpr.Name, "2006-01-02", func(t time.Time) int64 { return int64(t.Day()) }, colExpr.Name+"_day", alloc)
}

func applyHour(df *dataframe.DataFrame, he expr.HourExpr, alloc memory.Allocator) (series.Series, error) {
	colExpr, ok := he.Expr.(expr.Column)
	if !ok {
		return nil, errors.New("Hour only supports column expressions")
	}
	return applyDatetime(df, colExpr.Name, "2006-01-02T15:04", func(t time.Time) int64 { return int64(t.Hour()) }, colExpr.Name+"_hour", alloc)
}

func applyMinute(df *dataframe.DataFrame, me expr.MinuteExpr, alloc memory.Allocator) (series.Series, error) {
	colExpr, ok := me.Expr.(expr.Column)
	if !ok {
		return nil, errors.New("Minute only supports column expressions")
	}
	return applyDatetime(df, colExpr.Name, "2006-01-02T15:04", func(t time.Time) int64 { return int64(t.Minute()) }, colExpr.Name+"_minute", alloc)
}

func applySecond(df *dataframe.DataFrame, se expr.SecondExpr, alloc memory.Allocator) (series.Series, error) {
	colExpr, ok := se.Expr.(expr.Column)
	if !ok {
		return nil, errors.New("Second only supports column expressions")
	}
	return applyDatetime(df, colExpr.Name, "2006-01-02T15:04:05", func(t time.Time) int64 { return int64(t.Second()) }, colExpr.Name+"_second", alloc)
}

func applyWeekday(df *dataframe.DataFrame, we expr.WeekdayExpr, alloc memory.Allocator) (series.Series, error) {
	colExpr, ok := we.Expr.(expr.Column)
	if !ok {
		return nil, errors.New("Weekday only supports column expressions")
	}
	return applyDatetime(df, colExpr.Name, "2006-01-02", func(t time.Time) int64 { return int64(t.Weekday()) }, colExpr.Name+"_weekday", alloc)
}
