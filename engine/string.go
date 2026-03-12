package engine

import (
	"errors"
	"regexp"
	"strconv"
	"strings"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func applyContains(df *dataframe.DataFrame, e expr.ContainsExpr, alloc memory.Allocator) (series.Series, error) {
	col, err := df.ColByName(e.Expr.(expr.Column).Name)
	if err != nil {
		return nil, err
	}
	strCol, ok := col.(*series.StringSeries)
	if !ok {
		return nil, errors.New("Contains requires string column")
	}
	substr := e.Substr.(expr.Literal).Value.(string)
	var resultVals []string
	var valid []bool
	for j := 0; j < strCol.Len(); j++ {
		if strCol.IsNull(j) {
			resultVals = append(resultVals, "")
			valid = append(valid, false)
		} else {
			resultVals = append(resultVals, strconv.FormatBool(strings.Contains(strCol.Value(j), substr)))
			valid = append(valid, true)
		}
	}
	return series.NewStringSeries("contains_result", alloc, resultVals, valid), nil
}

func applyReplace(df *dataframe.DataFrame, e expr.ReplaceExpr, alloc memory.Allocator) (series.Series, error) {
	col, err := df.ColByName(e.Expr.(expr.Column).Name)
	if err != nil {
		return nil, err
	}
	strCol, ok := col.(*series.StringSeries)
	if !ok {
		return nil, errors.New("Replace requires string column")
	}
	oldStr := e.Old.(expr.Literal).Value.(string)
	newStr := e.New.(expr.Literal).Value.(string)
	var resultVals []string
	var valid []bool
	for j := 0; j < strCol.Len(); j++ {
		if strCol.IsNull(j) {
			resultVals = append(resultVals, "")
			valid = append(valid, false)
		} else {
			resultVals = append(resultVals, strings.Replace(strCol.Value(j), oldStr, newStr, -1))
			valid = append(valid, true)
		}
	}
	return series.NewStringSeries(strCol.Name(), alloc, resultVals, valid), nil
}

func applyUpper(df *dataframe.DataFrame, e expr.UpperExpr, alloc memory.Allocator) (series.Series, error) {
	col, err := df.ColByName(e.Expr.(expr.Column).Name)
	if err != nil {
		return nil, err
	}
	strCol, ok := col.(*series.StringSeries)
	if !ok {
		return nil, errors.New("Upper requires string column")
	}
	var resultVals []string
	var valid []bool
	for j := 0; j < strCol.Len(); j++ {
		if strCol.IsNull(j) {
			resultVals = append(resultVals, "")
			valid = append(valid, false)
		} else {
			resultVals = append(resultVals, strings.ToUpper(strCol.Value(j)))
			valid = append(valid, true)
		}
	}
	return series.NewStringSeries(strCol.Name(), alloc, resultVals, valid), nil
}

func applyLower(df *dataframe.DataFrame, e expr.LowerExpr, alloc memory.Allocator) (series.Series, error) {
	col, err := df.ColByName(e.Expr.(expr.Column).Name)
	if err != nil {
		return nil, err
	}
	strCol, ok := col.(*series.StringSeries)
	if !ok {
		return nil, errors.New("Lower requires string column")
	}
	var resultVals []string
	var valid []bool
	for j := 0; j < strCol.Len(); j++ {
		if strCol.IsNull(j) {
			resultVals = append(resultVals, "")
			valid = append(valid, false)
		} else {
			resultVals = append(resultVals, strings.ToLower(strCol.Value(j)))
			valid = append(valid, true)
		}
	}
	return series.NewStringSeries(strCol.Name(), alloc, resultVals, valid), nil
}

func applyStrip(df *dataframe.DataFrame, e expr.StripExpr, alloc memory.Allocator) (series.Series, error) {
	col, err := df.ColByName(e.Expr.(expr.Column).Name)
	if err != nil {
		return nil, err
	}
	strCol, ok := col.(*series.StringSeries)
	if !ok {
		return nil, errors.New("Strip requires string column")
	}
	var resultVals []string
	var valid []bool
	for j := 0; j < strCol.Len(); j++ {
		if strCol.IsNull(j) {
			resultVals = append(resultVals, "")
			valid = append(valid, false)
		} else {
			resultVals = append(resultVals, strings.TrimSpace(strCol.Value(j)))
			valid = append(valid, true)
		}
	}
	return series.NewStringSeries(strCol.Name(), alloc, resultVals, valid), nil
}

func applyLength(df *dataframe.DataFrame, e expr.LengthExpr, alloc memory.Allocator) (series.Series, error) {
	col, err := df.ColByName(e.Expr.(expr.Column).Name)
	if err != nil {
		return nil, err
	}
	strCol, ok := col.(*series.StringSeries)
	if !ok {
		return nil, errors.New("Length requires string column")
	}
	var lenVals []int64
	var lenValid []bool
	for j := 0; j < strCol.Len(); j++ {
		if strCol.IsNull(j) {
			lenVals = append(lenVals, 0)
			lenValid = append(lenValid, false)
		} else {
			lenVals = append(lenVals, int64(len(strCol.Value(j))))
			lenValid = append(lenValid, true)
		}
	}
	return series.NewInt64Series(strCol.Name(), alloc, lenVals, lenValid), nil
}

func applyTrim(df *dataframe.DataFrame, e expr.TrimExpr, alloc memory.Allocator) (series.Series, error) {
	col, err := df.ColByName(e.Expr.(expr.Column).Name)
	if err != nil {
		return nil, err
	}
	strCol, ok := col.(*series.StringSeries)
	if !ok {
		return nil, errors.New("Trim requires string column")
	}
	var trimVals []string
	var trimValid []bool
	for j := 0; j < strCol.Len(); j++ {
		if strCol.IsNull(j) {
			trimVals = append(trimVals, "")
			trimValid = append(trimValid, false)
		} else {
			trimVals = append(trimVals, strings.Trim(strCol.Value(j), " \t\n"))
			trimValid = append(trimValid, true)
		}
	}
	return series.NewStringSeries(strCol.Name(), alloc, trimVals, trimValid), nil
}

func applyLPad(df *dataframe.DataFrame, e expr.LPadExpr, alloc memory.Allocator) (series.Series, error) {
	col, err := df.ColByName(e.Expr.(expr.Column).Name)
	if err != nil {
		return nil, err
	}
	strCol, ok := col.(*series.StringSeries)
	if !ok {
		return nil, errors.New("LPad requires string column")
	}
	targetLen := e.Length.(expr.Literal).Value.(int)
	var lpadVals []string
	var lpadValid []bool
	for j := 0; j < strCol.Len(); j++ {
		if strCol.IsNull(j) {
			lpadVals = append(lpadVals, "")
			lpadValid = append(lpadValid, false)
		} else {
			s := strCol.Value(j)
			if len(s) < targetLen {
				s = strings.Repeat(" ", targetLen-len(s)) + s
			}
			lpadVals = append(lpadVals, s)
			lpadValid = append(lpadValid, true)
		}
	}
	return series.NewStringSeries(strCol.Name(), alloc, lpadVals, lpadValid), nil
}

func applyRPad(df *dataframe.DataFrame, e expr.RPadExpr, alloc memory.Allocator) (series.Series, error) {
	col, err := df.ColByName(e.Expr.(expr.Column).Name)
	if err != nil {
		return nil, err
	}
	strCol, ok := col.(*series.StringSeries)
	if !ok {
		return nil, errors.New("RPad requires string column")
	}
	targetLen := e.Length.(expr.Literal).Value.(int)
	var rpadVals []string
	var rpadValid []bool
	for j := 0; j < strCol.Len(); j++ {
		if strCol.IsNull(j) {
			rpadVals = append(rpadVals, "")
			rpadValid = append(rpadValid, false)
		} else {
			s := strCol.Value(j)
			if len(s) < targetLen {
				s = s + strings.Repeat(" ", targetLen-len(s))
			}
			rpadVals = append(rpadVals, s)
			rpadValid = append(rpadValid, true)
		}
	}
	return series.NewStringSeries(strCol.Name(), alloc, rpadVals, rpadValid), nil
}

func applyContainsRegex(df *dataframe.DataFrame, e expr.ContainsRegexExpr, alloc memory.Allocator) (series.Series, error) {
	col, err := df.ColByName(e.Expr.(expr.Column).Name)
	if err != nil {
		return nil, err
	}
	strCol, ok := col.(*series.StringSeries)
	if !ok {
		return nil, errors.New("ContainsRegex requires string column")
	}
	pattern := e.Pattern.(expr.Literal).Value.(string)
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	var regexVals []string
	var regexValid []bool
	for j := 0; j < strCol.Len(); j++ {
		if strCol.IsNull(j) {
			regexVals = append(regexVals, "")
			regexValid = append(regexValid, false)
		} else {
			regexVals = append(regexVals, strconv.FormatBool(re.MatchString(strCol.Value(j))))
			regexValid = append(regexValid, true)
		}
	}
	return series.NewStringSeries("contains_regex", alloc, regexVals, regexValid), nil
}

func applySlice(df *dataframe.DataFrame, e expr.SliceExpr, alloc memory.Allocator) (series.Series, error) {
	col, err := df.ColByName(e.Expr.(expr.Column).Name)
	if err != nil {
		return nil, err
	}
	strCol, ok := col.(*series.StringSeries)
	if !ok {
		return nil, errors.New("Slice requires string column")
	}
	start := e.Start.(expr.Literal).Value.(int)
	length := e.Length.(expr.Literal).Value.(int)
	var sliceVals []string
	var sliceValid []bool
	for j := 0; j < strCol.Len(); j++ {
		if strCol.IsNull(j) {
			sliceVals = append(sliceVals, "")
			sliceValid = append(sliceValid, false)
		} else {
			s := strCol.Value(j)
			if start < len(s) {
				end := start + length
				if end > len(s) {
					end = len(s)
				}
				sliceVals = append(sliceVals, s[start:end])
				sliceValid = append(sliceValid, true)
			} else {
				sliceVals = append(sliceVals, "")
				sliceValid = append(sliceValid, false)
			}
		}
	}
	return series.NewStringSeries(strCol.Name(), alloc, sliceVals, sliceValid), nil
}

func applySplit(df *dataframe.DataFrame, e expr.SplitExpr, alloc memory.Allocator) (series.Series, error) {
	col, err := df.ColByName(e.Expr.(expr.Column).Name)
	if err != nil {
		return nil, err
	}
	strCol, ok := col.(*series.StringSeries)
	if !ok {
		return nil, errors.New("Split requires string column")
	}
	delim := e.Delim.(expr.Literal).Value.(string)
	var splitVals []string
	var splitValid []bool
	for j := 0; j < strCol.Len(); j++ {
		if strCol.IsNull(j) {
			splitVals = append(splitVals, "")
			splitValid = append(splitValid, false)
		} else {
			parts := strings.Split(strCol.Value(j), delim)
			splitVals = append(splitVals, strings.Join(parts, "|"))
			splitValid = append(splitValid, true)
		}
	}
	return series.NewStringSeries(strCol.Name(), alloc, splitVals, splitValid), nil
}
