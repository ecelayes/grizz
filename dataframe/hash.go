package dataframe

import (
	"hash/fnv"
	"strconv"

	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

type HashExpr struct {
	Expr expr.Expr
}

func (e HashExpr) String() string {
	return "hash()"
}

func Hash(e expr.Expr) HashExpr {
	return HashExpr{Expr: e}
}

func (df *DataFrame) IsUnique() *DataFrame {
	result := New()

	seen := make(map[string]bool)
	var indices []int
	var valid []bool

	for i := 0; i < df.NumRows(); i++ {
		key := rowKey(df, i)
		if !seen[key] {
			seen[key] = true
			indices = append(indices, i)
			valid = append(valid, true)
		}
	}

	for j := 0; j < df.NumCols(); j++ {
		col, _ := df.Col(j)
		newCol := filterSeriesByIndices(col, indices, valid)
		result.AddSeries(newCol)
	}

	return result
}

func (df *DataFrame) IsDuplicated() *DataFrame {
	result := New()

	seen := make(map[string]bool)
	firstOccurrence := make(map[string]int)

	for i := 0; i < df.NumRows(); i++ {
		key := rowKey(df, i)
		if _, exists := firstOccurrence[key]; !exists {
			firstOccurrence[key] = i
		}
		seen[key] = true
	}

	var indices []int
	var valid []bool

	for i := 0; i < df.NumRows(); i++ {
		key := rowKey(df, i)
		if seen[key] && firstOccurrence[key] != i {
			indices = append(indices, i)
			valid = append(valid, true)
		}
	}

	for j := 0; j < df.NumCols(); j++ {
		col, _ := df.Col(j)
		newCol := filterSeriesByIndices(col, indices, valid)
		result.AddSeries(newCol)
	}

	return result
}

func rowKey(df *DataFrame, row int) string {
	key := ""
	for i := 0; i < df.NumCols(); i++ {
		col, _ := df.Col(i)
		if col.IsNull(row) {
			key += "null"
		} else {
			switch c := col.(type) {
			case *series.Int64Series:
				key += string(rune(c.Value(row)))
			case *series.Float64Series:
				key += string(rune(int(c.Value(row))))
			case *series.StringSeries:
				key += c.Value(row)
			case *series.BooleanSeries:
				if c.Value(row) {
					key += "true"
				} else {
					key += "false"
				}
			}
		}
		key += "|"
	}
	return key
}

func filterSeriesByIndices(col series.Series, indices []int, valid []bool) series.Series {
	switch c := col.(type) {
	case *series.Int64Series:
		values := make([]int64, len(indices))
		for i, idx := range indices {
			values[i] = c.Value(idx)
		}
		return series.NewInt64Series(c.Name(), memory.DefaultAllocator, values, valid)
	case *series.Float64Series:
		values := make([]float64, len(indices))
		for i, idx := range indices {
			values[i] = c.Value(idx)
		}
		return series.NewFloat64Series(c.Name(), memory.DefaultAllocator, values, valid)
	case *series.StringSeries:
		values := make([]string, len(indices))
		for i, idx := range indices {
			values[i] = c.Value(idx)
		}
		return series.NewStringSeries(c.Name(), memory.DefaultAllocator, values, valid)
	case *series.BooleanSeries:
		values := make([]bool, len(indices))
		for i, idx := range indices {
			values[i] = c.Value(idx)
		}
		return series.NewBooleanSeries(c.Name(), memory.DefaultAllocator, values, valid)
	default:
		return col
	}
}

func HashValues(values ...interface{}) uint64 {
	h := fnv.New64a()
	for _, v := range values {
		switch val := v.(type) {
		case int64:
			h.Write([]byte(strconv.FormatInt(val, 10)))
		case float64:
			h.Write([]byte(strconv.FormatFloat(val, 'f', -1, 64)))
		case string:
			h.Write([]byte(val))
		case bool:
			if val {
				h.Write([]byte("true"))
			} else {
				h.Write([]byte("false"))
			}
		}
	}
	return h.Sum64()
}
