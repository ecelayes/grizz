package dataframe

import (
	"errors"
	"fmt"

	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

type DataFrame struct {
	columns []series.Series
	rows    int
}

func New() *DataFrame {
	return &DataFrame{
		columns: make([]series.Series, 0),
		rows:    0,
	}
}

func (df *DataFrame) AddSeries(s series.Series) error {
	if df.rows > 0 && s.Len() != df.rows {
		return fmt.Errorf("series length %d does not match dataframe rows %d", s.Len(), df.rows)
	}

	if df.rows == 0 {
		df.rows = s.Len()
	}

	df.columns = append(df.columns, s)
	return nil
}

func (df *DataFrame) NumCols() int {
	return len(df.columns)
}

func (df *DataFrame) NumRows() int {
	return df.rows
}

func (df *DataFrame) Col(index int) (series.Series, error) {
	if index < 0 || index >= len(df.columns) {
		return nil, errors.New("column index out of bounds")
	}
	return df.columns[index], nil
}

func (df *DataFrame) Release() {
	for _, col := range df.columns {
		col.Release()
	}
}

func (df *DataFrame) ColByName(name string) (series.Series, error) {
	for _, col := range df.columns {
		if col.Name() == name {
			return col, nil
		}
	}
	return nil, fmt.Errorf("column %s not found", name)
}

func (df *DataFrame) Concat(other *DataFrame) (*DataFrame, error) {
	if df.NumCols() != other.NumCols() {
		return nil, fmt.Errorf("cannot concat: different number of columns (%d vs %d)", df.NumCols(), other.NumCols())
	}

	result := New()
	alloc := memory.DefaultAllocator

	for i := 0; i < df.NumCols(); i++ {
		col1, _ := df.Col(i)
		col2, _ := other.Col(i)

		switch c1 := col1.(type) {
		case *series.Int64Series:
			c2 := col2.(*series.Int64Series)
			values := make([]int64, c1.Len()+c2.Len())
			valid := make([]bool, c1.Len()+c2.Len())
			for j := 0; j < c1.Len(); j++ {
				values[j] = c1.Value(j)
				valid[j] = !c1.IsNull(j)
			}
			for j := 0; j < c2.Len(); j++ {
				values[c1.Len()+j] = c2.Value(j)
				valid[c1.Len()+j] = !c2.IsNull(j)
			}
			result.AddSeries(series.NewInt64Series(c1.Name(), alloc, values, valid))

		case *series.Float64Series:
			c2 := col2.(*series.Float64Series)
			values := make([]float64, c1.Len()+c2.Len())
			valid := make([]bool, c1.Len()+c2.Len())
			for j := 0; j < c1.Len(); j++ {
				values[j] = c1.Value(j)
				valid[j] = !c1.IsNull(j)
			}
			for j := 0; j < c2.Len(); j++ {
				values[c1.Len()+j] = c2.Value(j)
				valid[c1.Len()+j] = !c2.IsNull(j)
			}
			result.AddSeries(series.NewFloat64Series(c1.Name(), alloc, values, valid))

		case *series.StringSeries:
			c2 := col2.(*series.StringSeries)
			values := make([]string, c1.Len()+c2.Len())
			valid := make([]bool, c1.Len()+c2.Len())
			for j := 0; j < c1.Len(); j++ {
				values[j] = c1.Value(j)
				valid[j] = !c1.IsNull(j)
			}
			for j := 0; j < c2.Len(); j++ {
				values[c1.Len()+j] = c2.Value(j)
				valid[c1.Len()+j] = !c2.IsNull(j)
			}
			result.AddSeries(series.NewStringSeries(c1.Name(), alloc, values, valid))

		case *series.BooleanSeries:
			c2 := col2.(*series.BooleanSeries)
			values := make([]bool, c1.Len()+c2.Len())
			valid := make([]bool, c1.Len()+c2.Len())
			for j := 0; j < c1.Len(); j++ {
				values[j] = c1.Value(j)
				valid[j] = !c1.IsNull(j)
			}
			for j := 0; j < c2.Len(); j++ {
				values[c1.Len()+j] = c2.Value(j)
				valid[c1.Len()+j] = !c2.IsNull(j)
			}
			result.AddSeries(series.NewBooleanSeries(c1.Name(), alloc, values, valid))
		}
	}

	return result, nil
}

func (df *DataFrame) Union(other *DataFrame) (*DataFrame, error) {
	return df.Concat(other)
}

func (df *DataFrame) Pivot(index, column, value string) (*DataFrame, error) {
	columnCol, err := df.ColByName(column)
	if err != nil {
		return nil, err
	}
	valueCol, err := df.ColByName(value)
	if err != nil {
		return nil, err
	}

	uniqueIndex := df.UniqueValues(index)
	uniqueColumns := df.UniqueValues(column)

	result := New()
	alloc := memory.DefaultAllocator

	result.AddSeries(series.NewStringSeries(index, alloc, uniqueIndex, nil))

	indexToRow := make(map[string]int)
	for i, idxVal := range uniqueIndex {
		indexToRow[idxVal] = i
	}

	for _, colVal := range uniqueColumns {
		values := make([]string, len(uniqueIndex))
		for i := 0; i < len(values); i++ {
			values[i] = ""
		}

		for i := 0; i < df.NumRows(); i++ {
			colValAt := ""
			switch c := columnCol.(type) {
			case *series.StringSeries:
				colValAt = c.Value(i)
			case *series.Int64Series:
				colValAt = fmt.Sprintf("%d", c.Value(i))
			case *series.Float64Series:
				colValAt = fmt.Sprintf("%f", c.Value(i))
			}
			if colValAt == colVal {
				indexVal := ""
				switch idxCol := df.columns[0].(type) {
				case *series.StringSeries:
					indexVal = idxCol.Value(i)
				case *series.Int64Series:
					indexVal = fmt.Sprintf("%d", idxCol.Value(i))
				case *series.Float64Series:
					indexVal = fmt.Sprintf("%f", idxCol.Value(i))
				}
				if rowIdx, ok := indexToRow[indexVal]; ok {
					valAt := ""
					switch v := valueCol.(type) {
					case *series.StringSeries:
						valAt = v.Value(i)
					case *series.Int64Series:
						valAt = fmt.Sprintf("%d", v.Value(i))
					case *series.Float64Series:
						valAt = fmt.Sprintf("%f", v.Value(i))
					case *series.BooleanSeries:
						valAt = fmt.Sprintf("%t", v.Value(i))
					}
					values[rowIdx] = valAt
				}
			}
		}
		result.AddSeries(series.NewStringSeries(colVal, alloc, values, nil))
	}

	return result, nil
}

func (df *DataFrame) Melt(idVars []string, valueVars []string) (*DataFrame, error) {
	result := New()
	alloc := memory.DefaultAllocator

	for _, idVar := range idVars {
		col, err := df.ColByName(idVar)
		if err != nil {
			return nil, err
		}

		var newVals []string
		for rep := 0; rep < len(valueVars); rep++ {
			for i := 0; i < df.NumRows(); i++ {
				if col.IsNull(i) {
					newVals = append(newVals, "")
				} else {
					switch c := col.(type) {
					case *series.StringSeries:
						newVals = append(newVals, c.Value(i))
					case *series.Int64Series:
						newVals = append(newVals, fmt.Sprintf("%d", c.Value(i)))
					case *series.Float64Series:
						newVals = append(newVals, fmt.Sprintf("%f", c.Value(i)))
					case *series.BooleanSeries:
						newVals = append(newVals, fmt.Sprintf("%t", c.Value(i)))
					}
				}
			}
		}
		result.AddSeries(series.NewStringSeries(idVar, alloc, newVals, nil))
	}

	var allValues []string
	var allVariables []string
	for _, valVar := range valueVars {
		col, err := df.ColByName(valVar)
		if err != nil {
			return nil, err
		}
		for i := 0; i < df.NumRows(); i++ {
			if col.IsNull(i) {
				allValues = append(allValues, "")
			} else {
				var val string
				switch c := col.(type) {
				case *series.StringSeries:
					val = c.Value(i)
				case *series.Int64Series:
					val = fmt.Sprintf("%d", c.Value(i))
				case *series.Float64Series:
					val = fmt.Sprintf("%f", c.Value(i))
				case *series.BooleanSeries:
					val = fmt.Sprintf("%t", c.Value(i))
				}
				allValues = append(allValues, val)
			}
			allVariables = append(allVariables, valVar)
		}
	}

	result.AddSeries(series.NewStringSeries("variable", alloc, allVariables, nil))
	result.AddSeries(series.NewStringSeries("value", alloc, allValues, nil))

	return result, nil
}

func (df *DataFrame) UniqueValues(colName string) []string {
	col, err := df.ColByName(colName)
	if err != nil {
		return nil
	}

	seen := make(map[string]bool)
	var unique []string

	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			continue
		}
		var val string
		switch c := col.(type) {
		case *series.StringSeries:
			val = c.Value(i)
		case *series.Int64Series:
			val = fmt.Sprintf("%d", c.Value(i))
		case *series.Float64Series:
			val = fmt.Sprintf("%f", c.Value(i))
		case *series.BooleanSeries:
			val = fmt.Sprintf("%t", c.Value(i))
		}
		if !seen[val] {
			seen[val] = true
			unique = append(unique, val)
		}
	}

	return unique
}

func (df *DataFrame) Columns() []string {
	cols := make([]string, df.NumCols())
	for i := 0; i < df.NumCols(); i++ {
		col, _ := df.Col(i)
		cols[i] = col.Name()
	}
	return cols
}

func (df *DataFrame) Dtypes() []string {
	dtypes := make([]string, df.NumCols())
	for i := 0; i < df.NumCols(); i++ {
		col, _ := df.Col(i)
		dtypes[i] = col.Type().Name()
	}
	return dtypes
}

func (df *DataFrame) Shape() (int, int) {
	return df.NumRows(), df.NumCols()
}

func (df *DataFrame) IsEmpty() bool {
	return df.NumRows() == 0
}

func (df *DataFrame) Rename(columns map[string]string) *DataFrame {
	result := New()
	for i := 0; i < df.NumCols(); i++ {
		col, _ := df.Col(i)
		newName := columns[col.Name()]
		if newName == "" {
			newName = col.Name()
		}

		switch typedCol := col.(type) {
		case *series.Int64Series:
			var values []int64
			var valid []bool
			for j := 0; j < typedCol.Len(); j++ {
				values = append(values, typedCol.Value(j))
				valid = append(valid, !typedCol.IsNull(j))
			}
			result.AddSeries(series.NewInt64Series(newName, memory.DefaultAllocator, values, valid))

		case *series.Float64Series:
			var values []float64
			var valid []bool
			for j := 0; j < typedCol.Len(); j++ {
				values = append(values, typedCol.Value(j))
				valid = append(valid, !typedCol.IsNull(j))
			}
			result.AddSeries(series.NewFloat64Series(newName, memory.DefaultAllocator, values, valid))

		case *series.StringSeries:
			var values []string
			var valid []bool
			for j := 0; j < typedCol.Len(); j++ {
				values = append(values, typedCol.Value(j))
				valid = append(valid, !typedCol.IsNull(j))
			}
			result.AddSeries(series.NewStringSeries(newName, memory.DefaultAllocator, values, valid))

		case *series.BooleanSeries:
			var values []bool
			var valid []bool
			for j := 0; j < typedCol.Len(); j++ {
				values = append(values, typedCol.Value(j))
				valid = append(valid, !typedCol.IsNull(j))
			}
			result.AddSeries(series.NewBooleanSeries(newName, memory.DefaultAllocator, values, valid))
		}
	}
	return result
}

func (df *DataFrame) VStack(other *DataFrame) (*DataFrame, error) {
	if df.NumCols() != other.NumCols() {
		return nil, fmt.Errorf("cannot vstack: different number of columns (%d vs %d)", df.NumCols(), other.NumCols())
	}

	result := New()
	alloc := memory.DefaultAllocator

	for i := 0; i < df.NumCols(); i++ {
		col1, _ := df.Col(i)
		col2, _ := other.Col(i)

		switch c1 := col1.(type) {
		case *series.Int64Series:
			c2 := col2.(*series.Int64Series)
			values := make([]int64, c1.Len()+c2.Len())
			valid := make([]bool, c1.Len()+c2.Len())
			for j := 0; j < c1.Len(); j++ {
				values[j] = c1.Value(j)
				valid[j] = !c1.IsNull(j)
			}
			for j := 0; j < c2.Len(); j++ {
				values[c1.Len()+j] = c2.Value(j)
				valid[c1.Len()+j] = !c2.IsNull(j)
			}
			result.AddSeries(series.NewInt64Series(c1.Name(), alloc, values, valid))

		case *series.Float64Series:
			c2 := col2.(*series.Float64Series)
			values := make([]float64, c1.Len()+c2.Len())
			valid := make([]bool, c1.Len()+c2.Len())
			for j := 0; j < c1.Len(); j++ {
				values[j] = c1.Value(j)
				valid[j] = !c1.IsNull(j)
			}
			for j := 0; j < c2.Len(); j++ {
				values[c1.Len()+j] = c2.Value(j)
				valid[c1.Len()+j] = !c2.IsNull(j)
			}
			result.AddSeries(series.NewFloat64Series(c1.Name(), alloc, values, valid))

		case *series.StringSeries:
			c2 := col2.(*series.StringSeries)
			values := make([]string, c1.Len()+c2.Len())
			valid := make([]bool, c1.Len()+c2.Len())
			for j := 0; j < c1.Len(); j++ {
				values[j] = c1.Value(j)
				valid[j] = !c1.IsNull(j)
			}
			for j := 0; j < c2.Len(); j++ {
				values[c1.Len()+j] = c2.Value(j)
				valid[c1.Len()+j] = !c2.IsNull(j)
			}
			result.AddSeries(series.NewStringSeries(c1.Name(), alloc, values, valid))

		case *series.BooleanSeries:
			c2 := col2.(*series.BooleanSeries)
			values := make([]bool, c1.Len()+c2.Len())
			valid := make([]bool, c1.Len()+c2.Len())
			for j := 0; j < c1.Len(); j++ {
				values[j] = c1.Value(j)
				valid[j] = !c1.IsNull(j)
			}
			for j := 0; j < c2.Len(); j++ {
				values[c1.Len()+j] = c2.Value(j)
				valid[c1.Len()+j] = !c2.IsNull(j)
			}
			result.AddSeries(series.NewBooleanSeries(c1.Name(), alloc, values, valid))
		}
	}

	return result, nil
}

func (df *DataFrame) Join(other *DataFrame, on string, how JoinType) (*DataFrame, error) {
	switch how {
	case Inner:
		return innerJoin(df, other, on)
	case Left:
		return leftJoin(df, other, on)
	case Right:
		return rightJoin(df, other, on)
	case Outer:
		return outerJoin(df, other, on)
	case Cross:
		return crossJoin(df, other)
	default:
		return nil, fmt.Errorf("unsupported join type")
	}
}

func innerJoin(left, right *DataFrame, on string) (*DataFrame, error) {
	leftKeyCol, err := left.ColByName(on)
	if err != nil {
		return nil, err
	}
	rightKeyCol, err := right.ColByName(on)
	if err != nil {
		return nil, err
	}

	if leftStrCol, ok := leftKeyCol.(*series.StringSeries); ok {
		if rightStrCol, ok := rightKeyCol.(*series.StringSeries); ok {
			return innerJoinString(left, right, on, leftStrCol, rightStrCol)
		}
	}

	if leftIntCol, ok := leftKeyCol.(*series.Int64Series); ok {
		if rightIntCol, ok := rightKeyCol.(*series.Int64Series); ok {
			return innerJoinInt(left, right, on, leftIntCol, rightIntCol)
		}
	}

	if leftFloatCol, ok := leftKeyCol.(*series.Float64Series); ok {
		if rightFloatCol, ok := rightKeyCol.(*series.Float64Series); ok {
			return innerJoinFloat(left, right, on, leftFloatCol, rightFloatCol)
		}
	}

	return nil, fmt.Errorf("join key must be the same type in both dataframes")
}

func innerJoinString(left, right *DataFrame, on string, leftCol, rightCol *series.StringSeries) (*DataFrame, error) {
	rightMap := make(map[string][]int)
	for i := 0; i < rightCol.Len(); i++ {
		if !rightCol.IsNull(i) {
			rightMap[rightCol.Value(i)] = append(rightMap[rightCol.Value(i)], i)
		}
	}

	var leftIdx, rightIdx []int
	for i := 0; i < leftCol.Len(); i++ {
		if leftCol.IsNull(i) {
			continue
		}
		val := leftCol.Value(i)
		if rIdxs, exists := rightMap[val]; exists {
			for _, rIdx := range rIdxs {
				leftIdx = append(leftIdx, i)
				rightIdx = append(rightIdx, rIdx)
			}
		}
	}
	return buildJoinResult(left, right, on, leftIdx, rightIdx)
}

func innerJoinInt(left, right *DataFrame, on string, leftCol, rightCol *series.Int64Series) (*DataFrame, error) {
	rightMap := make(map[int64][]int)
	for i := 0; i < rightCol.Len(); i++ {
		if !rightCol.IsNull(i) {
			rightMap[rightCol.Value(i)] = append(rightMap[rightCol.Value(i)], i)
		}
	}

	var leftIdx, rightIdx []int
	for i := 0; i < leftCol.Len(); i++ {
		if leftCol.IsNull(i) {
			continue
		}
		val := leftCol.Value(i)
		if rIdxs, exists := rightMap[val]; exists {
			for _, rIdx := range rIdxs {
				leftIdx = append(leftIdx, i)
				rightIdx = append(rightIdx, rIdx)
			}
		}
	}
	return buildJoinResult(left, right, on, leftIdx, rightIdx)
}

func innerJoinFloat(left, right *DataFrame, on string, leftCol, rightCol *series.Float64Series) (*DataFrame, error) {
	rightMap := make(map[float64][]int)
	for i := 0; i < rightCol.Len(); i++ {
		if !rightCol.IsNull(i) {
			rightMap[rightCol.Value(i)] = append(rightMap[rightCol.Value(i)], i)
		}
	}

	var leftIdx, rightIdx []int
	for i := 0; i < leftCol.Len(); i++ {
		if leftCol.IsNull(i) {
			continue
		}
		val := leftCol.Value(i)
		if rIdxs, exists := rightMap[val]; exists {
			for _, rIdx := range rIdxs {
				leftIdx = append(leftIdx, i)
				rightIdx = append(rightIdx, rIdx)
			}
		}
	}
	return buildJoinResult(left, right, on, leftIdx, rightIdx)
}

func leftJoin(left, right *DataFrame, on string) (*DataFrame, error) {
	leftKeyCol, _ := left.ColByName(on)
	rightKeyCol, _ := right.ColByName(on)
	leftStrCol := leftKeyCol.(*series.StringSeries)
	rightStrCol := rightKeyCol.(*series.StringSeries)

	rightMap := make(map[string][]int)
	for i := 0; i < rightStrCol.Len(); i++ {
		if !rightStrCol.IsNull(i) {
			rightMap[rightStrCol.Value(i)] = append(rightMap[rightStrCol.Value(i)], i)
		}
	}

	var leftIdx, rightIdx []int
	for i := 0; i < leftStrCol.Len(); i++ {
		if leftStrCol.IsNull(i) {
			leftIdx = append(leftIdx, i)
			rightIdx = append(rightIdx, -1)
			continue
		}
		val := leftStrCol.Value(i)
		if rIdxs, exists := rightMap[val]; exists {
			for _, rIdx := range rIdxs {
				leftIdx = append(leftIdx, i)
				rightIdx = append(rightIdx, rIdx)
			}
		} else {
			leftIdx = append(leftIdx, i)
			rightIdx = append(rightIdx, -1)
		}
	}
	return buildJoinResultWithNulls(left, right, on, leftIdx, rightIdx)
}

func rightJoin(left, right *DataFrame, on string) (*DataFrame, error) {
	leftKeyCol, _ := left.ColByName(on)
	rightKeyCol, _ := right.ColByName(on)
	leftStrCol := leftKeyCol.(*series.StringSeries)
	rightStrCol := rightKeyCol.(*series.StringSeries)

	leftMap := make(map[string][]int)
	for i := 0; i < leftStrCol.Len(); i++ {
		if !leftStrCol.IsNull(i) {
			leftMap[leftStrCol.Value(i)] = append(leftMap[leftStrCol.Value(i)], i)
		}
	}

	var leftIdx, rightIdx []int
	for i := 0; i < rightStrCol.Len(); i++ {
		if rightStrCol.IsNull(i) {
			leftIdx = append(leftIdx, -1)
			rightIdx = append(rightIdx, i)
			continue
		}
		val := rightStrCol.Value(i)
		if lIdxs, exists := leftMap[val]; exists {
			for _, lIdx := range lIdxs {
				leftIdx = append(leftIdx, lIdx)
				rightIdx = append(rightIdx, i)
			}
		} else {
			leftIdx = append(leftIdx, -1)
			rightIdx = append(rightIdx, i)
		}
	}
	return buildJoinResultWithNulls(left, right, on, leftIdx, rightIdx)
}

func outerJoin(left, right *DataFrame, on string) (*DataFrame, error) {
	leftKeyCol, _ := left.ColByName(on)
	rightKeyCol, _ := right.ColByName(on)
	leftStrCol := leftKeyCol.(*series.StringSeries)
	rightStrCol := rightKeyCol.(*series.StringSeries)

	rightMap := make(map[string][]int)
	for i := 0; i < rightStrCol.Len(); i++ {
		if !rightStrCol.IsNull(i) {
			rightMap[rightStrCol.Value(i)] = append(rightMap[rightStrCol.Value(i)], i)
		}
	}

	leftMap := make(map[string][]int)
	for i := 0; i < leftStrCol.Len(); i++ {
		if !leftStrCol.IsNull(i) {
			leftMap[leftStrCol.Value(i)] = append(leftMap[leftStrCol.Value(i)], i)
		}
	}

	var leftIdx, rightIdx []int
	rightMatched := make(map[int]bool)

	for i := 0; i < leftStrCol.Len(); i++ {
		if leftStrCol.IsNull(i) {
			leftIdx = append(leftIdx, i)
			rightIdx = append(rightIdx, -1)
			continue
		}
		val := leftStrCol.Value(i)
		if rIdxs, exists := rightMap[val]; exists {
			for _, rIdx := range rIdxs {
				leftIdx = append(leftIdx, i)
				rightIdx = append(rightIdx, rIdx)
				rightMatched[rIdx] = true
			}
		} else {
			leftIdx = append(leftIdx, i)
			rightIdx = append(rightIdx, -1)
		}
	}

	for i := 0; i < rightStrCol.Len(); i++ {
		if !rightMatched[i] {
			leftIdx = append(leftIdx, -1)
			rightIdx = append(rightIdx, i)
		}
	}
	return buildJoinResultWithNulls(left, right, on, leftIdx, rightIdx)
}

func crossJoin(left, right *DataFrame) (*DataFrame, error) {
	var leftIdx, rightIdx []int
	for i := 0; i < left.NumRows(); i++ {
		for j := 0; j < right.NumRows(); j++ {
			leftIdx = append(leftIdx, i)
			rightIdx = append(rightIdx, j)
		}
	}
	return buildJoinResult(left, right, "", leftIdx, rightIdx)
}

func buildJoinResult(left, right *DataFrame, on string, leftIdx, rightIdx []int) (*DataFrame, error) {
	result := New()
	alloc := memory.DefaultAllocator

	for i := 0; i < left.NumCols(); i++ {
		col, _ := left.Col(i)
		result.AddSeries(copySeriesByIndices(col, leftIdx, alloc))
	}

	for i := 0; i < right.NumCols(); i++ {
		col, _ := right.Col(i)
		if on != "" && col.Name() == on {
			continue
		}
		result.AddSeries(copySeriesByIndices(col, rightIdx, alloc))
	}
	return result, nil
}

func buildJoinResultWithNulls(left, right *DataFrame, on string, leftIdx, rightIdx []int) (*DataFrame, error) {
	result := New()
	alloc := memory.DefaultAllocator

	for i := 0; i < left.NumCols(); i++ {
		col, _ := left.Col(i)
		valid := make([]bool, len(leftIdx))
		for j, idx := range leftIdx {
			valid[j] = idx >= 0
		}
		result.AddSeries(copySeriesByIndicesWithNulls(col, leftIdx, valid, alloc))
	}

	for i := 0; i < right.NumCols(); i++ {
		col, _ := right.Col(i)
		if on != "" && col.Name() == on {
			continue
		}
		valid := make([]bool, len(rightIdx))
		for j, idx := range rightIdx {
			valid[j] = idx >= 0
		}
		result.AddSeries(copySeriesByIndicesWithNulls(col, rightIdx, valid, alloc))
	}
	return result, nil
}

func copySeriesByIndices(col series.Series, indices []int, alloc memory.Allocator) series.Series {
	switch c := col.(type) {
	case *series.StringSeries:
		var v []string
		for _, idx := range indices {
			v = append(v, c.Value(idx))
		}
		return series.NewStringSeries(c.Name(), alloc, v, nil)
	case *series.Float64Series:
		var v []float64
		for _, idx := range indices {
			v = append(v, c.Value(idx))
		}
		return series.NewFloat64Series(c.Name(), alloc, v, nil)
	case *series.Int64Series:
		var v []int64
		for _, idx := range indices {
			v = append(v, c.Value(idx))
		}
		return series.NewInt64Series(c.Name(), alloc, v, nil)
	case *series.BooleanSeries:
		var v []bool
		for _, idx := range indices {
			v = append(v, c.Value(idx))
		}
		return series.NewBooleanSeries(c.Name(), alloc, v, nil)
	}
	return nil
}

func copySeriesByIndicesWithNulls(col series.Series, indices []int, valid []bool, alloc memory.Allocator) series.Series {
	switch c := col.(type) {
	case *series.StringSeries:
		var v []string
		for i, idx := range indices {
			if valid[i] && idx >= 0 {
				v = append(v, c.Value(idx))
			} else {
				v = append(v, "")
			}
		}
		return series.NewStringSeries(c.Name(), alloc, v, valid)
	case *series.Float64Series:
		var v []float64
		for i, idx := range indices {
			if valid[i] && idx >= 0 {
				v = append(v, c.Value(idx))
			} else {
				v = append(v, 0)
			}
		}
		return series.NewFloat64Series(c.Name(), alloc, v, valid)
	case *series.Int64Series:
		var v []int64
		for i, idx := range indices {
			if valid[i] && idx >= 0 {
				v = append(v, c.Value(idx))
			} else {
				v = append(v, 0)
			}
		}
		return series.NewInt64Series(c.Name(), alloc, v, valid)
	case *series.BooleanSeries:
		var v []bool
		for i, idx := range indices {
			if valid[i] && idx >= 0 {
				v = append(v, c.Value(idx))
			} else {
				v = append(v, false)
			}
		}
		return series.NewBooleanSeries(c.Name(), alloc, v, valid)
	}
	return nil
}
