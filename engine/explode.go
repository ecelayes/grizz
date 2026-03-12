package engine

import (
	"errors"
	"strings"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func applyExplode(df *dataframe.DataFrame, ee expr.ExplodeExpr, alloc memory.Allocator) (series.Series, error) {
	colExpr, ok := ee.Expr.(expr.Column)
	if !ok {
		return nil, errors.New("Explode only supports column expressions")
	}

	col, err := df.ColByName(colExpr.Name)
	if err != nil {
		return nil, err
	}

	if strCol, ok := col.(*series.StringSeries); ok {
		var allValues []string
		for i := 0; i < strCol.Len(); i++ {
			if strCol.IsNull(i) {
				continue
			}
			parts := strings.Split(strCol.Value(i), ee.Delimiter)
			allValues = append(allValues, parts...)
		}
		return series.NewStringSeries(colExpr.Name+"_exploded", alloc, allValues, nil), nil
	}

	return nil, errors.New("Explode only supports string columns")
}
