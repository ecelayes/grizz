package dataframe

import (
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/ecelayes/grizz/series"
)

func (df *DataFrame) Show() {
	fmt.Printf("Shape: (%d rows, %d cols)\n", df.rows, len(df.columns))

	if df.rows == 0 || len(df.columns) == 0 {
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)

	var headers []string
	var types []string
	var separators []string

	for _, col := range df.columns {
		headers = append(headers, col.Name())
		types = append(types, fmt.Sprintf("<%s>", col.Type().Name()))
		separators = append(separators, strings.Repeat("-", len(col.Name())+4))
	}

	fmt.Fprintln(w, strings.Join(headers, "\t|\t"))
	fmt.Fprintln(w, strings.Join(types, "\t|\t"))
	fmt.Fprintln(w, strings.Join(separators, "\t|\t"))

	for r := 0; r < df.rows; r++ {
		var row []string
		for _, col := range df.columns {
			if col.IsNull(r) {
				row = append(row, "null")
				continue
			}
			
			switch c := col.(type) {
			case *series.StringSeries:
				row = append(row, c.Value(r))
			case *series.Int64Series:
				row = append(row, fmt.Sprintf("%d", c.Value(r)))
			case *series.Float64Series:
				row = append(row, fmt.Sprintf("%v", c.Value(r)))
			case *series.BooleanSeries:
				row = append(row, fmt.Sprintf("%t", c.Value(r)))
			default:
				row = append(row, "unknown")
			}
		}
		fmt.Fprintln(w, strings.Join(row, "\t|\t"))
	}

	w.Flush()
	fmt.Println()
}
