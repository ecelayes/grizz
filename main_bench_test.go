package main

import (
	"testing"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/engine"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/internal/memory"
	"github.com/ecelayes/grizz/series"
)

func BenchmarkDataFrameFilter(b *testing.B) {
	df := createTestDataFrame(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = engine.Execute(df.Lazy().Filter(expr.Col("Age").Gt(expr.Lit(25))).Plan())
	}
}

func BenchmarkDataFrameSelect(b *testing.B) {
	df := createTestDataFrame(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = engine.Execute(df.Lazy().Select(expr.Col("Name"), expr.Col("Age")).Plan())
	}
}

func BenchmarkDataFrameJoin(b *testing.B) {
	df1 := createTestDataFrame(5000)
	df2 := createDepartmentsDataFrame(100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lf := df1.Lazy().Join(df2.Lazy(), "Department", dataframe.Inner)
		_, _ = engine.Execute(lf.Plan())
	}
}

func BenchmarkDataFrameGroupBy(b *testing.B) {
	df := createTestDataFrame(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = engine.Execute(df.Lazy().GroupBy("Department").Agg(expr.Sum("Age")).Plan())
	}
}

func createTestDataFrame(n int) *dataframe.DataFrame {
	names := []string{"Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack"}
	depts := []string{"Engineering", "Marketing", "Sales", "HR", "Finance"}

	namesCol := make([]string, n)
	agesCol := make([]int64, n)
	scoresCol := make([]float64, n)
	deptsCol := make([]string, n)
	actives := make([]bool, n)

	for i := 0; i < n; i++ {
		namesCol[i] = names[i%len(names)]
		agesCol[i] = int64(20 + i%50)
		scoresCol[i] = float64(50 + i%50)
		deptsCol[i] = depts[i%len(depts)]
		actives[i] = i%2 == 0
	}

	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("Name", memory.DefaultAllocator, namesCol, nil))
	df.AddSeries(series.NewInt64Series("Age", memory.DefaultAllocator, agesCol, nil))
	df.AddSeries(series.NewFloat64Series("Score", memory.DefaultAllocator, scoresCol, nil))
	df.AddSeries(series.NewStringSeries("Department", memory.DefaultAllocator, deptsCol, nil))
	df.AddSeries(series.NewBooleanSeries("Active", memory.DefaultAllocator, actives, nil))

	return df
}

func createDepartmentsDataFrame(n int) *dataframe.DataFrame {
	depts := []string{"Engineering", "Marketing", "Sales", "HR", "Finance"}
	managers := []string{"Zack", "Yara", "Xavier", "Wendy", "Victor"}

	deptsCol := make([]string, n)
	mgrsCol := make([]string, n)
	budgetsCol := make([]int64, n)

	for i := 0; i < n; i++ {
		deptsCol[i] = depts[i%len(depts)]
		mgrsCol[i] = managers[i%len(managers)]
		budgetsCol[i] = int64(500000 + i*1000)
	}

	df := dataframe.New()
	df.AddSeries(series.NewStringSeries("Department", memory.DefaultAllocator, deptsCol, nil))
	df.AddSeries(series.NewStringSeries("Manager", memory.DefaultAllocator, mgrsCol, nil))
	df.AddSeries(series.NewInt64Series("Budget", memory.DefaultAllocator, budgetsCol, nil))

	return df
}
