package main

import (
	"fmt"
	"log"

	"github.com/ecelayes/grizz/dataframe"
	"github.com/ecelayes/grizz/engine"
	"github.com/ecelayes/grizz/expr"
	"github.com/ecelayes/grizz/io/csv"
)

func main() {
	employeesDF, err := csv.Read("data.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer employeesDF.Release()

	departmentsDF, err := csv.Read("departments.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer departmentsDF.Release()

	fmt.Println("--- Employees Data ---")
	employeesDF.Show()

	fmt.Println("--- Departments Data ---")
	departmentsDF.Show()

	employeesLazy := employeesDF.Lazy().Filter(expr.Col("Age").Gt(expr.Lit(25)))
	departmentsLazy := departmentsDF.Lazy()

	lf := employeesLazy.Join(departmentsLazy, "Department", dataframe.Inner).
		Select(
			expr.Col("Name"),
			expr.Col("Department"),
			expr.Col("Manager"),
			expr.Col("Budget"),
		)

	fmt.Println("--- Logical Plan ---")
	fmt.Println(lf.Explain())

	resultDF, err := engine.Execute(lf.Plan())
	if err != nil {
		log.Fatal(err)
	}
	defer resultDF.Release()

	fmt.Println("--- Joined Result ---")
	resultDF.Show()

	err = csv.Write(resultDF, "output.csv")
	if err != nil {
		log.Fatal(err)
	}
}
