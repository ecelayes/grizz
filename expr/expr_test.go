package expr

import (
	"testing"

	grizzarrows "github.com/ecelayes/grizz/internal/arrow"
)

func TestColumnString(t *testing.T) {
	col := Column{Name: "Age"}
	if col.String() != "Age" {
		t.Errorf("Expected Age, got %s", col.String())
	}
}

func TestColumnEq(t *testing.T) {
	result := Column{Name: "Age"}.Eq(Lit(25))
	expected := "(Age == 25)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestColumnGt(t *testing.T) {
	result := Column{Name: "Score"}.Gt(Lit(50))
	expected := "(Score > 50)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestColumnLt(t *testing.T) {
	result := Column{Name: "Age"}.Lt(Lit(30))
	expected := "(Age < 30)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestColumnNe(t *testing.T) {
	result := Column{Name: "Name"}.Ne(Lit("test"))
	expected := `(Name != "test")`
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestColumnLtEq(t *testing.T) {
	result := Column{Name: "Age"}.LtEq(Lit(30))
	expected := "(Age <= 30)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestColumnGtEq(t *testing.T) {
	result := Column{Name: "Age"}.GtEq(Lit(18))
	expected := "(Age >= 18)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestAnd(t *testing.T) {
	left := Column{Name: "Age"}.Gt(Lit(20))
	right := Column{Name: "Score"}.Gt(Lit(50))
	result := And(left, right)
	expected := "((Age > 20) And (Score > 50))"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestOr(t *testing.T) {
	left := Column{Name: "A"}.Eq(Lit(1))
	right := Column{Name: "B"}.Eq(Lit(2))
	result := Or(left, right)
	expected := "((A == 1) Or (B == 2))"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestNot(t *testing.T) {
	cond := Column{Name: "Active"}.Eq(Lit(true))
	result := Not(cond)
	expected := "Not((Active == true))"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestAlias(t *testing.T) {
	result := Alias(Column{Name: "age"}, "Age")
	expected := "age AS Age"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestArithmeticAdd(t *testing.T) {
	result := Column{Name: "a"}.Add(Lit(10))
	expected := "(a + 10)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestArithmeticMul(t *testing.T) {
	result := Column{Name: "price"}.Mul(Lit(1.1))
	expected := "(price * 1.1)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestArithmeticSub(t *testing.T) {
	result := Column{Name: "a"}.Sub(Lit(5))
	expected := "(a - 5)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestArithmeticDiv(t *testing.T) {
	result := Column{Name: "a"}.Div(Lit(2))
	expected := "(a / 2)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestStringContains(t *testing.T) {
	result := Contains(Column{Name: "name"}, Lit("test"))
	expected := `Contains(name, "test")`
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestStringUpper(t *testing.T) {
	result := Upper(Column{Name: "name"})
	expected := "Upper(name)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestStringLower(t *testing.T) {
	result := Lower(Column{Name: "name"})
	expected := "Lower(name)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestStringReplace(t *testing.T) {
	result := Replace(Column{Name: "name"}, Lit("old"), Lit("new"))
	expected := `Replace(name, "old", "new")`
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestStringStrip(t *testing.T) {
	result := Strip(Column{Name: "name"})
	expected := "Strip(name)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestIsNull(t *testing.T) {
	result := IsNull(Column{Name: "age"})
	expected := "IsNull(age)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestIsNotNull(t *testing.T) {
	result := IsNotNull(Column{Name: "age"})
	expected := "IsNotNull(age)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestFillNull(t *testing.T) {
	result := FillNull(Column{Name: "score"}, Lit(0))
	expected := "FillNull(score, 0)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestCoalesce(t *testing.T) {
	result := Coalesce(Column{Name: "a"}, Column{Name: "b"})
	expected := "Coalesce(a, b)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestWhenThen(t *testing.T) {
	result := When(Column{Name: "age"}.Gt(Lit(18))).Then(Lit("adult"))
	expected := `Then("adult")`
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestWhenThenOtherwise(t *testing.T) {
	result := When(Column{Name: "age"}.Gt(Lit(18))).Then(Lit("adult")).Otherwise(Lit("minor"))
	expected := `When((age > 18)).Then("adult").Otherwise("minor")`
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestAggregationSum(t *testing.T) {
	result := Sum("score")
	expected := "Sum(score)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestAggregationMean(t *testing.T) {
	result := Mean("score")
	expected := "Mean(score)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestAggregationMin(t *testing.T) {
	result := Min("score")
	expected := "Min(score)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestAggregationMax(t *testing.T) {
	result := Max("score")
	expected := "Max(score)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestAggregationStd(t *testing.T) {
	result := Std("score")
	expected := "Std(score)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestAggregationVar(t *testing.T) {
	result := Var("score")
	expected := "Var(score)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestAggregationMedian(t *testing.T) {
	result := Median("score")
	expected := "Median(score)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestAggregationQuantile(t *testing.T) {
	result := Quantile("score", 0.75)
	expected := "Quantile(score, 0.75)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestCol(t *testing.T) {
	result := Col("age")
	expected := "age"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestLitInt(t *testing.T) {
	result := Lit(42)
	expected := "42"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestLitString(t *testing.T) {
	result := Lit("hello")
	expected := `"hello"`
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestLitFloat(t *testing.T) {
	result := Lit(3.14)
	expected := "3.14"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestCount(t *testing.T) {
	result := Count("Age")
	expected := "Count(Age)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestCastString(t *testing.T) {
	result := Cast(Col("Age"), grizzarrows.String)
	expected := "Cast(Age, utf8)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestCastInt(t *testing.T) {
	result := Cast(Col("Score"), grizzarrows.Int64)
	expected := "Cast(Score, int64)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestWhenThenWithCondition(t *testing.T) {
	result := When(Col("Age").Gt(Lit(18))).Then(Lit("adult"))
	expected := `Then("adult")`
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestWhenThenOtherwiseString(t *testing.T) {
	result := When(Col("Age").Gt(Lit(18))).Then(Lit("adult")).Otherwise(Lit("minor"))
	expected := `When((Age > 18)).Then("adult").Otherwise("minor")`
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestRowNumber(t *testing.T) {
	result := RowNumber()
	expected := "row_number()"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestRank(t *testing.T) {
	result := Rank()
	expected := "rank()"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestLead(t *testing.T) {
	result := Lead(Col("value"), 1)
	expected := "lead(value, 1)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestLag(t *testing.T) {
	result := Lag(Col("value"), 1)
	expected := "lag(value, 1)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestWhenExprString(t *testing.T) {
	result := When(Col("Age").Gt(Lit(18)))
	expected := "When((Age > 18))"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestStringLength(t *testing.T) {
	result := Length(Col("name"))
	expected := "Length(name)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestStringSplit(t *testing.T) {
	result := Split(Col("names"), Lit(","))
	expected := `Split(names, ",")`
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestStringTrim(t *testing.T) {
	result := Trim(Col("name"))
	expected := "Trim(name)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestStringLPad(t *testing.T) {
	result := LPad(Col("name"), Lit(10))
	expected := "LPad(name, 10)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestStringRPad(t *testing.T) {
	result := RPad(Col("name"), Lit(10))
	expected := "RPad(name, 10)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestStringContainsRegex(t *testing.T) {
	result := ContainsRegex(Col("name"), Lit("^test"))
	expected := `ContainsRegex(name, "^test")`
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestStringSlice(t *testing.T) {
	result := Slice(Col("name"), Lit(0), Lit(5))
	expected := "Slice(name, 0, 5)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestFillNullForward(t *testing.T) {
	result := FillNullForward(Col("score"))
	expected := "FillNullForward(score)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestFillNullBackward(t *testing.T) {
	result := FillNullBackward(Col("score"))
	expected := "FillNullBackward(score)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestColumnChainingAdd(t *testing.T) {
	result := Col("a").Add(Lit(10)).Add(Lit(5))
	expected := "((a + 10) + 5)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestColumnChainingMul(t *testing.T) {
	result := Col("a").Mul(Lit(2)).Mul(Lit(3))
	expected := "((a * 2) * 3)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestColumnChainingAlias(t *testing.T) {
	result := Col("a").Add(Lit(10)).Alias("result")
	expected := "(a + 10) AS result"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestColumnAnd(t *testing.T) {
	result := Col("a").Gt(Lit(5)).And(Col("b").Lt(Lit(10)))
	expected := "((a > 5) And (b < 10))"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestColumnOr(t *testing.T) {
	result := Col("a").Eq(Lit(1)).Or(Col("b").Eq(Lit(2)))
	expected := "((a == 1) Or (b == 2))"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestAggregationNUnique(t *testing.T) {
	result := NUnique("age")
	expected := "NUnique(age)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestAggregationFirst(t *testing.T) {
	result := First("name")
	expected := "First(name)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestAggregationLast(t *testing.T) {
	result := Last("value")
	expected := "Last(value)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestAggregationArgMin(t *testing.T) {
	result := ArgMin("score")
	expected := "ArgMin(score)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestAggregationArgMax(t *testing.T) {
	result := ArgMax("temperature")
	expected := "ArgMax(temperature)"
	if result.String() != expected {
		t.Errorf("Expected %s, got %s", expected, result.String())
	}
}

func TestIsInInt(t *testing.T) {
	col := Col("a")
	result := col.IsIn([]any{1, 2, 3})
	if len(result.Values) != 3 {
		t.Error("IsIn should have 3 values")
	}
}

func TestIsInString(t *testing.T) {
	col := Col("name")
	result := col.IsIn([]any{"alice", "bob"})
	if len(result.Values) != 2 {
		t.Error("IsIn should have 2 values")
	}
}

func TestIsInFloat(t *testing.T) {
	col := Col("value")
	result := col.IsIn([]any{1.5, 2.5, 3.5})
	if len(result.Values) != 3 {
		t.Error("IsIn should have 3 values")
	}
}

func TestColumnAddColumn(t *testing.T) {
	colA := Col("a")
	colB := Col("b")
	_ = colA.Add(colB)
}

func TestColumnSubColumn(t *testing.T) {
	colA := Col("a")
	colB := Col("b")
	_ = colA.Sub(colB)
}

func TestColumnMulColumn(t *testing.T) {
	colA := Col("a")
	colB := Col("b")
	_ = colA.Mul(colB)
}

func TestColumnDivColumn(t *testing.T) {
	colA := Col("a")
	colB := Col("b")
	_ = colA.Div(colB)
}
