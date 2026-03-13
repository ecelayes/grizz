package expr

import "fmt"

type ContainsExpr struct {
	Expr   Expr
	Substr Expr
}

func (e ContainsExpr) String() string {
	return fmt.Sprintf("Contains(%s, %s)", e.Expr.String(), e.Substr.String())
}

type ReplaceExpr struct {
	Expr Expr
	Old  Expr
	New  Expr
}

func (e ReplaceExpr) String() string {
	return fmt.Sprintf("Replace(%s, %s, %s)", e.Expr.String(), e.Old.String(), e.New.String())
}

type UpperExpr struct {
	Expr Expr
}

func (e UpperExpr) String() string {
	return fmt.Sprintf("Upper(%s)", e.Expr.String())
}

type LowerExpr struct {
	Expr Expr
}

func (e LowerExpr) String() string {
	return fmt.Sprintf("Lower(%s)", e.Expr.String())
}

type StripExpr struct {
	Expr Expr
}

func (e StripExpr) String() string {
	return fmt.Sprintf("Strip(%s)", e.Expr.String())
}

func Contains(expr Expr, substr Expr) ContainsExpr {
	return ContainsExpr{Expr: expr, Substr: substr}
}

func Replace(expr Expr, old Expr, new Expr) ReplaceExpr {
	return ReplaceExpr{Expr: expr, Old: old, New: new}
}

func Upper(expr Expr) UpperExpr {
	return UpperExpr{Expr: expr}
}

func Lower(expr Expr) LowerExpr {
	return LowerExpr{Expr: expr}
}

func Strip(expr Expr) StripExpr {
	return StripExpr{Expr: expr}
}

type LengthExpr struct {
	Expr Expr
}

func (e LengthExpr) String() string {
	return fmt.Sprintf("Length(%s)", e.Expr.String())
}

func Length(expr Expr) LengthExpr {
	return LengthExpr{Expr: expr}
}

type SplitExpr struct {
	Expr  Expr
	Delim Expr
}

func (e SplitExpr) String() string {
	return fmt.Sprintf("Split(%s, %s)", e.Expr.String(), e.Delim.String())
}

func Split(expr Expr, delim Expr) SplitExpr {
	return SplitExpr{Expr: expr, Delim: delim}
}

type TrimExpr struct {
	Expr Expr
}

func (e TrimExpr) String() string {
	return fmt.Sprintf("Trim(%s)", e.Expr.String())
}

func Trim(expr Expr) TrimExpr {
	return TrimExpr{Expr: expr}
}

type LPadExpr struct {
	Expr   Expr
	Length Expr
}

func (e LPadExpr) String() string {
	return fmt.Sprintf("LPad(%s, %s)", e.Expr.String(), e.Length.String())
}

func LPad(expr Expr, length Expr) LPadExpr {
	return LPadExpr{Expr: expr, Length: length}
}

type RPadExpr struct {
	Expr   Expr
	Length Expr
}

func (e RPadExpr) String() string {
	return fmt.Sprintf("RPad(%s, %s)", e.Expr.String(), e.Length.String())
}

func RPad(expr Expr, length Expr) RPadExpr {
	return RPadExpr{Expr: expr, Length: length}
}

type ContainsRegexExpr struct {
	Expr    Expr
	Pattern Expr
}

func (e ContainsRegexExpr) String() string {
	return fmt.Sprintf("ContainsRegex(%s, %s)", e.Expr.String(), e.Pattern.String())
}

func ContainsRegex(expr Expr, pattern Expr) ContainsRegexExpr {
	return ContainsRegexExpr{Expr: expr, Pattern: pattern}
}

type ExtractExpr struct {
	Expr    Expr
	Pattern Expr
}

func (e ExtractExpr) String() string {
	return fmt.Sprintf("Extract(%s, %s)", e.Expr.String(), e.Pattern.String())
}

func (e ExtractExpr) Alias(name string) AliasExpr {
	return AliasExpr{Expr: e, Alias: name}
}

func Extract(expr Expr, pattern Expr) ExtractExpr {
	return ExtractExpr{Expr: expr, Pattern: pattern}
}

type FindExpr struct {
	Expr   Expr
	Substr Expr
}

func (e FindExpr) String() string {
	return fmt.Sprintf("Find(%s, %s)", e.Expr.String(), e.Substr.String())
}

func (e FindExpr) Alias(name string) AliasExpr {
	return AliasExpr{Expr: e, Alias: name}
}

func Find(expr Expr, substr Expr) FindExpr {
	return FindExpr{Expr: expr, Substr: substr}
}

type SliceExpr struct {
	Expr   Expr
	Start  Expr
	Length Expr
}

func (e SliceExpr) String() string {
	return fmt.Sprintf("Slice(%s, %s, %s)", e.Expr.String(), e.Start.String(), e.Length.String())
}

func Slice(expr Expr, start Expr, length Expr) SliceExpr {
	return SliceExpr{Expr: expr, Start: start, Length: length}
}
