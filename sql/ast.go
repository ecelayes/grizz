package sql

type TokenType int

const (
	TokenEOF TokenType = iota
	TokenError

	TokenIdentifier
	TokenString
	TokenNumber

	TokenAND
	TokenAS
	TokenASC
	TokenBETWEEN
	TokenBY
	TokenCOUNT
	TokenDESC
	TokenDISTINCT
	TokenFALSE
	TokenFROM
	TokenGROUP
	TokenHAVING
	TokenIN
	TokenIS
	TokenLIMIT
	TokenLIKE
	TokenMAX
	TokenMEAN
	TokenMIN
	TokenNOT
	TokenNULL
	TokenOR
	TokenORDER
	TokenSELECT
	TokenSUM
	TokenTRUE
	TokenWHERE

	TokenEQ
	TokenNE
	TokenLT
	TokenGT
	TokenLTE
	TokenGTE
	TokenComma
	TokenDot
	TokenLParen
	TokenRParen
	TokenStar
	TokenPlus
	TokenMinus
	TokenSlash
)

type Token struct {
	Type   TokenType
	Lexeme string
	Value  any
	Line   int
	Column int
}

func (t TokenType) TokenTypeString() string {
	switch t {
	case TokenEOF:
		return "EOF"
	case TokenError:
		return "ERROR"
	case TokenIdentifier:
		return "IDENTIFIER"
	case TokenString:
		return "STRING"
	case TokenNumber:
		return "NUMBER"
	case TokenAND:
		return "AND"
	case TokenAS:
		return "AS"
	case TokenASC:
		return "ASC"
	case TokenBETWEEN:
		return "BETWEEN"
	case TokenBY:
		return "BY"
	case TokenCOUNT:
		return "COUNT"
	case TokenDESC:
		return "DESC"
	case TokenDISTINCT:
		return "DISTINCT"
	case TokenFALSE:
		return "FALSE"
	case TokenFROM:
		return "FROM"
	case TokenGROUP:
		return "GROUP"
	case TokenHAVING:
		return "HAVING"
	case TokenIN:
		return "IN"
	case TokenIS:
		return "IS"
	case TokenLIMIT:
		return "LIMIT"
	case TokenLIKE:
		return "LIKE"
	case TokenMAX:
		return "MAX"
	case TokenMEAN:
		return "MEAN"
	case TokenMIN:
		return "MIN"
	case TokenNOT:
		return "NOT"
	case TokenNULL:
		return "NULL"
	case TokenOR:
		return "OR"
	case TokenORDER:
		return "ORDER"
	case TokenSELECT:
		return "SELECT"
	case TokenSUM:
		return "SUM"
	case TokenTRUE:
		return "TRUE"
	case TokenWHERE:
		return "WHERE"
	case TokenEQ:
		return "="
	case TokenNE:
		return "!="
	case TokenLT:
		return "<"
	case TokenGT:
		return ">"
	case TokenLTE:
		return "<="
	case TokenGTE:
		return ">="
	case TokenComma:
		return ","
	case TokenDot:
		return "."
	case TokenLParen:
		return "("
	case TokenRParen:
		return ")"
	case TokenStar:
		return "*"
	case TokenPlus:
		return "+"
	case TokenMinus:
		return "-"
	case TokenSlash:
		return "/"
	default:
		return "UNKNOWN"
	}
}

type SQLStatement struct {
	Select  SelectClause
	From    FromClause
	Where   WhereClause
	GroupBy GroupByClause
	Having  HavingClause
	OrderBy OrderByClause
	Limit   LimitClause
}

type SelectClause struct {
	IsDistinct bool
	Columns    []SelectColumn
}

type SelectColumn struct {
	Expr    Expression
	Alias   string
	IsAgg   bool
	AggFunc string
}

type FromClause struct {
	TableName string
}

type WhereClause struct {
	Condition Expression
}

type GroupByClause struct {
	Columns []Expression
}

type HavingClause struct {
	Condition Expression
}

type OrderByClause struct {
	Columns []OrderByColumn
}

type OrderByColumn struct {
	Expr       Expression
	Descending bool
}

type LimitClause struct {
	Count int
}

type Expression interface {
	isExpression()
}

type ColumnRef struct {
	Name string
}

func (c ColumnRef) isExpression() {}

type Literal struct {
	Value any
}

func (l Literal) isExpression() {}

type BinaryExpr struct {
	Left  Expression
	Op    string
	Right Expression
}

func (b BinaryExpr) isExpression() {}

type UnaryExpr struct {
	Op   string
	Expr Expression
}

func (u UnaryExpr) isExpression() {}

type LogicalExpr struct {
	Left  Expression
	Op    string
	Right Expression
}

func (l LogicalExpr) isExpression() {}

type BetweenExpr struct {
	Expr  Expression
	Lower Expression
	Upper Expression
}

func (b BetweenExpr) isExpression() {}

type InExpr struct {
	Expr   Expression
	Values []Expression
}

func (i InExpr) isExpression() {}

type LikeExpr struct {
	Expr    Expression
	Pattern Expression
}

func (l LikeExpr) isExpression() {}

type FunctionCallExpr struct {
	Name string
	Args []Expression
}

func (f FunctionCallExpr) isExpression() {}

type AggExpr struct {
	Func  string
	Expr  Expression
	Alias string
}

func (a AggExpr) isExpression() {}
