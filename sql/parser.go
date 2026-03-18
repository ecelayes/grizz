package sql

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
)

type Parser struct {
	tokens  []Token
	current int
	line    int
	column  int
}

func NewParser(tokens []Token) *Parser {
	return &Parser{
		tokens:  tokens,
		current: 0,
		line:    1,
		column:  1,
	}
}

func (p *Parser) Parse() (*SQLStatement, error) {
	stmt := &SQLStatement{}

	if !p.match(TokenSELECT) {
		return nil, errors.New("expected SELECT keyword")
	}

	if p.match(TokenDISTINCT) {
		stmt.Select.IsDistinct = true
	}

	if err := p.parseSelectColumns(&stmt.Select); err != nil {
		return nil, err
	}

	if !p.match(TokenFROM) {
		return nil, errors.New("expected FROM keyword")
	}

	if p.check(TokenIdentifier) {
		stmt.From.TableName = p.advance().Lexeme
	}

	if p.match(TokenWHERE) {
		cond, err := p.parseExpression()
		if err != nil {
			return nil, fmt.Errorf("failed to parse WHERE clause: %w", err)
		}
		stmt.Where.Condition = cond
	}

	if p.match(TokenGROUP) {
		if !p.match(TokenBY) {
			return nil, errors.New("expected BY after GROUP")
		}
		if err := p.parseGroupBy(&stmt.GroupBy); err != nil {
			return nil, err
		}

		if p.match(TokenHAVING) {
			cond, err := p.parseExpression()
			if err != nil {
				return nil, fmt.Errorf("failed to parse HAVING clause: %w", err)
			}
			stmt.Having.Condition = cond
		}
	}

	if p.match(TokenORDER) {
		if !p.match(TokenBY) {
			return nil, errors.New("expected BY after ORDER")
		}
		if err := p.parseOrderBy(&stmt.OrderBy); err != nil {
			return nil, err
		}
	}

	if p.match(TokenLIMIT) {
		if !p.check(TokenNumber) {
			return nil, errors.New("expected number after LIMIT")
		}
		tok := p.advance()
		switch v := tok.Value.(type) {
		case int:
			stmt.Limit.Count = v
		case int64:
			stmt.Limit.Count = int(v)
		default:
			return nil, errors.New("invalid LIMIT value")
		}
	}

	if p.match(TokenOFFSET) {
		if !p.check(TokenNumber) {
			return nil, errors.New("expected number after OFFSET")
		}
		tok := p.advance()
		switch v := tok.Value.(type) {
		case int:
			stmt.Offset.Count = v
		case int64:
			stmt.Offset.Count = int(v)
		default:
			return nil, errors.New("invalid OFFSET value")
		}
	}

	if !p.isAtEnd() {
		return nil, errors.New("unexpected tokens after query")
	}

	return stmt, nil
}

func (p *Parser) parseSelectColumns(sc *SelectClause) error {
	if p.check(TokenStar) {
		p.advance()
		sc.Columns = []SelectColumn{{Expr: ColumnRef{Name: "*"}}}
		return nil
	}

	for {
		col, err := p.parseSelectColumn()
		if err != nil {
			return err
		}
		sc.Columns = append(sc.Columns, col)

		if !p.match(TokenComma) {
			break
		}
	}
	return nil
}

func (p *Parser) parseSelectColumn() (SelectColumn, error) {
	col := SelectColumn{}

	expr, err := p.parseExpression()
	if err != nil {
		return col, err
	}

	switch e := expr.(type) {
	case ColumnRef:
		col.Expr = e
	case FunctionCallExpr:
		upperName := strings.ToUpper(e.Name)
		if upperName == "COUNT" || upperName == "SUM" || upperName == "MIN" || upperName == "MAX" || upperName == "MEAN" || upperName == "STDDEV" || upperName == "VARIANCE" || upperName == "MEDIAN" || upperName == "QUANTILE" {
			col.IsAgg = true
			col.AggFunc = upperName
			if upperName == "COUNT" && len(e.Args) == 2 {
				if firstArg, ok := e.Args[0].(ColumnRef); ok && strings.ToUpper(firstArg.Name) == "DISTINCT" {
					col.Distinct = true
					col.Expr = e.Args[1]
				} else {
					col.Expr = e.Args[0]
				}
			} else if len(e.Args) > 0 {
				col.Expr = e.Args[0]
			}
		} else {
			col.Expr = e
		}
	default:
		col.Expr = e
	}

	if p.match(TokenAS) || p.check(TokenIdentifier) {
		if !p.check(TokenAS) {
			if p.check(TokenIdentifier) {
				aliasTok := p.advance()
				if p.previous().Type == TokenIdentifier && !p.check(TokenComma) && !p.check(TokenRParen) {
					col.Alias = aliasTok.Lexeme
					return col, nil
				}
			}
		}
		if p.match(TokenAS) || p.previous().Type == TokenIdentifier {
			if p.check(TokenIdentifier) {
				col.Alias = p.advance().Lexeme
			}
		}
	}

	return col, nil
}

func (p *Parser) parseGroupBy(gb *GroupByClause) error {
	for {
		expr, err := p.parseExpression()
		if err != nil {
			return err
		}
		gb.Columns = append(gb.Columns, expr)

		if !p.match(TokenComma) {
			break
		}
	}
	return nil
}

func (p *Parser) parseOrderBy(ob *OrderByClause) error {
	for {
		expr, err := p.parseExpression()
		if err != nil {
			return err
		}

		col := OrderByColumn{Expr: expr}
		if p.match(TokenDESC) {
			col.Descending = true
		} else if p.match(TokenASC) {
			col.Descending = false
		}

		ob.Columns = append(ob.Columns, col)

		if !p.match(TokenComma) {
			break
		}
	}
	return nil
}

func (p *Parser) parseExpression() (Expression, error) {
	return p.parseOr()
}

func (p *Parser) parseOr() (Expression, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.match(TokenOR) {
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = LogicalExpr{Left: left, Op: "Or", Right: right}
	}
	return left, nil
}

func (p *Parser) parseAnd() (Expression, error) {
	left, err := parseEquality(p)
	if err != nil {
		return nil, err
	}

	for p.match(TokenAND) {
		right, err := parseEquality(p)
		if err != nil {
			return nil, err
		}
		left = LogicalExpr{Left: left, Op: "And", Right: right}
	}
	return left, nil
}

func parseEquality(p *Parser) (Expression, error) {
	left, err := p.parseComparison()
	if err != nil {
		return nil, err
	}

	for p.match(TokenEQ, TokenNE) {
		op := p.previous().Lexeme
		right, err := p.parseComparison()
		if err != nil {
			return nil, err
		}
		left = BinaryExpr{Left: left, Op: op, Right: right}
	}
	return left, nil
}

func (p *Parser) parseComparison() (Expression, error) {
	if p.match(TokenNOT) {
		expr, err := p.parseComparison()
		if err != nil {
			return nil, err
		}
		return UnaryExpr{Op: "Not", Expr: expr}, nil
	}

	left, err := p.parseArithmetic()
	if err != nil {
		return nil, err
	}

	for p.match(TokenLT, TokenGT, TokenLTE, TokenGTE) {
		op := p.previous().Lexeme
		right, err := p.parseArithmetic()
		if err != nil {
			return nil, err
		}
		left = BinaryExpr{Left: left, Op: op, Right: right}
	}

	if p.match(TokenIS) {
		if p.match(TokenNULL) {
			return IsNullExpr{Expr: left, Negated: false}, nil
		}
		if p.match(TokenNOT) {
			if !p.match(TokenNULL) {
				return nil, errors.New("expected NULL after IS NOT")
			}
			return IsNullExpr{Expr: left, Negated: true}, nil
		}
		return nil, errors.New("expected NULL after IS")
	}

	return left, nil
}

func (p *Parser) parseArithmetic() (Expression, error) {
	left, err := p.parseTerm()
	if err != nil {
		return nil, err
	}

	for p.match(TokenPlus, TokenMinus) {
		op := p.previous().Lexeme
		right, err := p.parseTerm()
		if err != nil {
			return nil, err
		}
		left = BinaryExpr{Left: left, Op: op, Right: right}
	}
	return left, nil
}

func (p *Parser) parseTerm() (Expression, error) {
	left, err := p.parseFactor()
	if err != nil {
		return nil, err
	}

	for p.match(TokenStar, TokenSlash) {
		op := p.previous().Lexeme
		right, err := p.parseFactor()
		if err != nil {
			return nil, err
		}
		left = BinaryExpr{Left: left, Op: op, Right: right}
	}
	return left, nil
}

func (p *Parser) parseFactor() (Expression, error) {
	left, err := p.parseRange()
	if err != nil {
		return nil, err
	}
	return left, nil
}

func (p *Parser) parseRange() (Expression, error) {
	left, err := p.parseIn()
	if err != nil {
		return nil, err
	}

	if p.match(TokenBETWEEN) {
		lower, err := p.parseIn()
		if err != nil {
			return nil, err
		}
		if !p.match(TokenAND) {
			return nil, errors.New("expected AND in BETWEEN expression")
		}
		upper, err := p.parseIn()
		if err != nil {
			return nil, err
		}
		return BetweenExpr{Expr: left, Lower: lower, Upper: upper}, nil
	}

	return left, nil
}

func (p *Parser) parseIn() (Expression, error) {
	left, err := p.parseLike()
	if err != nil {
		return nil, err
	}

	if p.match(TokenIN) {
		if !p.match(TokenLParen) {
			return nil, errors.New("expected ( after IN")
		}

		var values []Expression
		for !p.check(TokenRParen) {
			val, err := p.parsePrimary()
			if err != nil {
				return nil, err
			}
			values = append(values, val)

			if !p.match(TokenComma) {
				break
			}
		}

		if !p.match(TokenRParen) {
			return nil, errors.New("expected ) after IN values")
		}

		return InExpr{Expr: left, Values: values}, nil
	}

	return left, nil
}

func (p *Parser) parseLike() (Expression, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	if p.match(TokenLIKE) {
		pattern, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return LikeExpr{Expr: left, Pattern: pattern}, nil
	}

	return left, nil
}

func (p *Parser) parseUnary() (Expression, error) {
	if p.match(TokenMinus) {
		expr, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return UnaryExpr{Op: "-", Expr: expr}, nil
	}

	return p.parsePrimary()
}

func (p *Parser) parsePrimary() (Expression, error) {
	if p.match(TokenNumber) {
		return Literal{Value: p.previous().Value}, nil
	}

	if p.match(TokenString) {
		return Literal{Value: p.previous().Value}, nil
	}

	if p.match(TokenTRUE) {
		return Literal{Value: true}, nil
	}

	if p.match(TokenFALSE) {
		return Literal{Value: false}, nil
	}

	if p.match(TokenCASE) {
		return p.parseCase()
	}

	if p.match(TokenIdentifier) || p.match(TokenCOUNT) || p.match(TokenSUM) || p.match(TokenMIN) || p.match(TokenMAX) || p.match(TokenMEAN) || p.match(TokenSTDDEV) || p.match(TokenVARIANCE) || p.match(TokenMEDIAN) || p.match(TokenQUANTILE) {
		name := p.previous().Lexeme

		if p.match(TokenLParen) {
			args := []Expression{}
			upperName := strings.ToUpper(name)
			if upperName == "COUNT" && p.match(TokenDISTINCT) {
				args = append(args, ColumnRef{Name: "DISTINCT"})
			}
			for !p.check(TokenRParen) {
				arg, err := p.parseExpression()
				if err != nil {
					return nil, err
				}
				args = append(args, arg)

				if !p.match(TokenComma) {
					break
				}
			}

			if !p.match(TokenRParen) {
				return nil, errors.New("expected ) after function arguments")
			}

			return FunctionCallExpr{Name: name, Args: args}, nil
		}

		return ColumnRef{Name: name}, nil
	}

	if p.match(TokenDISTINCT) {
		return ColumnRef{Name: "DISTINCT"}, nil
	}

	if p.match(TokenLParen) {
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}

		if !p.match(TokenRParen) {
			return nil, errors.New("expected )")
		}

		return expr, nil
	}

	if p.match(TokenYEAR) || p.match(TokenMONTH) || p.match(TokenDAY) || p.match(TokenHOUR) || p.match(TokenUPPER) || p.match(TokenLOWER) || p.match(TokenTRIM) || p.match(TokenLENGTH) || p.match(TokenSUBSTRING) {
		name := p.previous().Lexeme

		if p.match(TokenLParen) {
			args := []Expression{}
			for !p.check(TokenRParen) {
				arg, err := p.parseExpression()
				if err != nil {
					return nil, err
				}
				args = append(args, arg)

				if !p.match(TokenComma) {
					break
				}
			}

			if !p.match(TokenRParen) {
				return nil, errors.New("expected ) after function arguments")
			}

			return FunctionCallExpr{Name: name, Args: args}, nil
		}
	}

	return nil, fmt.Errorf("unexpected token: %v", p.peek())
}

func (p *Parser) parseCase() (Expression, error) {
	caseExpr := CaseExpr{}

	if !p.check(TokenWHEN) {
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		caseExpr.Expr = expr
	}

	for {
		if !p.match(TokenWHEN) {
			return nil, errors.New("expected WHEN in CASE expression")
		}

		condition, err := p.parseExpression()
		if err != nil {
			return nil, err
		}

		if !p.match(TokenTHEN) {
			return nil, errors.New("expected THEN in CASE expression")
		}

		then, err := p.parseExpression()
		if err != nil {
			return nil, err
		}

		caseExpr.Whens = append(caseExpr.Whens, WhenClause{
			Condition: condition,
			Then:      then,
		})

		if !p.check(TokenWHEN) {
			break
		}
	}

	if p.match(TokenELSE) {
		elseExpr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		caseExpr.Else = elseExpr
	}

	if !p.match(TokenEND) {
		return nil, errors.New("expected END in CASE expression")
	}

	return caseExpr, nil
}

func (p *Parser) match(types ...TokenType) bool {
	for _, t := range types {
		if p.check(t) {
			p.advance()
			return true
		}
	}
	return false
}

func (p *Parser) check(t TokenType) bool {
	if p.isAtEnd() {
		return false
	}
	return p.peek().Type == t
}

func (p *Parser) advance() Token {
	if !p.isAtEnd() {
		p.current++
	}
	return p.previous()
}

func (p *Parser) previous() Token {
	return p.tokens[p.current-1]
}

func (p *Parser) peek() Token {
	return p.tokens[p.current]
}

func (p *Parser) isAtEnd() bool {
	return p.peek().Type == TokenEOF
}

type Lexer struct {
	input        string
	position     int
	readPosition int
	ch           rune
	line         int
	column       int
}

func NewLexer(input string) *Lexer {
	l := &Lexer{
		input:  input,
		line:   1,
		column: 1,
	}
	l.readChar()
	return l
}

func (l *Lexer) readChar() {
	if l.readPosition >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = rune(l.input[l.readPosition])
	}
	l.position = l.readPosition
	l.readPosition++
}

func (l *Lexer) Lex() []Token {
	var tokens []Token

	for l.ch != 0 {
		startLine := l.line
		startColumn := l.column

		if l.ch == ' ' || l.ch == '\t' || l.ch == '\r' {
			l.readChar()
			continue
		}

		if l.ch == '\n' {
			l.line++
			l.column = 1
			l.readChar()
			continue
		}

		if l.ch == '=' {
			l.readChar()
			if l.ch == '=' {
				tokens = append(tokens, Token{Type: TokenEQ, Lexeme: "==", Line: startLine, Column: startColumn})
				l.readChar()
			} else {
				tokens = append(tokens, Token{Type: TokenEQ, Lexeme: "=", Line: startLine, Column: startColumn})
			}
			continue
		}

		if l.ch == '!' {
			l.readChar()
			if l.ch == '=' {
				tokens = append(tokens, Token{Type: TokenNE, Lexeme: "!=", Line: startLine, Column: startColumn})
				l.readChar()
			} else {
				tokens = append(tokens, Token{Type: TokenError, Lexeme: "!", Line: startLine, Column: startColumn})
			}
			continue
		}

		if l.ch == '<' {
			l.readChar()
			if l.ch == '=' {
				tokens = append(tokens, Token{Type: TokenLTE, Lexeme: "<=", Line: startLine, Column: startColumn})
				l.readChar()
			} else {
				tokens = append(tokens, Token{Type: TokenLT, Lexeme: "<", Line: startLine, Column: startColumn})
			}
			continue
		}

		if l.ch == '>' {
			l.readChar()
			if l.ch == '=' {
				tokens = append(tokens, Token{Type: TokenGTE, Lexeme: ">=", Line: startLine, Column: startColumn})
				l.readChar()
			} else {
				tokens = append(tokens, Token{Type: TokenGT, Lexeme: ">", Line: startLine, Column: startColumn})
			}
			continue
		}

		if l.ch == ',' {
			tokens = append(tokens, Token{Type: TokenComma, Lexeme: ",", Line: startLine, Column: startColumn})
			l.readChar()
			continue
		}

		if l.ch == '.' {
			tokens = append(tokens, Token{Type: TokenDot, Lexeme: ".", Line: startLine, Column: startColumn})
			l.readChar()
			continue
		}

		if l.ch == '(' {
			tokens = append(tokens, Token{Type: TokenLParen, Lexeme: "(", Line: startLine, Column: startColumn})
			l.readChar()
			continue
		}

		if l.ch == ')' {
			tokens = append(tokens, Token{Type: TokenRParen, Lexeme: ")", Line: startLine, Column: startColumn})
			l.readChar()
			continue
		}

		if l.ch == '*' {
			tokens = append(tokens, Token{Type: TokenStar, Lexeme: "*", Line: startLine, Column: startColumn})
			l.readChar()
			continue
		}

		if l.ch == '+' {
			tokens = append(tokens, Token{Type: TokenPlus, Lexeme: "+", Line: startLine, Column: startColumn})
			l.readChar()
			continue
		}

		if l.ch == '-' {
			tokens = append(tokens, Token{Type: TokenMinus, Lexeme: "-", Line: startLine, Column: startColumn})
			l.readChar()
			continue
		}

		if l.ch == '/' {
			tokens = append(tokens, Token{Type: TokenSlash, Lexeme: "/", Line: startLine, Column: startColumn})
			l.readChar()
			continue
		}

		if l.ch == '"' || l.ch == '\'' {
			tok := l.readString(l.ch)
			tok.Line = startLine
			tok.Column = startColumn
			tokens = append(tokens, tok)
			continue
		}

		if unicode.IsDigit(l.ch) {
			tok := l.readNumber()
			tok.Line = startLine
			tok.Column = startColumn
			tokens = append(tokens, tok)
			continue
		}

		if l.ch == '_' || unicode.IsLetter(l.ch) {
			tok := l.readIdentifier()
			tok.Line = startLine
			tok.Column = startColumn
			tokens = append(tokens, tok)
			continue
		}

		tokens = append(tokens, Token{Type: TokenError, Lexeme: string(l.ch), Line: startLine, Column: startColumn})
		l.readChar()
	}

	tokens = append(tokens, Token{Type: TokenEOF, Lexeme: "", Line: l.line, Column: l.column})
	return tokens
}

func (l *Lexer) readString(quote rune) Token {
	l.readChar()

	var sb strings.Builder
	for l.ch != 0 && l.ch != quote {
		if l.ch == '\\' {
			l.readChar()
			switch l.ch {
			case 'n':
				sb.WriteByte('\n')
			case 't':
				sb.WriteByte('\t')
			case '\\':
				sb.WriteByte('\\')
			case '\'':
				sb.WriteByte('\'')
			case '"':
				sb.WriteByte('"')
			default:
				sb.WriteByte(byte(l.ch))
			}
		} else {
			sb.WriteByte(byte(l.ch))
		}
		l.readChar()
	}

	l.readChar()

	return Token{
		Type:   TokenString,
		Lexeme: sb.String(),
		Value:  sb.String(),
	}
}

func (l *Lexer) readNumber() Token {
	start := l.position
	hasDecimal := false

	for unicode.IsDigit(l.ch) {
		l.readChar()
	}

	if l.ch == '.' {
		hasDecimal = true
		l.readChar()
		for unicode.IsDigit(l.ch) {
			l.readChar()
		}
	}

	lit := l.input[start:l.position]
	if hasDecimal {
		var val float64
		fmt.Sscanf(lit, "%f", &val)
		return Token{Type: TokenNumber, Lexeme: lit, Value: val}
	}

	var val int64
	fmt.Sscanf(lit, "%d", &val)
	return Token{Type: TokenNumber, Lexeme: lit, Value: val}
}

func (l *Lexer) readIdentifier() Token {
	start := l.position
	for l.ch == '_' || unicode.IsLetter(l.ch) || unicode.IsDigit(l.ch) {
		l.readChar()
	}

	lit := l.input[start:l.position]
	upper := strings.ToUpper(lit)

	tokType := TokenIdentifier
	switch upper {
	case "SELECT":
		tokType = TokenSELECT
	case "FROM":
		tokType = TokenFROM
	case "WHERE":
		tokType = TokenWHERE
	case "AND":
		tokType = TokenAND
	case "OR":
		tokType = TokenOR
	case "NOT":
		tokType = TokenNOT
	case "IS":
		tokType = TokenIS
	case "NULL":
		tokType = TokenNULL
	case "TRUE":
		tokType = TokenTRUE
	case "FALSE":
		tokType = TokenFALSE
	case "IN":
		tokType = TokenIN
	case "LIKE":
		tokType = TokenLIKE
	case "BETWEEN":
		tokType = TokenBETWEEN
	case "GROUP":
		tokType = TokenGROUP
	case "BY":
		tokType = TokenBY
	case "HAVING":
		tokType = TokenHAVING
	case "ORDER":
		tokType = TokenORDER
	case "ASC":
		tokType = TokenASC
	case "DESC":
		tokType = TokenDESC
	case "LIMIT":
		tokType = TokenLIMIT
	case "DISTINCT":
		tokType = TokenDISTINCT
	case "AS":
		tokType = TokenAS
	case "COUNT":
		tokType = TokenCOUNT
	case "SUM":
		tokType = TokenSUM
	case "MIN":
		tokType = TokenMIN
	case "MAX":
		tokType = TokenMAX
	case "MEAN":
		tokType = TokenMEAN
	case "STDDEV":
		tokType = TokenSTDDEV
	case "VARIANCE":
		tokType = TokenVARIANCE
	case "MEDIAN":
		tokType = TokenMEDIAN
	case "QUANTILE":
		tokType = TokenQUANTILE
	case "CASE":
		tokType = TokenCASE
	case "WHEN":
		tokType = TokenWHEN
	case "THEN":
		tokType = TokenTHEN
	case "ELSE":
		tokType = TokenELSE
	case "END":
		tokType = TokenEND
	case "OFFSET":
		tokType = TokenOFFSET
	case "YEAR":
		tokType = TokenYEAR
	case "MONTH":
		tokType = TokenMONTH
	case "DAY":
		tokType = TokenDAY
	case "HOUR":
		tokType = TokenHOUR
	case "UPPER":
		tokType = TokenUPPER
	case "LOWER":
		tokType = TokenLOWER
	case "TRIM":
		tokType = TokenTRIM
	case "LENGTH":
		tokType = TokenLENGTH
	case "SUBSTRING":
		tokType = TokenSUBSTRING
	case "JOIN":
		tokType = TokenJOIN
	case "INNER":
		tokType = TokenINNER
	case "LEFT":
		tokType = TokenLEFT
	case "RIGHT":
		tokType = TokenRIGHT
	case "ON":
		tokType = TokenON
	}

	return Token{Type: tokType, Lexeme: lit}
}

func Parse(sql string) (*SQLStatement, error) {
	lexer := NewLexer(sql)
	tokens := lexer.Lex()
	parser := NewParser(tokens)
	return parser.Parse()
}
