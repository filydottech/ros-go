package routeros

// Expr is an immutable RouterOS query expression that supplies ordered query
// words through Words.
type Expr interface {
	Words() []string
	queryExpr()
}

type queryExpr struct {
	words []string
}

func (e queryExpr) Words() []string {
	return append([]string(nil), e.words...)
}

func (queryExpr) queryExpr() {}

func newExpr(words ...string) Expr {
	return queryExpr{words: append([]string(nil), words...)}
}

// Equal matches a field whose value equals value.
func Equal(name, value string) Expr {
	return newExpr("?" + name + "=" + value)
}

// LessThan matches a field whose value is less than value.
func LessThan(name, value string) Expr {
	return newExpr("?<" + name + "=" + value)
}

// GreaterThan matches a field whose value is greater than value.
func GreaterThan(name, value string) Expr {
	return newExpr("?>" + name + "=" + value)
}

// Exists matches records where name exists.
func Exists(name string) Expr {
	return newExpr("?" + name)
}

// Absent matches records where name is absent.
func Absent(name string) Expr {
	return newExpr("?-" + name)
}

// Not inverts expression with RouterOS's postfix query operator. It panics if
// expression is nil.
func Not(expression Expr) Expr {
	words := exprWords(expression)
	return newExpr(append(words, "?#!")...)
}

// And combines expressions with RouterOS's postfix logical-and operator. It
// panics if any expression is nil.
func And(first Expr, rest ...Expr) Expr {
	return combine("?#&", first, rest...)
}

// Or combines expressions with RouterOS's postfix logical-or operator. It
// panics if any expression is nil.
func Or(first Expr, rest ...Expr) Expr {
	return combine("?#|", first, rest...)
}

func combine(operator string, first Expr, rest ...Expr) Expr {
	words := exprWords(first)
	for _, expression := range rest {
		words = append(words, exprWords(expression)...)
	}
	for range rest {
		words = append(words, operator)
	}
	return newExpr(words...)
}

func exprWords(expression Expr) []string {
	if expression == nil {
		panic("routeros: query expression is nil")
	}
	return expression.Words()
}
