package parser

type TokenType string

type Token struct {
	Type    TokenType
	Literal string
}

const (
	ILLEGAL = "ILLEGAL"
	EOF     = "EOF"

	IDENT = "IDENT"

	// Delimiters

	LPAREN = "("
	RPAREN = ")"
	COMMA  = ","

	// Keywords

	GET = "GET"
	PUT = "PUT"
	DEL = "DEL"

	DROP = "DROP"
	MAKE = "MAKE"
)

var keywords = map[string]TokenType{
	"GET": GET, // GET(table, key)
	"PUT": PUT, // PUT(table, key, value)
	"DEL": DEL, // DEL(table, key)

	"MAKE": MAKE, // MAKE(table)
	"DROP": DROP, // DROP(table)
}

func LookupIdentifier(ident string) TokenType {
	if tok, exists := keywords[ident]; exists {
		return tok
	}
	return IDENT
}
