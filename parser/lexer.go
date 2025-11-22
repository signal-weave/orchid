package parser

// Lexer is responsible for reading through the characters of a command.
type Lexer struct {
	input        string
	position     int  // current position in input (points to current char)
	readPosition int  // current reading position in input (after current char)
	ch           byte // current char under examination
}

func NewLexer(input string) *Lexer {
	l := &Lexer{input: input}
	l.readChar()
	return l
}

// Advances the current and peek pointers to the next positions for the lexer.
func (l *Lexer) readChar() {
	if l.readPosition >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.readPosition]
	}
	l.position = l.readPosition
	l.readPosition += 1
}

// NextToken checks the current byte to match against a pre-defined token.
// If one is found, a new token of a premade type is returned at the current
// char position, otherwise, a new token of user inputs is created and returned.
func (l *Lexer) NextToken() Token {
	var tok Token

	l.skipWhitespace()

	switch l.ch {
	case ',':
		tok = newToken(COMMA, l.ch)
	case '(':
		tok = newToken(LPAREN, l.ch)
	case ')':
		tok = newToken(RPAREN, l.ch)
	case 0:
		tok.Literal = ""
		tok.Type = EOF
	default:
		if isLetterOrDigit(l.ch) {
			tok.Literal = l.readIdentifier()
			tok.Type = LookupIdentifier(tok.Literal)
			return tok
		} else {
			tok = newToken(ILLEGAL, l.ch)
		}
	}

	l.readChar()
	return tok
}

// Skips whitespace until a new char is hit.
func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

// Creates a token of the given type with a string value of the given bytes.
func newToken(tokenType TokenType, ch byte) Token {
	return Token{Type: tokenType, Literal: string(ch)}
}

// Reads all chars until a non-char is found and returns the identifier string
// composed of those chars.
func (l *Lexer) readIdentifier() string {
	position := l.position
	for isLetterOrDigit(l.ch) {
		l.readChar()
	}
	return l.input[position:l.position]
}

// Is the current byte a character?
func isLetterOrDigit(ch byte) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_' || '0' <= ch && ch <= '9'
}
