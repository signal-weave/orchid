package parser

import (
	"fmt"
	"strings"

	"orchiddb/globals"
)

type parseCmdFn func() Node

// Parser is the primary token parsing engine.
// Contains a lexer that will lex a byte stream, creating an array of tokens.
// These tokens will then be parsed into command nodes and finally packed into a
// command struct containing the command.
type Parser struct {
	l      *Lexer
	errors []string

	curToken  Token
	peekToken Token

	parseFunctions map[TokenType]parseCmdFn
}

func NewParser(l *Lexer) *Parser {
	p := &Parser{
		l:      l,
		errors: []string{},
	}

	p.parseFunctions = make(map[TokenType]parseCmdFn)
	p.registerParseFn(STOP, p.paseStopCommand)
	p.registerParseFn(MAKE, p.parseMakeCommand)
	p.registerParseFn(DROP, p.parseDropCommand)
	p.registerParseFn(GET, p.parseGetCommand)
	p.registerParseFn(PUT, p.parsePutCommand)
	p.registerParseFn(DEL, p.parseDelCommand)

	// Read two tokens, so curToken and peekToken are both set.
	p.nextToken()
	p.nextToken()

	return p
}

func (p *Parser) registerParseFn(tokenType TokenType, fn parseCmdFn) {
	p.parseFunctions[tokenType] = fn
}

// ParseCommand instructs the Lexer to parse out the next token and then decides
// which parse function to invoke based on the lexed token type.
// Currently only supports queries of a single command.
func (p *Parser) ParseCommand() *Command {
	cmd := &Command{}

	for p.curToken.Type != EOF {
		parseFn, ok := p.parseFunctions[p.curToken.Type]

		if !ok {
			p.noPrefixParseFnError(p.curToken.Type)
			return nil
		}

		n := parseFn()
		cmd.Command = n
		p.nextToken()
	}

	return cmd
}

// -------Token Handling--------------------------------------------------------

// nextToken sets curToken to the peekToken and moves the peekToken forward by one.
func (p *Parser) nextToken() {
	p.curToken = p.peekToken
	p.peekToken = p.l.NextToken()
}

// peekTokenIs checks if the peekToken is of the given type.
func (p *Parser) peekTokenIs(t TokenType) bool {
	return p.peekToken.Type == t
}

// expectPeek checks if the peekToken is of the given type, and if so, moves the
// peekToken forward by one.
func (p *Parser) expectPeek(t TokenType) bool {
	if p.peekTokenIs(t) {
		p.nextToken()
		return true
	}

	p.peekError(t)
	return false
}

// -------Error Handling--------------------------------------------------------

func (p *Parser) Errors() []string {
	return p.errors
}

func (p *Parser) peekError(t TokenType) {
	msg := fmt.Sprintf("[ERROR] expected next token to be %s, got %s instead",
		t, p.peekToken.Type)
	p.errors = append(p.errors, msg)
}

func (p *Parser) noPrefixParseFnError(t TokenType) {
	msg := fmt.Sprintf("[ERROR] no prefix parse function for %s found", t)
	p.errors = append(p.errors, msg)
}

func (p *Parser) missingArgumentError(arg, cmd string) {
	msg := fmt.Sprintf("[ERROR] missing %s argument for %s command", arg, cmd)
	p.errors = append(p.errors, msg)
}

// PrintCurTokenData is for debugging purposes.
func (p *Parser) PrintCurTokenData() {
	fmt.Println("cur type", p.curToken.Type)
	fmt.Println("cur literal", p.curToken.Literal)
}

// -------Command Parsing-------------------------------------------------------

func (p *Parser) missingNextArg(argName, cmd string) bool {
	if p.peekTokenIs(RPAREN) || p.peekTokenIs(COMMA) {
		p.nextToken() // Move passed RPAREN
		p.missingArgumentError(argName, cmd)
		return true
	}
	return false
}

func (p *Parser) paseStopCommand() Node {
	cmd := &StopCommand{Token: p.curToken}

	if !p.expectPeek(LPAREN) {
		return nil
	}
	if !p.expectPeek(RPAREN) {
		return nil
	}

	return cmd
}

func (p *Parser) parseMakeCommand() Node {
	cmd := &MakeCommand{Token: p.curToken}

	if !p.expectPeek(LPAREN) {
		return nil
	}

	if p.missingNextArg("Table", MAKE) {
		// move passed RPAREN
		return nil
	}

	p.nextToken() // Move passed LPAREN to TABLE

	cmd.Table = NormalizeTableKey(p.curToken.Literal)

	if !p.expectPeek(RPAREN) {
		return nil
	}

	return cmd
}

func (p *Parser) parseDropCommand() Node {
	cmd := &DropCommand{Token: p.curToken}

	if !p.expectPeek(LPAREN) {
		return nil
	}

	if p.missingNextArg("Table", DROP) {
		// move passed RPAREN
		return nil
	}

	p.nextToken() // Move passed LPAREN to TABLE

	cmd.Table = NormalizeTableKey(p.curToken.Literal)

	if !p.expectPeek(RPAREN) {
		return nil
	}

	return cmd
}

func (p *Parser) parseGetCommand() Node {
	cmd := &GetCommand{Token: p.curToken}

	if !p.expectPeek(LPAREN) {
		return nil
	}

	args := p.parseGetParameters()
	if args == nil {
		return nil
	} else if len(args) != 2 { // table + key
		return nil
	}

	cmd.Table = NormalizeTableKey(args[0].String())
	cmd.Key = args[1].String()

	return cmd
}

func (p *Parser) parseGetParameters() []*Identifier {
	var identifiers []*Identifier

	if p.missingNextArg("Table", GET) {
		// move passed RPAREN
		return nil
	}

	p.nextToken() // Move passed LPAREN to TABLE

	table := &Identifier{Token: p.curToken, Value: p.curToken.Literal}
	identifiers = append(identifiers, table)

	p.nextToken() // Move to COMMA
	if p.missingNextArg("Key", GET) {
		return nil
	}
	p.nextToken() // Move to KEY

	key := &Identifier{Token: p.curToken, Value: p.curToken.Literal}
	identifiers = append(identifiers, key)

	if !p.expectPeek(RPAREN) {
		return nil
	}

	return identifiers
}

func (p *Parser) parsePutCommand() Node {
	cmd := &PutCommand{Token: p.curToken}

	if !p.expectPeek(LPAREN) {
		return nil
	}

	args := p.parsePutParameters()
	if args == nil {
		return nil
	} else if len(args) != 3 { // table + key + value
		return nil
	}

	cmd.Table = NormalizeTableKey(args[0].String())
	cmd.Key = args[1].String()
	cmd.Value = args[2].String()

	return cmd
}

func (p *Parser) parsePutParameters() []*Identifier {
	var identifiers []*Identifier

	if p.missingNextArg("Table", PUT) {
		// move passed RPAREN
		return nil
	}

	p.nextToken() // Move passed LPAREN to TABLE

	table := &Identifier{Token: p.curToken, Value: p.curToken.Literal}
	identifiers = append(identifiers, table)

	p.nextToken() // Move to COMMA
	if p.missingNextArg("Key", PUT) {
		return nil
	}
	p.nextToken() // Move to KEY

	key := &Identifier{Token: p.curToken, Value: p.curToken.Literal}
	identifiers = append(identifiers, key)

	p.nextToken() // Move to COMMA
	if p.missingNextArg("Value", PUT) {
		return nil
	}
	p.nextToken() // Move to VALUE

	value := &Identifier{Token: p.curToken, Value: p.curToken.Literal}
	identifiers = append(identifiers, value)

	if !p.expectPeek(RPAREN) {
		return nil
	}

	return identifiers
}

func (p *Parser) parseDelCommand() Node {
	cmd := &DelCommand{Token: p.curToken}

	if !p.expectPeek(LPAREN) {
		return nil
	}

	args := p.parseDelParameters()
	if args == nil {
		return nil
	} else if len(args) != 2 { // table + key
		return nil
	}

	cmd.Table = NormalizeTableKey(args[0].String())
	cmd.Key = args[1].String()

	return cmd
}

func (p *Parser) parseDelParameters() []*Identifier {
	var identifiers []*Identifier

	if p.missingNextArg("Table", GET) {
		// move passed RPAREN
		return nil
	}

	p.nextToken() // Move passed LPAREN to TABLE

	table := &Identifier{Token: p.curToken, Value: p.curToken.Literal}
	identifiers = append(identifiers, table)

	p.nextToken() // Move to COMMA
	if p.missingNextArg("Key", GET) {
		return nil
	}
	p.nextToken() // Move to KEY

	key := &Identifier{Token: p.curToken, Value: p.curToken.Literal}
	identifiers = append(identifiers, key)

	if !p.expectPeek(RPAREN) {
		return nil
	}

	return identifiers
}

// -------Helpers---------------------------------------------------------------

// NormalizeTableKey ensures the table has no suffix.
func NormalizeTableKey(name string) string {
	if strings.HasSuffix(name, globals.TBL_SUFFIX) {
		return strings.TrimSuffix(name, globals.TBL_SUFFIX)
	}
	return name
}
