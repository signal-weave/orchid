package parser

import (
	"fmt"
)

// A node is a struct that contains the fields of a command.
// E.g. a GetCommand contains the GET token, a table identifier, and a key
// identifier.
type Node interface {
	TokenLiteral() string
	String() string
}

// A Command is a struct with a valid command node.
// These are created by the parser.
type Command struct {
	// Its very possible the parser parses an array of command nodes in the
	// future, which is why this wrapper exists.
	Command Node
}

type Identifier struct {
	Token Token // the token.IDENT token
	Value string
}

func (i *Identifier) TokenLiteral() string { return i.Token.Literal }
func (i *Identifier) String() string       { return i.Value }

// -------MAKE Command----------------------------------------------------------

// MakeCommand represents user intent to create a new table.
type MakeCommand struct {
	// MAKE(table)
	Token Token       // the 'MAKE' keyword token
	Table *Identifier // the first argument identifier
}

func (dc *MakeCommand) TokenLiteral() string {
	return dc.Token.Literal
}

func (dc *MakeCommand) String() string {
	return fmt.Sprintf(
		"cmd: %s( table: %s )", dc.Token.Literal, dc.Table.Token.Literal,
	)
}

// -------DROP Command----------------------------------------------------------

// DropCommand represents user intent to drop, or remove, an existing table.
type DropCommand struct {
	// DROP(table)
	Token Token       // the 'DROP' keyword token
	Table *Identifier // the first argument identifier
}

func (dc *DropCommand) TokenLiteral() string {
	return dc.Token.Literal
}

func (dc *DropCommand) String() string {
	return fmt.Sprintf(
		"cmd: %s( table: %s )", dc.Token.Literal, dc.Table.Token.Literal,
	)
}

// -------GET Command-----------------------------------------------------------

// GetCommand represents user intent to get the value of cmd.Key from cmd.Table.
type GetCommand struct {
	// GET(table, key)
	Token Token       // the 'GET' keyword token
	Table *Identifier // the first argument identifier
	Key   *Identifier // The second argument identifier
}

func (gc *GetCommand) TokenLiteral() string {
	return gc.Token.Literal
}

func (gc *GetCommand) String() string {
	return fmt.Sprintf(
		"cmd: %s( table: %s, key: %s )",
		gc.Token.Literal, gc.Table.Token.Literal, gc.Key.Token.Literal,
	)
}

// -------PUT Command-----------------------------------------------------------

// PutCommand represents user intent to put the value of cmd.Value for cmd.Key
// into cmd.Table.
type PutCommand struct {
	// PUT(table, key, value)
	Token Token       // the 'PUT' keyword token
	Table *Identifier // The first argument identifier
	Key   *Identifier // The second argument identifier
	Value *Identifier // The third argument identifier
}

func (dc *PutCommand) TokenLiteral() string {
	return dc.Token.Literal
}

func (dc *PutCommand) String() string {
	return fmt.Sprintf(
		"cmd: %s( table: %s, key: %s, value: %s )",
		dc.Token.Literal, dc.Table.Token.Literal,
		dc.Key.Token.Literal, dc.Value.Token.Literal,
	)
}

// -------DEL Command-----------------------------------------------------------

// DelCommand represents user intent to delete the cmd.Key from cmd.Table.
type DelCommand struct {
	// DEL(table, key)
	Token Token       // the 'DEL' keyword token
	Table *Identifier // The first argument identifier
	Key   *Identifier // The second argument identifier
}

func (dc *DelCommand) TokenLiteral() string {
	return dc.Token.Literal
}

func (dc *DelCommand) String() string {
	return fmt.Sprintf(
		"cmd: %s( table: %s, key: %s )",
		dc.Token.Literal, dc.Table.Token.Literal, dc.Key.Token.Literal,
	)
}
