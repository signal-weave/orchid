package parser

import (
	"fmt"
	"net"
)

// A node is a struct that contains the fields of a command.
// E.g. a GetCommand contains the GET token, a table identifier, and a key
// identifier.
type Node interface {
	TokenLiteral() string
	String() string
	GetTable() string
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
	Token Token // the 'MAKE' keyword token
	Table string
}

func (mc *MakeCommand) TokenLiteral() string {
	return mc.Token.Literal
}

func (mc *MakeCommand) String() string {
	return fmt.Sprintf("cmd: %s( table: %s )", mc.Token.Literal, mc.Table)
}

func (mc *MakeCommand) GetTable() string {
	return mc.Table
}

// -------DROP Command----------------------------------------------------------

// DropCommand represents user intent to drop, or remove, an existing table.
type DropCommand struct {
	// DROP(table)
	Token Token  // the 'DROP' keyword token
	Table string // the first argument identifier
}

func (dc *DropCommand) TokenLiteral() string {
	return dc.Token.Literal
}

func (dc *DropCommand) String() string {
	return fmt.Sprintf("cmd: %s( table: %s )", dc.Token.Literal, dc.Table)
}

func (dc *DropCommand) GetTable() string {
	return dc.Table
}

// -------GET Command-----------------------------------------------------------

// GetCommand represents user intent to get the value of cmd.Key from cmd.Table.
type GetCommand struct {
	// GET(table, key)
	Conn net.Conn // Used to respond to requester

	Token Token  // the 'GET' keyword token
	Table string // the first argument identifier
	Key   string // The second argument identifier
}

func (gc *GetCommand) TokenLiteral() string {
	return gc.Token.Literal
}

func (gc *GetCommand) String() string {
	return fmt.Sprintf(
		"cmd: %s( table: %s, key: %s )", gc.Token.Literal, gc.Table, gc.Key,
	)
}

func (gc *GetCommand) GetTable() string {
	return gc.Table
}

// -------PUT Command-----------------------------------------------------------

// PutCommand represents user intent to put the value of cmd.Value for cmd.Key
// into cmd.Table.
type PutCommand struct {
	// PUT(table, key, value)
	Token Token  // the 'PUT' keyword token
	Table string // The first argument identifier
	Key   string // The second argument identifier
	Value string // The third argument identifier
}

func (dc *PutCommand) TokenLiteral() string {
	return dc.Token.Literal
}

func (dc *PutCommand) String() string {
	return fmt.Sprintf(
		"cmd: %s( table: %s, key: %s, value: %s )",
		dc.Token.Literal, dc.Table, dc.Key, dc.Value,
	)
}

func (pc *PutCommand) GetTable() string {
	return pc.Table
}

// -------DEL Command-----------------------------------------------------------

// DelCommand represents user intent to delete the cmd.Key from cmd.Table.
type DelCommand struct {
	// DEL(table, key)
	Token Token  // the 'DEL' keyword token
	Table string // The first argument identifier
	Key   string // The second argument identifier
}

func (dc *DelCommand) TokenLiteral() string {
	return dc.Token.Literal
}

func (dc *DelCommand) String() string {
	return fmt.Sprintf(
		"cmd: %s( table: %s, key: %s )", dc.Token.Literal, dc.Table, dc.Key,
	)
}

func (dc *DelCommand) GetTable() string {
	return dc.Table
}
