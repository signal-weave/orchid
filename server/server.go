package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"

	"orchiddb/execution"
	"orchiddb/globals"
	"orchiddb/parser"
)

type Server struct {
	listener net.Listener

	address string
	port    int
}

func NewServer() (*Server, error) {
	addr := fmt.Sprintf("%s:%d", globals.Address, globals.Port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("cannot create listener for %s: %w", addr, err)
	}

	return &Server{
		address:  globals.Address,
		port:     globals.Port,
		listener: l,
	}, nil
}

// Run...
func (server *Server) Run() error {
	for !globals.PerformShutdown {
		conn, err := server.listener.Accept()
		if err != nil {
			fmt.Println("accept error:", err)
			continue
		}

		go handleConnection(conn)
	}

	return nil
}

// Parses the incoming query and sends it to the execution layer.
func handleConnection(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		rawQuery := scanner.Text()

		l := parser.NewLexer(rawQuery)
		p := parser.NewParser(l)
		cmd := p.ParseCommand()
		if cmd == nil || cmd.Command == nil {
			io.WriteString(conn, "ERR: parseError\n")
			continue
		}

		fmt.Println("parsed command:", cmd.Command.String())

		switch t := cmd.Command.(type) {
		case *parser.GetCommand:
			t.Conn = conn
		}
		execution.ExecuteCommand(cmd)
	}

	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		fmt.Println("read error:", err)
	}
}
