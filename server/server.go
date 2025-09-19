package server

import (
	"fmt"
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
		return nil, fmt.Errorf("cannot create listener for %s", addr)
	}

	return &Server{
		address:  globals.Address,
		port:     globals.Port,
		listener: l,
	}, nil
}

func (server *Server) Run() error {
	for !globals.PerformShutdown {
		conn, err := server.listener.Accept()
		if err != nil {
			return err
		}

		go handleConnection(conn)
	}

	return nil
}

func handleConnection(conn net.Conn) {
	data := []byte{}
	_, err := conn.Read(data)
	if err != nil {
		rawQuery := string(data)
		l := parser.NewLexer(rawQuery)
		p := parser.NewParser(l)
		cmd := p.ParseCommand()

		// Its unbelievably stupid that Go restricts struct.(type) to switches.
		switch t := cmd.Command.(type) {
		case *parser.GetCommand:
			t.Conn = conn
		}

		execution.ExecuteCommand(cmd)
	}
}
