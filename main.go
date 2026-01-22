package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"orchiddb/execution"
	"orchiddb/server"
	"orchiddb/system"
	"orchiddb/system/startup"
)

var majorVersion = 0 // Proud version
var minorVersion = 2 // Real  version
var patchVersion = 2 // Sucky version

func main() {
	system.PrintStartupText(majorVersion, minorVersion, patchVersion)

	startup.Startup(os.Args[1:])

	go startServer()
	awaitSigterm()
}

func startServer() {
	s, err := server.NewServer()
	if err != nil {
		fmt.Println("server start error:", err)
		os.Exit(2)
	}
	if err := s.Run(); err != nil {
		fmt.Println("server runtime error:", err)
	}
}

func awaitSigterm() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

	sig := <-sigs
	fmt.Println("received signal:", sig)
	fmt.Println("shutting down...")

	execution.CloseAllTables()
}
