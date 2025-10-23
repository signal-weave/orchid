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

var majorVersion int = 0 // Proud version
var minorVersion int = 2 // Real  version
var patchVersion int = 2 // Sucky version

func main() {
	system.PrintStartupText(majorVersion, minorVersion, patchVersion)

	startup.Startup(os.Args[1:])

	go startServer()
	awaitSigterm()
}

func startServer() {
	server, err := server.NewServer()
	if err != nil {
		fmt.Println("server start error:", err)
		os.Exit(2)
		select {}
	}
	if err := server.Run(); err != nil {
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
