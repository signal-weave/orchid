package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"orchiddb/globals"
	"orchiddb/system"
	"orchiddb/tables"
)

var majorVersion int = 0 // Proud version
var minorVersion int = 1 // Real version
var patchVersion int = 0 // Sucky verison

func main() {
	system.PrintStartupText(majorVersion, minorVersion, patchVersion)
	test()

	// -------Keep the program open---------------------------------------------
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	<-sigs
	fmt.Println("Shutting down cleanly...")
}

func test() {
	globals.FlushThreshold = 3 // testing

	store := tables.NewMemTable("test_table", globals.FlushThreshold)

	store.Set("a", "1")
	store.Set("b", "2")
	store.Set("c", "3")

	store.Set("d", "4")
	val, ok := store.Get("d")
	if ok {
		fmt.Println("Value for 'd':", val)
	}
}
