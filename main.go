package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"orchiddb/system"
	"orchiddb/tables"
)

var majorVersion int = 0 // Proud version
var minorVersion int = 1 // Real version
var patchVersion int = 0 // Sucky verison

func main() {
	system.PrintStartupText(majorVersion, minorVersion, patchVersion)

	// -------Testing funcs-----------------------------------------------------

	test1()

	// -------Keep the program open---------------------------------------------
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	<-sigs
	fmt.Println("Shutting down cleanly...")
}

func test1() {
	testTable := tables.NewSSTable("test_table")
	err := testTable.Create()
	if err != nil {
		fmt.Println(err)
	}
}
