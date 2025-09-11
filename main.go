package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"orchiddb/storage/tables"
	"orchiddb/system"
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
	testTable := tables.NewSSTable("phone_numbers")
	err := testTable.Create()
	if err != nil {
		fmt.Println(err)
	}

	if err = testTable.Put("nate", "1234-567-890"); err != nil {
		fmt.Println(err)
	}
}
