package main

import (
	"fmt"

	"orchiddb/globals"
	"orchiddb/inmem"
	"orchiddb/system"
)

func main() {
	system.Startup()

	test()
}

func test() {
	globals.FlushThreshold = 3 // testing

	store := inmem.NewKVStore(globals.FlushThreshold)

	store.Set("a", "1")
	store.Set("b", "2")
	store.Set("c", "3")

	store.Set("d", "4")
	val, ok := store.Get("d")
	if ok {
		fmt.Println("Value for 'd':", val)
	}
}
