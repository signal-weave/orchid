package main

import (
	"fmt"
	"orchiddb/storage"
	"orchiddb/system"
)

var majorVersion int = 0 // Proud version
var minorVersion int = 1 // Real version
var patchVersion int = 0 // Sucky verison

func main() {
	system.PrintStartupText(majorVersion, minorVersion, patchVersion)

	test1()
}

func test1() {
	db, err := storage.GetDB("db.db")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Insert one record
	if err := db.Put([]byte("Key1"), []byte("Value1")); err != nil {
		panic(err)
	}

	// Fetch root node again
	node, err := db.GetNode(db.Meta.RootPage)
	if err != nil {
		panic(err)
	}

	index, containingNode, err := node.FindKey([]byte("Key1"))
	if err != nil {
		panic(err)
	}
	if index < 0 || containingNode == nil {
		fmt.Println("Key not found")
		return
	}

	res := containingNode.Items[index]
	fmt.Printf("Key is: %s, value is: %s\n", res.Key, res.Value)
}
