package main

import (
	"fmt"
	"orchiddb/globals"
	"orchiddb/parser"
	"orchiddb/storage"
	"orchiddb/system"
)

var majorVersion int = 0 // Proud version
var minorVersion int = 2 // Real  version
var patchVersion int = 0 // Sucky verison

func main() {
	system.PrintStartupText(majorVersion, minorVersion, patchVersion)

	test()
	test2()
}

func test() {
	globals.MinFillPercent = 0.0125
	globals.MaxFillPercent = 0.025
	options := storage.NewOptions()

	db, err := storage.GetTable("db.db", options)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	entries := map[string]string{
		"Key1": "Value1",
		"Key2": "Value2",
		"Key3": "Value3",
		"Key4": "Value4",
		"Key5": "Value5",
		"Key6": "Value6",
	}

	for k, v := range entries {
		db.Put([]byte(k), []byte(v))
	}
	db.Txn.Commit()

	for _, v := range []string{"Key1", "Key2", "Key3", "Key4", "Key5", "Key6"} {
		item, _ := db.Get([]byte(v))
		fmt.Printf("key is: %s, value is: %s\n", item.Key, item.Value)
	}

	_ = db.Del([]byte("Key1"))
	db.Txn.Commit()
	item, _ := db.Get([]byte("Key1"))

	db.WriteFreelist()
	db.Txn.Commit()
	fmt.Printf("item is: %+v\n", item)
	_ = db.Close()
}

func test2() {

	makeInput := "MAKE(TestTable)"
	putInput := "PUT(TestTable, Key7, Value7)"
	getInput := "GET(TestTable, Key7)"
	delInput := "DEL(TestTable, Key7)"
	dropInput := "DROP(TestTable)"

	inputs := []string{makeInput, putInput, getInput, delInput, dropInput}

	for _, v := range inputs {
		l := parser.NewLexer(v)
		p := parser.NewParser(l)
		cmd := p.ParseCommand()
		if len(p.Errors()) != 0 {
			for _, e := range p.Errors() {
				fmt.Println("[ERROR]", e)
			}
		} else {
			fmt.Println(cmd.Command.String())
		}
	}
}
