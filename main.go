package main

import (
	"fmt"
	"orchiddb/globals"
	"orchiddb/storage"
	"orchiddb/system"
)

var majorVersion int = 0 // Proud version
var minorVersion int = 1 // Real version
var patchVersion int = 0 // Sucky verison

func main() {
	system.PrintStartupText(majorVersion, minorVersion, patchVersion)

	test()
}

func test() {
	globals.MinFillPercent = 0.0125
	globals.MaxFillPercent = 0.025
	options := storage.NewOptions()

	db, err := storage.GetDB("db.db", options)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	db.Put([]byte("Key1"), []byte("Value1"))
	db.Put([]byte("Key2"), []byte("Value2"))
	db.Put([]byte("Key3"), []byte("Value3"))
	db.Put([]byte("Key4"), []byte("Value4"))
	db.Put([]byte("Key5"), []byte("Value5"))
	db.Put([]byte("Key6"), []byte("Value6"))
	for _, v := range []string{"Key1", "Key2", "Key3", "Key4", "Key5", "Key6"} {
		item, _ := db.Get([]byte(v))
		fmt.Printf("key is: %s, value is: %s\n", item.Key, item.Value)
	}

	_ = db.Del([]byte("Key1"))
	item, _ := db.Get([]byte("Key1"))

	db.WriteFreelist()
	fmt.Printf("item is: %+v\n", item)
	_ = db.Close()
}
