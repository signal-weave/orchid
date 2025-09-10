package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"orchiddb/consolidate"
	"orchiddb/globals"
	"orchiddb/inmem"
	"orchiddb/manifest"
	"orchiddb/system"
)

var majorVersion int = 0 // Proud version
var minorVersion int = 1 // Real version
var patchVersion int = 0 // Sucky verison

func main() {
	system.PrintStartupText(majorVersion, minorVersion, patchVersion)
	test()
	test2()

	// -------Keep the program open---------------------------------------------
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	<-sigs
	fmt.Println("Shutting down cleanly...")
}

func test() {
	globals.FlushThreshold = 3 // testing

	store := inmem.NewMemTable("test_table", globals.FlushThreshold)

	store.Set("a", "1")
	store.Set("b", "2")
	store.Set("c", "3")

	store.Set("d", "4")
	val, ok := store.Get("d")
	if ok {
		fmt.Println("Value for 'd':", val)
	}
}

func test2() {
	manifest := &manifest.Manifest{
		Entries: map[string]manifest.ManifestEntry{
			"sstable_a_1.json": {Start: "aardvark", End: "armadillo"},
			"sstable_g_1.json": {Start: "galaxy", End: "ghost"},
			"sstable_g_2.json": {Start: "giant", End: "gust"},
		},
	}

	kv := map[string]string{
		"ant":       "v1",
		"astronaut": "v2",
		"aardwolf":  "v3",
		"gamma":     "v4",
		"goblin":    "v5",
		"gyrfalcon": "v6",
	}

	buckets, updated := consolidate.BucketKeysByManifest(manifest, kv)

	fmt.Println("Buckets:")
	for file, keys := range buckets {
		fmt.Printf("  %s: %v\n", file, keys)
	}

	fmt.Println("\nUpdated Ranges:")
	for file, e := range updated {
		fmt.Printf("  %s: [%s, %s]\n", file, e.Start, e.End)
	}
}
