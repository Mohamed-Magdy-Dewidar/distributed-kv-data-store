package main

import (
	"distributed-kv-datastore/internal/store"
	"fmt"
	"log"
)

func main() {
	ds := store.NewDataStore("node-1")

	ds.Put("foo", "bar", nil)
	items, ok, err := ds.Get("foo")
	if err != nil {
		log.Fatalf("get(foo): %v", err)
	}
	fmt.Printf("get(foo) -> exists=%v value=%v vc=%v\n", ok, items[0].Value, items[0].VectorClock.Snapshot())
}
