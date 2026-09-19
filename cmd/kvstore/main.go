package main

import (
	"fmt"

	"distributed-kv-datastore/internal/store"
)

func main() {
	ds := store.NewDataStore("node-1")

	ds.Put("foo", "bar", nil)
	items, ok := ds.Get("foo")
	fmt.Printf("get(foo) -> exists=%v value=%v vc=%v\n", ok, items[0].Value, items[0].VectorClock.Snapshot())
}
