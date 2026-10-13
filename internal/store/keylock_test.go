package store

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/vectorclock"
)

// blockingPutPersister parks a Put of blockKey inside the persister, as a
// write waiting for its fsync would be, until released.
type blockingPutPersister struct {
	*fakePersister
	blockKey string
	entered  chan struct{}
	release  chan struct{}
}

func newBlockingPutPersister(blockKey string) *blockingPutPersister {
	return &blockingPutPersister{
		fakePersister: newFakePersister(),
		blockKey:      blockKey,
		entered:       make(chan struct{}),
		release:       make(chan struct{}),
	}
}

func (b *blockingPutPersister) Put(key string, item *model.DataItem) error {
	if key == b.blockKey {
		close(b.entered)
		<-b.release
	}
	return b.fakePersister.Put(key, item)
}

// keyInOtherStripe returns a key whose lock in ds is not key's. The seed is
// random, so two fixed keys share a lock in one run out of keyLockStripes.
func keyInOtherStripe(t *testing.T, ds *DataStore, key string) string {
	t.Helper()
	for i := range 4 * keyLockStripes {
		other := fmt.Sprintf("other-%d", i)
		if ds.keyLock(other) != ds.keyLock(key) {
			return other
		}
	}
	t.Fatalf("no key outside %q's lock", key)
	return ""
}

// parkPut starts ds.Put(key) and waits until it is inside the persister,
// holding key's lock. Releasing p lets it finish; the returned channel
// closes when it has.
func parkPut(t *testing.T, ds *DataStore, p *blockingPutPersister, key string) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if ds.Put(key, "parked", nil) == nil {
			t.Errorf("parked Put %q failed", key)
		}
	}()
	<-p.entered
	return done
}

// within fails t unless f returns within a second.
func within(t *testing.T, what string, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("%s is still waiting on the parked write", what)
	}
}

// Every write to another key completes while a write is still in the
// persister, as a write waiting for its fsync is: the lock is per key.
func TestWritesToOtherKeysDoNotWaitForAWriteInFlight(t *testing.T) {
	p := newBlockingPutPersister("a")
	ds := NewDataStoreWithPersister("node-1", p)
	other := keyInOtherStripe(t, ds, "a")
	parked := parkPut(t, ds, p, "a")

	within(t, "Put of another key", func() {
		if ds.Put(other, "v", nil) == nil {
			t.Error("Put failed")
		}
	})
	within(t, "MergeReplicated of another key", func() {
		if err := ds.MergeReplicated(other, &model.DataItem{Value: "w", VectorClock: vectorclock.FromSnapshot(map[string]uint32{"node-2": 1})}); err != nil {
			t.Errorf("MergeReplicated: %v", err)
		}
	})
	within(t, "RestoreVersions of another key", func() {
		if err := ds.RestoreVersions(other, nil); err != nil {
			t.Errorf("RestoreVersions: %v", err)
		}
	})

	close(p.release)
	<-parked
}

// A read takes no lock: a Get of the key being written completes while the
// write is in the persister, and sees the key as it was before it.
func TestReadsDoNotWaitForAWriteInFlightToTheirKey(t *testing.T) {
	p := newBlockingPutPersister("a")
	ds := NewDataStoreWithPersister("node-1", p)
	if ds.Put("b", "v", nil) == nil {
		t.Fatal("Put b failed")
	}
	parked := parkPut(t, ds, p, "a")

	within(t, "Get of the key being written", func() {
		if _, found, err := ds.Get("a"); err != nil || found {
			t.Errorf("Get a during its first write: found=%v err=%v, want not found", found, err)
		}
	})
	within(t, "GetLiveItems of another key", func() {
		if _, found := ds.GetLiveItems("b"); !found {
			t.Error("GetLiveItems b: not found")
		}
	})

	close(p.release)
	<-parked
	if _, found, err := ds.Get("a"); err != nil || !found {
		t.Fatalf("Get a after its write: found=%v err=%v", found, err)
	}
}

// Writes to one key still take turns: each Put with no context reads the
// key and versions on top of what it read, so if two ever read the same
// state they would get the same clock, and one would be dropped as Equal.
// After n concurrent Puts the key holds one version whose clock counts n.
func TestConcurrentWritesToOneKeyStillSerialize(t *testing.T) {
	const n = 64
	ds := NewDataStoreWithPersister("node-1", newFakePersister())

	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			if ds.Put("k", i, nil) == nil {
				t.Errorf("Put %d failed", i)
			}
		})
	}
	wg.Wait()

	items, found, err := ds.Get("k")
	if err != nil || !found {
		t.Fatalf("Get: found=%v err=%v", found, err)
	}
	if len(items) != 1 {
		t.Fatalf("got %d versions, want 1: concurrent Puts versioned from the same state", len(items))
	}
	if got := items[0].VectorClock.Snapshot()["node-1"]; got != n {
		t.Fatalf("clock counts %d writes, want %d", got, n)
	}
}
