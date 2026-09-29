package store

import (
	"testing"
	"time"
)

// blockingKeysPersister parks inside Keys until released.
type blockingKeysPersister struct {
	*fakePersister
	entered chan struct{}
	release chan struct{}
}

func (b *blockingKeysPersister) Keys() ([]string, error) {
	close(b.entered)
	<-b.release
	return b.fakePersister.Keys()
}

// A key listing in persister mode must not hold the DataStore's lock: a Put
// (and a Get) completes while Keys is still running.
func TestPersisterModeKeysDoesNotBlockPutOrGet(t *testing.T) {
	p := &blockingKeysPersister{fakePersister: newFakePersister(), entered: make(chan struct{}), release: make(chan struct{})}
	ds := NewDataStoreWithPersister("node-1", p)

	keysDone := make(chan error, 1)
	go func() {
		_, err := ds.Keys()
		keysDone <- err
	}()
	<-p.entered // Keys is now inside the persister

	opsDone := make(chan struct{})
	go func() {
		defer close(opsDone)
		if item := ds.Put("k", "v", nil); item == nil {
			t.Error("Put failed")
		}
		if _, found, err := ds.Get("k"); err != nil || !found {
			t.Errorf("Get: found=%v err=%v", found, err)
		}
	}()
	select {
	case <-opsDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Put/Get blocked behind an in-progress Keys")
	}

	close(p.release)
	if err := <-keysDone; err != nil {
		t.Fatalf("Keys: %v", err)
	}
}
