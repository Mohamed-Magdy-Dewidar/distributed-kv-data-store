package engine

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"distributed-kv-datastore/internal/storage/wal"
	"distributed-kv-datastore/internal/store"
)

// writeModes are the two ways writes reach the WAL (WithGroupCommit). The
// write-path invariants hold in both, so their tests run in both.
var writeModes = []struct {
	name string
	opt  Option
}{
	{"serial", WithGroupCommit(false)},
	{"group", WithGroupCommit(true)},
}

func forEachWriteMode(t *testing.T, test func(t *testing.T, mode Option)) {
	for _, m := range writeModes {
		t.Run(m.name, func(t *testing.T) { test(t, m.opt) })
	}
}

var (
	errInjectedSync  = errors.New("injected fsync failure")
	errInjectedWrite = errors.New("injected write failure")
	errCrashed       = errors.New("injected crash: the process is gone")
)

// walFaults opens an engine's WAL segments as fakeWALFiles and controls
// them all. A Sync records how much of its file is synced instead of
// fsyncing, since these tests model durability themselves: what a crash
// keeps is exactly what was synced, plus perhaps some of what wasn't.
type walFaults struct {
	mu     sync.Mutex
	files  map[string]*fakeWALFile // by path; the latest open of each
	writes int                     // Write calls, every file
	syncs  int                     // Sync calls, every file

	hold        chan struct{} // non-nil: every Sync waits until it is closed
	syncEntered chan struct{} // a Sync waiting on hold sends here first

	failSyncAt  int  // that Sync call (counting from 1) fails with errInjectedSync
	crashAtSync int  // that Sync call crashes: it and every Write and Sync after it fail
	failWrites  bool // every Write fails with errInjectedWrite
	shortWrites bool // every Write writes half its bytes, then fails
	crashed     bool

	batchBytes []int // the size of each successful Write, in order
}

func newWALFaults() *walFaults {
	return &walFaults{files: make(map[string]*fakeWALFile), syncEntered: make(chan struct{}, 1024)}
}

type fakeWALFile struct {
	wal.File
	c            *walFaults
	path         string
	size, synced int64
}

func (c *walFaults) open(path string) (wal.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	ff := &fakeWALFile{File: f, c: c, path: path, size: info.Size(), synced: info.Size()}
	c.mu.Lock()
	c.files[path] = ff
	c.mu.Unlock()
	return ff, nil
}

func (f *fakeWALFile) Write(b []byte) (int, error) {
	c := f.c
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes++
	switch {
	case c.crashed:
		return 0, errCrashed
	case c.failWrites:
		return 0, errInjectedWrite
	case c.shortWrites:
		n, _ := f.File.Write(b[:len(b)/2])
		f.size += int64(n)
		return n, errInjectedWrite
	}
	n, err := f.File.Write(b)
	f.size += int64(n)
	if err == nil {
		c.batchBytes = append(c.batchBytes, n)
	}
	return n, err
}

func (f *fakeWALFile) Sync() error {
	c := f.c
	c.mu.Lock()
	hold := c.hold
	c.mu.Unlock()
	if hold != nil {
		c.syncEntered <- struct{}{}
		<-hold
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.syncs++
	switch {
	case c.crashed:
		return errCrashed
	case c.syncs == c.crashAtSync:
		c.crashed = true
		return errCrashed
	case c.syncs == c.failSyncAt:
		return errInjectedSync
	}
	f.synced = f.size
	return nil
}

func (f *fakeWALFile) Truncate(size int64) error {
	if err := f.File.Truncate(size); err != nil {
		return err
	}
	f.c.mu.Lock()
	f.size = size
	f.synced = min(f.synced, size)
	f.c.mu.Unlock()
	return nil
}

// holdSyncs makes every Sync wait until the returned release is called.
func (c *walFaults) holdSyncs() (release func()) {
	hold := make(chan struct{})
	c.mu.Lock()
	c.hold = hold
	c.mu.Unlock()
	return sync.OnceFunc(func() {
		c.mu.Lock()
		c.hold = nil
		c.mu.Unlock()
		close(hold)
	})
}

func (c *walFaults) counts() (writes, syncs int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writes, c.syncs
}

// sizes returns every segment file's size as the fake has tracked it.
func (c *walFaults) sizes() map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int64, len(c.files))
	for p, f := range c.files {
		out[p] = f.size
	}
	return out
}

// dropUnsynced cuts every segment file still on disk to what was synced
// plus a random part of what wasn't, as a crash may. Its engine must be
// closed.
func (c *walFaults) dropUnsynced(t *testing.T, rng *rand.Rand) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for path, f := range c.files {
		keep := f.synced
		if unsynced := f.size - f.synced; unsynced > 0 {
			keep += rng.Int64N(unsynced + 1)
		}
		if err := os.Truncate(path, keep); err != nil && !os.IsNotExist(err) {
			t.Fatalf("drop unsynced bytes of %s: %v", path, err)
		}
	}
}

// waitFor fails t unless cond becomes true within five seconds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func queued(e *StorageEngine) func() int {
	return func() int { return len(e.queue) }
}

// assertAbsent fails unless key has no visible version in e.
func assertAbsent(t *testing.T, e *StorageEngine, key string) {
	t.Helper()
	if items, found, err := e.GetAll(key); err != nil || found {
		t.Fatalf("%q: found=%v %v err=%v, want absent", key, found, values(items), err)
	}
}

// putAsync starts e.Put(key, value) and returns where its error will land.
func putAsync(e *StorageEngine, key, value string) <-chan error {
	done := make(chan error, 1)
	go func() { done <- e.Put(key, sampleItem(value)) }()
	return done
}

// result waits up to five seconds for a write's error.
func result(t *testing.T, what string, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("%s is still waiting", what)
		return nil
	}
}

// TestWriteIsVisibleOnlyAfterItsSync: while a write's fsync has not
// returned, a read does not see it; once it has, the read does.
func TestWriteIsVisibleOnlyAfterItsSync(t *testing.T) {
	forEachWriteMode(t, func(t *testing.T, mode Option) {
		faults := newWALFaults()
		e := mustOpen(t, t.TempDir(), 1<<20, mode, WithWALOpenFile(faults.open))
		defer e.Close()

		release := faults.holdSyncs()
		defer release()
		done := putAsync(e, "k", "v")
		<-faults.syncEntered // written, not yet synced

		assertAbsent(t, e, "k")
		release()
		if err := result(t, "Put", done); err != nil {
			t.Fatalf("Put: %v", err)
		}
		assertPresent(t, e, "k", "v")
	})
}

// TestFailedSyncFailsItsWholeBatchAndHaltsWrites: with group commit, a
// batch of writes whose fsync fails all get that error, none is applied,
// and every write after it, queued or new, fails fast with
// ErrWritesHalted without another WAL Write.
func TestFailedSyncFailsItsWholeBatchAndHaltsWrites(t *testing.T) {
	const n = 8
	faults := newWALFaults()
	e := mustOpen(t, t.TempDir(), 1<<20, WithGroupCommit(true), WithWALOpenFile(faults.open))
	defer e.Close()

	release := faults.holdSyncs()
	first := putAsync(e, "first", "v")
	<-faults.syncEntered // the committer is in the first batch's fsync
	batch := make([]<-chan error, n)
	for i := range batch {
		batch[i] = putAsync(e, fmt.Sprintf("batched-%d", i), "v")
	}
	waitFor(t, "the writes to queue behind the first batch", func() bool { return queued(e)() == n })
	faults.mu.Lock()
	faults.failSyncAt = 2 // the batch of n
	faults.mu.Unlock()
	release()

	if err := result(t, "first Put", first); err != nil {
		t.Fatalf("first Put: %v", err)
	}
	for i, done := range batch {
		if err := result(t, "batched Put", done); !errors.Is(err, errInjectedSync) {
			t.Fatalf("batched Put %d returned %v, want the injected fsync failure", i, err)
		}
		assertAbsent(t, e, fmt.Sprintf("batched-%d", i))
	}
	writes, syncs := faults.counts()
	if syncs != 2 {
		t.Fatalf("%d fsyncs, want 2: the first write's, then one for the batch of %d", syncs, n)
	}

	if err := e.Put("after", sampleItem("v")); !errors.Is(err, ErrWritesHalted) || !errors.Is(err, errInjectedSync) {
		t.Fatalf("Put after the failure returned %v, want ErrWritesHalted wrapping the fsync failure", err)
	}
	if err := e.Restore("first", nil); !errors.Is(err, ErrWritesHalted) {
		t.Fatalf("Restore after the failure returned %v, want ErrWritesHalted", err)
	}
	if w, _ := faults.counts(); w != writes {
		t.Fatalf("%d WAL Writes after the failure, want none", w-writes)
	}
	assertPresent(t, e, "first", "v") // reads still work
}

// TestNoRecordIsWrittenAfterAFailedWrite: in both modes, once a WAL write
// or fsync has failed, no later write reaches the WAL file, and a reopen
// recovers every write acknowledged before it and none made after.
func TestNoRecordIsWrittenAfterAFailedWrite(t *testing.T) {
	faultsBy := map[string]func(*walFaults){
		"fsync fails": func(c *walFaults) { c.failSyncAt = c.syncs + 1 },
		"write fails": func(c *walFaults) { c.failWrites = true },
		"short write": func(c *walFaults) { c.shortWrites = true },
	}
	forEachWriteMode(t, func(t *testing.T, mode Option) {
		for name, inject := range faultsBy {
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				faults := newWALFaults()
				e := mustOpen(t, dir, 1<<20, mode, WithWALOpenFile(faults.open))
				for _, k := range []string{"a", "b"} {
					if err := e.Put(k, sampleItem(k)); err != nil {
						t.Fatalf("Put %s: %v", k, err)
					}
				}

				faults.mu.Lock()
				inject(faults)
				faults.mu.Unlock()
				if err := e.Put("failed", sampleItem("failed")); err == nil || errors.Is(err, ErrWritesHalted) {
					t.Fatalf("Put during the fault returned %v, want the fault itself", err)
				}
				writes, _ := faults.counts()
				sizes := faults.sizes()

				for i := range 5 {
					if err := e.Put(fmt.Sprintf("later-%d", i), sampleItem("later")); !errors.Is(err, ErrWritesHalted) {
						t.Fatalf("later Put %d returned %v, want ErrWritesHalted", i, err)
					}
				}
				if err := e.Restore("a", nil); !errors.Is(err, ErrWritesHalted) {
					t.Fatalf("later Restore returned %v, want ErrWritesHalted", err)
				}
				if w, _ := faults.counts(); w != writes {
					t.Fatalf("%d WAL Writes after the failure, want none", w-writes)
				}
				for path, size := range faults.sizes() {
					if size != sizes[path] {
						t.Fatalf("%s grew from %d to %d bytes after the failure", path, sizes[path], size)
					}
				}
				if err := e.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}

				reopened := mustOpen(t, dir, 1<<20)
				defer reopened.Close()
				assertPresent(t, reopened, "a", "a")
				assertPresent(t, reopened, "b", "b")
				for i := range 5 {
					assertAbsent(t, reopened, fmt.Sprintf("later-%d", i))
				}
				if err := reopened.Put("c", sampleItem("c")); err != nil {
					t.Fatalf("Put after reopening: %v", err)
				}
			})
		}
	})
}

// TestCloseCommitsQueuedWrites: with group commit, writes queued when
// Close begins are committed, not dropped, and none is left waiting;
// writes after it fail with ErrClosed.
func TestCloseCommitsQueuedWrites(t *testing.T) {
	const n = 8
	dir := t.TempDir()
	faults := newWALFaults()
	e := mustOpen(t, dir, 1<<20, WithGroupCommit(true), WithWALOpenFile(faults.open))

	release := faults.holdSyncs()
	writes := []<-chan error{putAsync(e, "first", "v")}
	<-faults.syncEntered
	for i := range n {
		writes = append(writes, putAsync(e, fmt.Sprintf("queued-%d", i), "v"))
	}
	waitFor(t, "the writes to queue", func() bool { return queued(e)() == n })

	closed := make(chan error, 1)
	go func() { closed <- e.Close() }()
	waitFor(t, "Close to close the queue", func() bool {
		e.queueMu.RLock()
		defer e.queueMu.RUnlock()
		return e.queueClosed
	})
	if err := e.Put("too-late", sampleItem("v")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Put after Close began returned %v, want ErrClosed", err)
	}
	release()

	for i, done := range writes {
		if err := result(t, "queued Put", done); err != nil {
			t.Fatalf("write %d queued before Close returned %v, want it committed", i, err)
		}
	}
	if err := result(t, "Close", closed); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := mustOpen(t, dir, 1<<20)
	defer reopened.Close()
	assertPresent(t, reopened, "first", "v")
	for i := range n {
		assertPresent(t, reopened, fmt.Sprintf("queued-%d", i), "v")
	}
	assertAbsent(t, reopened, "too-late")
}

// TestCloseLeavesNoSerialWriteWaiting: without group commit, writes
// waiting for the write lock when Close begins either commit before Close
// takes it or fail with ErrClosed; none is left waiting, and exactly the
// ones that succeeded survive a reopen.
func TestCloseLeavesNoSerialWriteWaiting(t *testing.T) {
	const n = 8
	dir := t.TempDir()
	faults := newWALFaults()
	e := mustOpen(t, dir, 1<<20, WithGroupCommit(false), WithWALOpenFile(faults.open))

	release := faults.holdSyncs()
	first := putAsync(e, "first", "v")
	<-faults.syncEntered // first holds writeMu
	writes := make([]<-chan error, n)
	for i := range writes {
		writes[i] = putAsync(e, fmt.Sprintf("waiting-%d", i), "v")
	}
	closed := make(chan error, 1)
	go func() { closed <- e.Close() }()
	release()

	if err := result(t, "first Put", first); err != nil {
		t.Fatalf("first Put: %v", err)
	}
	committed := map[string]bool{}
	for i, done := range writes {
		key := fmt.Sprintf("waiting-%d", i)
		switch err := result(t, "waiting Put", done); {
		case err == nil:
			committed[key] = true
		case !errors.Is(err, ErrClosed):
			t.Fatalf("Put %s returned %v, want nil or ErrClosed", key, err)
		}
	}
	if err := result(t, "Close", closed); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := mustOpen(t, dir, 1<<20)
	defer reopened.Close()
	assertPresent(t, reopened, "first", "v")
	for i := range n {
		key := fmt.Sprintf("waiting-%d", i)
		if committed[key] {
			assertPresent(t, reopened, key, "v")
		} else {
			assertAbsent(t, reopened, key)
		}
	}
}

// keyState is what a crash test knows of one key: the last write to it
// that was acknowledged, and the write in flight when its writer's
// process crashed, if that write was to this key. A write that failed was
// not acknowledged, but its record may still be durable.
type keyState struct {
	acked, failed bool
	ackedValue    string // "" for a Restore that removed the key
	failedValue   string // likewise
	uncertain     bool   // a Restore could not hide a flushed version (store.ErrRestoreIncomplete)
	written       map[string]bool
}

// TestCrashNeverLosesAnAcknowledgedWrite: writers Put and Restore
// concurrently, each on its own keys, with memtable swaps and flushes,
// until a crash at a random fsync; the WAL then keeps only what was synced
// plus a random part of the rest. Each writer's writes carry increasing
// clocks, so each supersedes the one before. After a reopen, every key is
// as its last acknowledged write left it, or as the one write that failed
// would have, and holds nothing that wasn't written. Many seeds, both
// modes.
func TestCrashNeverLosesAnAcknowledgedWrite(t *testing.T) {
	const seeds, writers, perWriter = 40, 8, 40
	forEachWriteMode(t, func(t *testing.T, mode Option) {
		for seed := range uint64(seeds) {
			rng := rand.New(rand.NewPCG(seed, 0x67726f7570)) // "group"
			dir := t.TempDir()
			faults := newWALFaults()
			faults.crashAtSync = 1 + rng.IntN(30)
			e := mustOpen(t, dir, 4096, mode, WithWALOpenFile(faults.open)) // swaps every ~20 writes

			var mu sync.Mutex
			keys := map[string]*keyState{}
			var wg sync.WaitGroup
			for w := range writers {
				wg.Go(func() {
					writer := fmt.Sprintf("w%d", w)
					for i := range perWriter {
						key := fmt.Sprintf("%s-k%d", writer, i%10) // rewrite some keys
						restore := i%7 == 6
						value := ""
						if !restore {
							value = fmt.Sprintf("s%d-%s-%d-%s", seed, writer, i, strings.Repeat("x", 64))
						}
						mu.Lock()
						k := keys[key]
						if k == nil {
							k = &keyState{written: map[string]bool{}}
							keys[key] = k
						}
						k.written[value] = true
						mu.Unlock()

						var err error
						if restore {
							err = e.Restore(key, nil)
						} else {
							err = e.Put(key, itemWithClock(value, map[string]uint32{writer: uint32(i + 1)}))
						}

						mu.Lock()
						switch {
						case err == nil:
							k.acked, k.ackedValue = true, value
						case errors.Is(err, store.ErrRestoreIncomplete):
							k.uncertain = true // applied, but an older flushed version may still show
						default:
							k.failed, k.failedValue = true, value
						}
						mu.Unlock()
						if err != nil && !errors.Is(err, store.ErrRestoreIncomplete) {
							return // crashed: this writer's process is gone
						}
					}
				})
			}
			wg.Wait()
			if !faults.crashed {
				t.Fatalf("seed %d: the writes ended before fsync %d: nothing crashed", seed, faults.crashAtSync)
			}
			if err := e.Close(); err != nil {
				t.Fatalf("seed %d: Close: %v", seed, err)
			}
			faults.dropUnsynced(t, rng)

			reopened := mustOpen(t, dir, 4096)
			for key, k := range keys {
				items, found, err := reopened.GetAll(key)
				if err != nil {
					t.Fatalf("seed %d: GetAll %s: %v", seed, key, err)
				}
				got := ""
				if found {
					if len(items) != 1 {
						t.Fatalf("seed %d: %s holds %v, want one version: its writes supersede each other", seed, key, values(items))
					}
					got = items[0].Value.(string)
				}
				if !k.written[got] && found {
					t.Fatalf("seed %d: %s holds %.30q..., which was never written", seed, key, got)
				}
				if k.uncertain {
					continue
				}
				allowed := map[string]bool{}
				if k.acked {
					allowed[k.ackedValue] = true
				} else {
					allowed[""] = true // never acknowledged: it may not exist
				}
				if k.failed {
					allowed[k.failedValue] = true
				}
				if !allowed[got] {
					t.Fatalf("seed %d (crash at fsync %d): %s is %.30q..., want its acknowledged state %.30q... (or its failed write's %.30q...)",
						seed, faults.crashAtSync, key, got, k.ackedValue, k.failedValue)
				}
			}
			allKeys, err := reopened.Keys()
			if err != nil {
				t.Fatalf("seed %d: Keys: %v", seed, err)
			}
			for _, key := range allKeys {
				if keys[key] == nil {
					t.Fatalf("seed %d: key %s was never written", seed, key)
				}
			}
			if err := reopened.Close(); err != nil {
				t.Fatalf("seed %d: Close reopened: %v", seed, err)
			}
		}
	})
}

// TestGroupCommitBatchesWhatQueuedDuringAnFsync: every write queued while
// an fsync runs goes into the next batch, one Write and one fsync for all
// of them, up to maxBatchRecords.
func TestGroupCommitBatchesWhatQueuedDuringAnFsync(t *testing.T) {
	const n = maxBatchRecords + 500 // the queue holds maxBatchRecords; the rest wait to send
	faults := newWALFaults()
	e := mustOpen(t, t.TempDir(), 64<<20, WithGroupCommit(true), WithWALOpenFile(faults.open))
	defer e.Close()

	release := faults.holdSyncs()
	writes := []<-chan error{putAsync(e, "first", "v")}
	<-faults.syncEntered
	for i := range n {
		writes = append(writes, putAsync(e, fmt.Sprintf("k%04d", i), "v"))
	}
	waitFor(t, "the queue to fill", func() bool { return queued(e)() == maxBatchRecords })
	release()
	for i, done := range writes {
		if err := result(t, "Put", done); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	record, err := wal.Encode(wal.Entry{Key: "k0000", Item: sampleItem("v")})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	faults.mu.Lock()
	batches := append([]int(nil), faults.batchBytes...)
	faults.mu.Unlock()
	if len(batches) < 3 {
		t.Fatalf("%d batches, want at least 3: the first write, a full batch, the rest", len(batches))
	}
	if batches[1] != maxBatchRecords*len(record) {
		t.Fatalf("second batch was %d bytes, want %d records of %d bytes: everything queued", batches[1], maxBatchRecords, len(record))
	}
	total := 0
	for _, b := range batches[1:] {
		if b > maxBatchRecords*len(record) {
			t.Fatalf("a batch of %d bytes holds more than %d records", b, maxBatchRecords)
		}
		total += b
	}
	if total != n*len(record) {
		t.Fatalf("batches after the first hold %d bytes, want %d", total, n*len(record))
	}
}

// TestGroupCommitCapsABatchsBytes: a batch stops before a record that would
// take it past maxBatchBytes, which goes into the next one, and a record
// larger than maxBatchBytes is a batch of its own.
func TestGroupCommitCapsABatchsBytes(t *testing.T) {
	faults := newWALFaults()
	e := mustOpen(t, t.TempDir(), 64<<20, WithGroupCommit(true), WithWALOpenFile(faults.open))
	defer e.Close()

	third := strings.Repeat("x", maxBatchBytes/3) // three of these, with headers, overflow a batch
	huge := strings.Repeat("y", 2*maxBatchBytes)
	release := faults.holdSyncs()
	writes := []<-chan error{putAsync(e, "first", "v")}
	<-faults.syncEntered
	keys := []string{"a", "b", "c", "huge", "d"}
	for i, key := range keys {
		value := third
		if key == "huge" {
			value = huge
		}
		writes = append(writes, putAsync(e, key, value))
		waitFor(t, "the write to queue", func() bool { return queued(e)() == i+1 }) // keep their order
	}
	release()
	for i, done := range writes {
		if err := result(t, "Put", done); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	size := func(value string) int {
		r, err := wal.Encode(wal.Entry{Key: "a", Item: sampleItem(value)})
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		return len(r)
	}
	faults.mu.Lock()
	got := append([]int(nil), faults.batchBytes[1:]...)
	faults.mu.Unlock()
	hugeSize := size(huge) + len("huge") - len("a")
	want := []int{2 * size(third), size(third), hugeSize, size(third)}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("batches of %v bytes, want %v: [a b] [c] [huge] [d]", got, want)
	}
}

// TestGroupCommitAddsNoAllocationsPerWrite: a Put through the committer
// allocates no more than one that commits itself.
func TestGroupCommitAddsNoAllocationsPerWrite(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector's instrumentation allocates")
	}
	allocs := map[string]float64{}
	for _, m := range writeModes {
		e := mustOpen(t, t.TempDir(), 64<<20, m.opt)
		item := sampleItem("v")
		allocs[m.name] = testing.AllocsPerRun(200, func() {
			if err := e.Put("k", item); err != nil {
				t.Fatalf("Put: %v", err)
			}
		})
		if err := e.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
	if allocs["group"] > allocs["serial"] {
		t.Fatalf("a Put allocates %v times with group commit, %v without", allocs["group"], allocs["serial"])
	}
}
