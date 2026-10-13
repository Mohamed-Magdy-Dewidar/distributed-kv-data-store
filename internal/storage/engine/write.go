package engine

import (
	"errors"
	"fmt"
	"sync"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/storage/memtable"
	"distributed-kv-datastore/internal/storage/wal"
	"distributed-kv-datastore/internal/store"
)

// How writes reach the WAL. Every write (Put or Restore) becomes a
// writeReq: its entry and the record wal.Encode made of it, encoded in the
// writer's own goroutine. commit then writes a batch of them with one
// WriteBatch and one fsync, and only after that fsync succeeds applies
// them to the memtable, in order, so a write is never visible before it is
// durable. commit always runs under writeMu, which keeps a write's segment
// and memtable together across a swap (see writeMu).
//
// With group commit (the default) one committer goroutine per engine runs
// commit. A batch is the request the committer was waiting for plus every
// request queued meanwhile, typically during the previous fsync, up to
// maxBatchRecords records or maxBatchBytes bytes: no timer, no size target,
// so a lone writer waits for nothing but its own fsync. Without it, each
// write takes writeMu and commits itself alone, as before group commit.
//
// A failed WAL write or fsync halts writes for good: every request in the
// failed batch gets the error, none is applied, and every later write
// fails with ErrWritesHalted without touching the WAL, until the engine is
// reopened. After a failed fsync the file's contents are unknown, and an
// fsync retried after a failure can report success for data the kernel
// already dropped; after a failed or short Write, a torn record may sit at
// the end of the segment, and records appended after it would be lost at
// the next replay, which stops at the first torn record. Halting is what
// keeps an acknowledged write from ever landing behind one.

// maxBatchRecords and maxBatchBytes cap one group commit batch, so one
// batch's Write, and the wait of the writes queued behind it, stay
// bounded. A record larger than maxBatchBytes is a batch of its own.
const (
	maxBatchRecords = 1000
	maxBatchBytes   = 1 << 20
)

// ErrWritesHalted is what every write returns once a WAL write or fsync
// has failed, wrapped with that failure. Reads still work. Reopening the
// engine (restarting the node) replays the WAL and accepts writes again.
var ErrWritesHalted = errors.New("engine: writes halted after a failed WAL write or fsync; reopen the engine")

// ErrClosed is what a write returns once Close has begun.
var ErrClosed = errors.New("engine: closed")

// writeReq is one write on its way to the WAL. done is buffered, so commit
// never waits for a writer; a writer waits on done for err. Requests are
// pooled: a writer returns its own once it has read err.
type writeReq struct {
	entry  wal.Entry
	record []byte
	err    error
	done   chan struct{}
}

var reqPool = sync.Pool{New: func() any { return &writeReq{done: make(chan struct{}, 1)} }}

// write durably writes entry and applies it to the memtable, by whichever
// path WithGroupCommit chose. Its error is the request's own: the WAL's
// (wrapped with entry's key), ErrWritesHalted, ErrClosed, or Restore's
// verification result.
func (e *StorageEngine) write(entry wal.Entry) error {
	if err := e.haltErr(); err != nil {
		return fmt.Errorf("engine: write of key %q: %w", entry.Key, err)
	}
	record, err := wal.Encode(entry)
	if err != nil {
		return fmt.Errorf("engine: encode write of key %q: %w", entry.Key, err)
	}

	req := reqPool.Get().(*writeReq)
	req.entry, req.record, req.err = entry, record, nil
	defer func() {
		*req = writeReq{done: req.done}
		reqPool.Put(req)
	}()

	if e.groupCommit {
		if !e.enqueue(req) {
			return fmt.Errorf("engine: write of key %q: %w", entry.Key, ErrClosed)
		}
		<-req.done
		return req.err
	}

	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	if e.writesClosed {
		return fmt.Errorf("engine: write of key %q: %w", entry.Key, ErrClosed)
	}
	e.commit([]*writeReq{req})
	<-req.done
	return req.err
}

// enqueue hands req to the committer, or reports false once Close has
// begun. It holds queueMu for reading while it sends, so Close, which
// takes it for writing before closing the queue, never closes it under a
// send; a send blocked on a full queue still completes, since the
// committer keeps draining it.
func (e *StorageEngine) enqueue(req *writeReq) bool {
	e.queueMu.RLock()
	defer e.queueMu.RUnlock()
	if e.queueClosed {
		return false
	}
	e.queue <- req
	return true
}

// runCommitter commits batches from e.queue until Close closes it and the
// requests already queued are committed.
func (e *StorageEngine) runCommitter() {
	defer close(e.committerDone)
	batch := make([]*writeReq, 0, maxBatchRecords)
	var carried *writeReq // received, but it would have overflowed the last batch
	for {
		first := carried
		carried = nil
		if first == nil {
			var ok bool
			if first, ok = <-e.queue; !ok {
				return
			}
		}
		batch = append(batch[:0], first)
		size := len(first.record)
	fill:
		for len(batch) < maxBatchRecords {
			select {
			case req, ok := <-e.queue:
				if !ok {
					break fill
				}
				if size+len(req.record) > maxBatchBytes {
					carried = req
					break fill
				}
				batch = append(batch, req)
				size += len(req.record)
			default:
				break fill // nothing more queued: commit what there is
			}
		}

		e.writeMu.Lock()
		e.commit(batch)
		e.writeMu.Unlock()
		clear(batch) // drop the pooled requests: their writers own them again
	}
}

// commit writes batch to the WAL with one fsync and, if that succeeded,
// applies it to the memtable and swaps the memtable out for flushing if it
// crossed its threshold. Then it signals every request. The caller holds
// writeMu.
func (e *StorageEngine) commit(batch []*writeReq) {
	defer func() {
		for _, req := range batch {
			req.done <- struct{}{}
		}
	}()

	if err := e.haltErr(); err != nil { // queued before an earlier batch failed
		for _, req := range batch {
			req.err = fmt.Errorf("engine: write of key %q: %w", req.entry.Key, err)
		}
		return
	}

	records := e.records[:0]
	for _, req := range batch {
		records = append(records, req.record)
	}
	err := e.wal.WriteBatch(records)
	clear(records)
	e.records = records[:0]
	if err != nil {
		e.halt(err)
		for _, req := range batch {
			req.err = walError(req.entry, err)
		}
		return
	}

	if testHookAfterWALAppend != nil {
		for _, req := range batch {
			testHookAfterWALAppend(req.entry.Key)
		}
	}
	if e.apply(batch) {
		e.swap()
	}
}

func walError(entry wal.Entry, err error) error {
	if entry.Op == wal.OpRestore {
		return fmt.Errorf("engine: wal append for restore of key %q: %w", entry.Key, err)
	}
	return fmt.Errorf("engine: wal append for key %q: %w", entry.Key, err)
}

// apply applies batch to the active memtable in WAL order, setting each
// Restore's verification result as its error (see Restore), and reports
// whether the memtable should be swapped out for flushing: a Put crossed
// its threshold and no flush is running. The caller holds writeMu.
//
// It holds e.mu for the whole batch, so a reader sees all of it or none.
// A Restore's check reads the SSTables, so with one in the batch it also
// holds filesMu, as Restore always has, so compaction can't delete a file
// it reads.
func (e *StorageEngine) apply(batch []*writeReq) (swap bool) {
	for _, req := range batch {
		if req.entry.Op == wal.OpRestore {
			e.filesMu.RLock()
			defer e.filesMu.RUnlock()
			break
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	crossed := false
	for _, req := range batch {
		switch req.entry.Op {
		case wal.OpRestore:
			req.err = e.replaceAndVerify(req.entry.Key, req.entry.Items)
		default:
			if e.active.Put(req.entry.Key, req.entry.Item) {
				crossed = true
			}
		}
	}
	return crossed && e.flushing == nil
}

// replaceAndVerify is Restore's memtable step. The caller holds e.mu and
// filesMu for reading.
func (e *StorageEngine) replaceAndVerify(key string, items []*model.DataItem) error {
	e.active.Replace(key, items)
	visible, _, err := mergeLayers(key, e.active, e.flushing, e.sstables)
	if err != nil {
		return fmt.Errorf("engine: verify restore of key %q: %w", key, err)
	}
	if !sameVersions(visible, items) {
		return fmt.Errorf("engine: restore of key %q: %w", key, store.ErrRestoreIncomplete)
	}
	return nil
}

// swap seals the WAL's current segment and freezes the active memtable for
// a background flush, so the sealed segments hold exactly what the frozen
// memtable (plus anything older put back into it) holds. The caller holds
// writeMu, so no write lands between the two, and only commit, under
// writeMu, ever sets e.flushing, so it is still nil here. If rotation
// fails, nothing is frozen: the memtable keeps growing and the next write
// that crosses tries again. Rotation runs outside e.mu so readers aren't
// blocked on file creation.
func (e *StorageEngine) swap() {
	sealed, err := e.wal.Rotate()
	if err != nil {
		return // the writes themselves are durable; the swap waits for the next crossing
	}

	e.mu.Lock()
	e.flushing = e.active
	e.active = memtable.New(e.maxBytes)
	frozen := e.flushing
	e.flushWG.Add(1)
	e.mu.Unlock()

	go e.flush(frozen, sealed)
}

// halt records the first WAL failure; every write from now on fails with
// it (see ErrWritesHalted). The caller holds writeMu.
func (e *StorageEngine) halt(cause error) {
	if e.halted.Load() {
		return
	}
	e.haltCause = fmt.Errorf("%w: %w", ErrWritesHalted, cause)
	e.halted.Store(true) // after haltCause: a reader that sees true sees it
}

// haltErr returns the error writes fail with once halted, or nil.
func (e *StorageEngine) haltErr() error {
	if !e.halted.Load() {
		return nil
	}
	return e.haltCause
}

// closeWrites makes every write from now on fail with ErrClosed, and
// returns once every write already accepted has been committed or failed:
// with group commit, once the committer has drained the queue and exited.
// Safe to call more than once.
func (e *StorageEngine) closeWrites() {
	if !e.groupCommit {
		e.writeMu.Lock()
		e.writesClosed = true
		e.writeMu.Unlock()
		return
	}
	e.queueMu.Lock()
	if !e.queueClosed {
		e.queueClosed = true
		close(e.queue)
	}
	e.queueMu.Unlock()
	<-e.committerDone
}
