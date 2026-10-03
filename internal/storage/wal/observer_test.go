package wal

import (
	"os"
	"sync"
	"testing"
	"time"
)

// recordingObserver records every fsync duration it is told of.
type recordingObserver struct {
	mu    sync.Mutex
	syncs []time.Duration
}

func (o *recordingObserver) WALSynced(d time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.syncs = append(o.syncs, d)
}

func (o *recordingObserver) count() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.syncs)
}

// TestSyncObserverIsToldOfEveryAppend: each Append fsyncs once and reports
// it, with a positive duration, in every segment, before and after a
// rotation.
func TestSyncObserverIsToldOfEveryAppend(t *testing.T) {
	obs := &recordingObserver{}
	l, _, err := OpenLog(t.TempDir(), WithSyncObserver(obs))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	defer l.Close()

	mustAppend(t, l, "a", "b")
	if _, err := l.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	mustAppend(t, l, "c")

	if got := obs.count(); got != 3 {
		t.Fatalf("observer told of %d fsyncs for 3 appends, want 3", got)
	}
	for i, d := range obs.syncs {
		if d <= 0 {
			t.Fatalf("fsync %d reported as %v, want a positive duration", i, d)
		}
	}
}

// TestReplayDoesNotReportSyncs: reopening a log replays its segments,
// truncating a torn tail with an fsync, and reports none of it. The first
// write afterwards is reported.
func TestReplayDoesNotReportSyncs(t *testing.T) {
	dir := t.TempDir()
	l, _ := mustOpenLog(t, dir)
	mustAppend(t, l, "a")
	seq, err := l.Rotate()
	if err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	mustAppend(t, l, "b")
	l.Close()

	f, err := os.OpenFile(segmentPath(dir, seq), os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("open segment %d: %v", seq, err)
	}
	f.Write([]byte{0xFF, 0xFF, 0, 0}) // a torn header: replay truncates it and fsyncs
	f.Close()

	obs := &recordingObserver{}
	l2, entries, err := OpenLog(dir, WithSyncObserver(obs))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	defer l2.Close()
	if len(entries) != 2 {
		t.Fatalf("replayed %d entries, want 2", len(entries))
	}
	if got := obs.count(); got != 0 {
		t.Fatalf("replay reported %d fsyncs, want 0", got)
	}

	mustAppend(t, l2, "c")
	if got := obs.count(); got != 1 {
		t.Fatalf("observer told of %d fsyncs after one append, want 1", got)
	}
}

// discardObserver does nothing, without allocating.
type discardObserver struct{}

func (discardObserver) WALSynced(time.Duration) {}

// TestSyncObserverAddsNoAllocations: an Append reporting to an observer
// allocates exactly as much as one without.
func TestSyncObserverAddsNoAllocations(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector's instrumentation allocates")
	}
	appendAllocs := func(opts ...Option) float64 {
		l, _, err := OpenLog(t.TempDir(), opts...)
		if err != nil {
			t.Fatalf("OpenLog: %v", err)
		}
		defer l.Close()
		entry := sampleEntry("k", "v")
		return testing.AllocsPerRun(50, func() {
			if err := l.Append(entry); err != nil {
				t.Fatalf("Append: %v", err)
			}
		})
	}
	without := appendAllocs()
	with := appendAllocs(WithSyncObserver(discardObserver{}))
	if with != without {
		t.Fatalf("Append allocates %v times with an observer, %v without; want the same", with, without)
	}
}
