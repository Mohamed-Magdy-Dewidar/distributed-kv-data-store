package node

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// aeRoundHook installs testHookAntiEntropyRound for one test.
func aeRoundHook(t *testing.T, hook func(start bool)) {
	t.Helper()
	testHookAntiEntropyRound = hook
	t.Cleanup(func() { testHookAntiEntropyRound = nil })
}

func aloneNode(t *testing.T) *Node {
	return newTestNode(t, "node-1", "unused", 1, 1, 1, nil)
}

// A trigger starts a round well before the interval (here an hour).
func TestTriggerAntiEntropyStartsARoundBeforeTheInterval(t *testing.T) {
	var rounds atomic.Int32
	aeRoundHook(t, func(start bool) {
		if start {
			rounds.Add(1)
		}
	})
	nd := aloneNode(t)
	nd.StartAntiEntropyLoop(context.Background(), time.Hour)
	t.Cleanup(nd.StopBackgroundLoops)

	nd.TriggerAntiEntropy()
	eventually(t, time.Second, "a triggered round", func() bool { return rounds.Load() >= 1 })

	nd.TriggerAntiEntropy()
	eventually(t, time.Second, "a second triggered round", func() bool { return rounds.Load() >= 2 })
}

// Rapid triggers, racing scheduled rounds, never make two rounds run at once.
func TestTriggeredRoundsNeverOverlapScheduledOnes(t *testing.T) {
	var active, maxActive, rounds atomic.Int32
	aeRoundHook(t, func(start bool) {
		if !start {
			active.Add(-1)
			return
		}
		rounds.Add(1)
		cur := active.Add(1)
		for {
			m := maxActive.Load()
			if cur <= m || maxActive.CompareAndSwap(m, cur) {
				break
			}
		}
		time.Sleep(15 * time.Millisecond) // long enough for an overlapping round to be seen
	})
	nd := aloneNode(t)
	nd.StartAntiEntropyLoop(context.Background(), 20*time.Millisecond)
	t.Cleanup(nd.StopBackgroundLoops)

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 300 {
				nd.TriggerAntiEntropy()
				time.Sleep(time.Millisecond)
			}
		}()
	}
	wg.Wait()
	nd.StopBackgroundLoops()

	if rounds.Load() < 5 {
		t.Fatalf("only %d rounds ran; the test did not exercise the loop", rounds.Load())
	}
	if got := maxActive.Load(); got != 1 {
		t.Fatalf("up to %d rounds ran at once, want 1", got)
	}
}

// Triggers that arrive while a round is running coalesce into one more round.
func TestTriggersCoalesce(t *testing.T) {
	var rounds atomic.Int32
	entered := make(chan struct{}, 8)
	release := make(chan struct{})
	aeRoundHook(t, func(start bool) {
		if start {
			rounds.Add(1)
			entered <- struct{}{}
			if rounds.Load() == 1 {
				<-release
			}
		}
	})
	nd := aloneNode(t)
	nd.StartAntiEntropyLoop(context.Background(), time.Hour)
	t.Cleanup(nd.StopBackgroundLoops)

	nd.TriggerAntiEntropy()
	<-entered // round 1 is running and held
	for range 200 {
		nd.TriggerAntiEntropy() // none of these may block
	}
	close(release)
	<-entered // the one coalesced round
	time.Sleep(200 * time.Millisecond)
	if got := rounds.Load(); got != 2 {
		t.Fatalf("%d rounds ran, want the running one plus exactly one coalesced", got)
	}
}
