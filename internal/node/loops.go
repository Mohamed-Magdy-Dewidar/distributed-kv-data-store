package node

import (
	"context"
	"math/rand/v2"
	"time"
)

// startLoop runs round on a repeating interval until ctx is canceled or
// StopBackgroundLoops (or Close) is called. It does nothing once
// StopBackgroundLoops has run.
//
// The first round runs after a random delay in [0, interval), then one
// every interval. Nodes started together (a whole cluster brought up at
// once) would otherwise run their rounds within milliseconds of each other
// every time; the random offset keeps each node's schedule independent of
// the others', so rounds don't all land on the cluster at once. Canceling
// during that first delay stops the loop at once.
//
// A receive on trigger (a nil trigger never fires) starts a round at once,
// including in place of the first delay, and restarts the interval from then.
// The round always runs on the loop's own goroutine, so a triggered round can
// never overlap a scheduled one.
func (n *Node) startLoop(ctx context.Context, interval time.Duration, round func(context.Context), trigger <-chan struct{}) {
	n.bgMu.Lock()
	defer n.bgMu.Unlock()
	if n.bgStopped {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	n.bgCancels = append(n.bgCancels, cancel)
	n.bgWG.Add(1)

	first := time.NewTimer(rand.N(interval))
	go func() {
		defer n.bgWG.Done()
		defer first.Stop()
		select {
		case <-first.C:
		case <-trigger:
		case <-ctx.Done():
			return
		}

		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			round(ctx)
			select {
			case <-ticker.C:
			case <-trigger:
				ticker.Reset(interval)
			case <-ctx.Done():
				return
			}
		}
	}()
}

// StopBackgroundLoops cancels every background loop n has started —
// anti-entropy and hint delivery — and waits until each has exited,
// including any round in progress: a round's RPCs fail fast once canceled,
// and any local write it's already making (an anti-entropy install, a
// delivered hint's marker) completes first. After it returns, those loops
// make no more calls into n's stores, and startLoop does nothing. Safe to
// call more than once, and when no loop was ever started.
func (n *Node) StopBackgroundLoops() {
	n.bgMu.Lock()
	n.bgStopped = true
	for _, cancel := range n.bgCancels {
		cancel()
	}
	n.bgCancels = nil
	n.bgMu.Unlock()

	n.bgWG.Wait()
}
