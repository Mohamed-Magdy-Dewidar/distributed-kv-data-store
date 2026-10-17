package node

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// StartHintDeliveryLoop delivers n's pending hints every interval (see
// deliverHints, and startLoop for scheduling and stopping).
func (n *Node) StartHintDeliveryLoop(ctx context.Context, interval time.Duration) {
	n.startLoop(ctx, interval, n.deliverHints, nil)
}

// testHookBeforeDeliveringHint and testHookAfterDeliveringHint, when set,
// run in deliverHintsTo before each hint's Replicate RPC, and after one
// succeeds but before the hint is marked delivered. Tests only. Always nil
// in production.
var (
	testHookBeforeDeliveringHint func(target, key string)
	testHookAfterDeliveringHint  func(target, key string)
)

// deliverHints is one hint-delivery round: every target n holds hints for,
// in turn (see deliverHintsTo). Failures are logged; whatever wasn't
// delivered stays pending for the next round. Once ctx is canceled it
// starts no further target.
//
// Targets that aren't in the current view are skipped: there is nowhere to
// send their hints (and the store can't delete, so they stay). So are
// targets heartbeats have marked dead: they are retried once they answer
// again. A round that runs to the end then counts the hints still pending,
// for every target (see recordPendingHints).
func (n *Node) deliverHints(ctx context.Context) {
	targets, err := n.hints.Targets()
	if err != nil {
		log.Printf("node %s: hint delivery: %v", n.ID, err)
		return
	}
	v := n.membership.Load()
	for _, target := range targets {
		if ctx.Err() != nil {
			return
		}
		if _, member := v.members[target]; !member || n.isDead(target) {
			continue
		}
		if err := n.deliverHintsTo(ctx, target); err != nil {
			log.Printf("node %s: delivering hints to %q: %v", n.ID, target, err)
		}
	}
	n.recordPendingHints()
}

// recordPendingHints counts the hinted items still undelivered, for every
// target (those skipped this round included), as Stats.HintsPending. Hint
// delivery calls it at the end of each round it completes, so the count is
// as of the last round, not live.
func (n *Node) recordPendingHints() {
	pending, err := n.hints.PendingItems()
	if err != nil {
		log.Printf("node %s: counting pending hints: %v", n.ID, err)
		return
	}
	n.hintsPending.Store(uint64(pending))
	n.hintsPendingKnown.Store(true)
}

// deliverHintsTo sends target each of its pending hints with the ordinary
// Replicate RPC — target merges them like any replicated write — and marks
// each delivered once target has accepted it. Each RPC is bounded by
// ReplicationTimeout.
//
// Unavailable (target still unreachable) or DeadlineExceeded (target not
// answering) ends target's turn this round: every other hint would only
// fail the same way, one timeout at a time. Any other error is logged and
// that hint kept, and delivery moves on to the next one.
func (n *Node) deliverHintsTo(ctx context.Context, target string) error {
	pending, err := n.hints.Pending(target)
	if err != nil {
		return err
	}
	for _, h := range pending {
		if ctx.Err() != nil {
			return nil
		}
		if testHookBeforeDeliveringHint != nil {
			testHookBeforeDeliveringHint(target, h.Key)
		}
		rpcCtx, cancel := context.WithTimeout(ctx, n.QuorumConfig.ReplicationTimeout)
		err := n.Replicate(rpcCtx, target, h.Key, h.Items)
		cancel()
		switch code := status.Code(err); {
		case code == codes.Unavailable || code == codes.DeadlineExceeded:
			return nil // still down; next round retries
		case err != nil:
			log.Printf("node %s: delivering hint for key %q to %q failed; kept for retry: %v", n.ID, h.Key, target, err)
			continue
		}
		if testHookAfterDeliveringHint != nil {
			testHookAfterDeliveringHint(target, h.Key)
		}
		// Even if ctx was canceled meanwhile: target has the hint.
		if err := n.hints.MarkDelivered(target, h.Key, h.Items); err != nil {
			return err // delivered but still pending: redelivered next round, harmlessly
		}
	}
	return nil
}
