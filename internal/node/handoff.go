package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/storage/fsutil"
)

// Handoff moves data to where a new membership says it belongs. When the view
// changes, some keys have new owners; the nodes that hold those keys push
// them to the owners that lack them. Nothing is ever deleted: a node that
// stops owning a key keeps its copy.
//
// handoffBase is the last view whose handoff completed. A handoff always runs
// from handoffBase to the newest view, and a newer view cancels the running
// one without moving the base — so if v2's handoff is cut short by v3, keys
// that changed owners only between the base and v2 still reach their v3 owners.

// HandoffStatus describes the node's handoff for the newest view it has.
type HandoffStatus struct {
	Epoch   uint64 `json:"epoch"`   // the view being handed off to (or last handed off to)
	Done    bool   `json:"done"`    // every key that had to move has been pushed or hinted
	Pushed  int    `json:"pushed"`  // pushes to a new owner that succeeded
	Hinted  int    `json:"hinted"`  // pushes that failed and were turned into hints instead
	Pending int    `json:"pending"` // keys not yet handled
}

var (
	// handoffRetryBase and handoffRetryMax bound the backoff between retries
	// of a push that must not be given up on. Variables so tests can shorten
	// them.
	handoffRetryBase = 100 * time.Millisecond
	handoffRetryMax  = 2 * time.Second

	// testHookHandoffKey, when set, runs in a handoff before each key it
	// moves. Tests only: it lets one pause or interrupt a handoff. Always nil
	// in production.
	testHookHandoffKey func(ctx context.Context, nodeID string, epoch uint64, key string)
)

// HandoffStatus returns the state of the handoff to the newest view.
func (n *Node) HandoffStatus() HandoffStatus {
	n.handoffMu.Lock()
	defer n.handoffMu.Unlock()
	return n.handoffStatus
}

// WaitHandoff blocks until a handoff to a view of epoch or later has
// completed on this node, or ctx ends.
func (n *Node) WaitHandoff(ctx context.Context, epoch uint64) error {
	for {
		n.handoffMu.Lock()
		if n.handoffBase.epoch >= epoch {
			n.handoffMu.Unlock()
			return nil
		}
		changed := n.handoffChanged
		n.handoffMu.Unlock()

		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// ResumeHandoff starts the handoff to the current view if the last completed
// one is older — after a restart, for instance. It does nothing if the node's
// data is already where the current view puts it, or a handoff to the current
// view is already running. Call it once the node's configuration is in place;
// SetMembership starts handoffs itself.
func (n *Node) ResumeHandoff() { n.startHandoff() }

// startHandoff (re)starts the single handoff worker from handoffBase to the
// newest view, canceling one running for an older view.
func (n *Node) startHandoff() {
	n.handoffMu.Lock()
	defer n.handoffMu.Unlock()

	newest := n.membership.Load()
	base := n.handoffBase
	if base.epoch >= newest.epoch {
		return
	}
	if n.handoffRunning && n.handoffTarget == newest {
		return
	}
	if n.handoffCancel != nil {
		n.handoffCancel()
	}
	n.handoffGen++
	gen := n.handoffGen
	n.handoffTarget = newest
	n.handoffStatus = HandoffStatus{Epoch: newest.epoch}

	runCtx, cancel := context.WithCancel(context.Background())
	started := n.goBackground(func(bg context.Context) {
		stop := context.AfterFunc(bg, cancel)
		defer stop()
		defer cancel()
		n.runHandoff(runCtx, gen, base, newest)
	})
	if !started {
		cancel()
		return
	}
	n.handoffCancel = cancel
	n.handoffRunning = true
}

// handoffTargets returns the nodes key must be pushed to when the view moves
// from base to newest, from this node's point of view.
//
//   - If this node stays an owner of key, the new owners that weren't owners
//     before: the rest already hold their copy.
//   - If it no longer owns key, every new owner: this node may hold the only
//     copy, and nobody else can know what it has.
//
// This node itself is never a target.
func (n *Node) handoffTargets(base, newest *view, key string) []string {
	newOwners := newest.ring.GetPreferenceList(key, n.QuorumConfig.N)
	stays := slices.Contains(newOwners, n.ID)

	var oldOwners []string
	if stays {
		oldOwners = base.ring.GetPreferenceList(key, n.QuorumConfig.N)
	}
	var targets []string
	for _, id := range newOwners {
		if id != n.ID && !slices.Contains(oldOwners, id) {
			targets = append(targets, id)
		}
	}
	return targets
}

type handoffItem struct {
	key     string
	targets []string
}

// runHandoff pushes every local key that has new owners between base and
// newest, then records base as newest. It returns early, recording nothing,
// if ctx is canceled: the base stays where it was.
func (n *Node) runHandoff(ctx context.Context, gen uint64, base, newest *view) {
	keys, ok := n.handoffKeys(ctx)
	if !ok {
		return
	}
	var plan []handoffItem
	for _, key := range keys {
		if targets := n.handoffTargets(base, newest, key); len(targets) > 0 {
			plan = append(plan, handoffItem{key, targets})
		}
	}
	n.updateHandoff(gen, func(s *HandoffStatus) { s.Pending = len(plan) })

	for _, item := range plan {
		if ctx.Err() != nil {
			return
		}
		if testHookHandoffKey != nil {
			testHookHandoffKey(ctx, n.ID, newest.epoch, item.key)
			if ctx.Err() != nil {
				return
			}
		}
		if !n.handoffKey(ctx, gen, newest, item) {
			return
		}
		n.updateHandoff(gen, func(s *HandoffStatus) { s.Pending-- })
	}
	n.finishHandoff(gen, newest)
}

// handoffKeys lists the local keys, retrying a failed listing until it works
// or ctx ends.
func (n *Node) handoffKeys(ctx context.Context) ([]string, bool) {
	for backoff := handoffRetryBase; ; backoff = min(2*backoff, handoffRetryMax) {
		keys, err := n.Store.Keys()
		if err == nil {
			return keys, true
		}
		log.Printf("node %s: handoff: listing keys failed, retrying: %v", n.ID, err)
		if !sleepCtx(ctx, backoff) {
			return nil, false
		}
	}
}

// handoffKey pushes one key's full sibling set to each of its targets, in
// parallel. It reports false only if ctx ended first.
func (n *Node) handoffKey(ctx context.Context, gen uint64, newest *view, item handoffItem) bool {
	var siblings []*model.DataItem
	for backoff := handoffRetryBase; ; backoff = min(2*backoff, handoffRetryMax) {
		items, found, err := n.Store.Get(item.key)
		if err == nil {
			if !found {
				return true // nothing left to hand off
			}
			siblings = items
			break
		}
		log.Printf("node %s: handoff: reading %q failed, retrying: %v", n.ID, item.key, err)
		if !sleepCtx(ctx, backoff) {
			return false
		}
	}

	var wg sync.WaitGroup
	for _, target := range item.targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n.handoffPush(ctx, gen, newest, item.key, siblings, target)
		}()
	}
	wg.Wait()
	return ctx.Err() == nil
}

// handoffPush delivers siblings to target. A push that fails is handled by
// where this node stands in the newest view:
//
//   - a node that stays in the view (and can keep hints) stores a hint and
//     moves on: hint delivery will get it there when target is back, and this
//     node is around to do it;
//   - a node that is leaving, or can't keep hints, retries with backoff until
//     the push succeeds, because nobody else will ever deliver it.
//
// It returns early only if ctx ends.
func (n *Node) handoffPush(ctx context.Context, gen uint64, newest *view, key string, siblings []*model.DataItem, target string) {
	_, staysInView := newest.members[n.ID]
	for backoff := handoffRetryBase; ; backoff = min(2*backoff, handoffRetryMax) {
		err := n.pushOnce(ctx, newest, target, key, siblings)
		if err == nil {
			n.updateHandoff(gen, func(s *HandoffStatus) { s.Pushed++ })
			return
		}
		if ctx.Err() != nil {
			return
		}
		if staysInView {
			herr := n.addHints(target, key, siblings)
			if herr == nil {
				n.updateHandoff(gen, func(s *HandoffStatus) { s.Hinted++ })
				return
			}
			log.Printf("node %s: handoff: could not store a hint for %q (key %q): %v", n.ID, target, key, herr)
		}
		if !sleepCtx(ctx, backoff) {
			return
		}
	}
}

func (n *Node) pushOnce(ctx context.Context, newest *view, target, key string, siblings []*model.DataItem) error {
	if n.isDead(target) {
		return errPeerDead(target)
	}
	pushCtx, cancel := context.WithTimeout(ctx, n.QuorumConfig.ReplicationTimeout)
	defer cancel()
	return n.replicate(pushCtx, newest, target, key, siblings)
}

func (n *Node) addHints(target, key string, siblings []*model.DataItem) error {
	for _, item := range siblings {
		if err := n.hints.Add(target, key, item); err != nil {
			return err
		}
	}
	return nil
}

// sleepCtx waits d or until ctx ends; it reports whether the full wait passed.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (n *Node) updateHandoff(gen uint64, update func(*HandoffStatus)) {
	n.handoffMu.Lock()
	defer n.handoffMu.Unlock()
	if gen == n.handoffGen {
		update(&n.handoffStatus)
	}
}

// finishHandoff records newest as the base — durably first, for a persistent
// node — wakes WaitHandoff, and asks for an anti-entropy round to settle
// anything the push missed. A run that was superseded meanwhile records
// nothing.
func (n *Node) finishHandoff(gen uint64, newest *view) {
	n.handoffMu.Lock()
	defer n.handoffMu.Unlock()
	if gen != n.handoffGen {
		return
	}
	if err := n.persistHandoff(newest); err != nil {
		// The data has moved; only the record is missing, and repeating a
		// handoff after a restart is harmless. Carry on in memory.
		log.Printf("node %s: could not persist handoff to epoch %d: %v", n.ID, newest.epoch, err)
	}
	n.handoffBase = newest
	n.handoffRunning = false
	n.handoffStatus.Done = true
	n.handoffStatus.Pending = 0
	close(n.handoffChanged)
	n.handoffChanged = make(chan struct{})
	n.TriggerAntiEntropy()
}

// handoffFileName is the last completed handoff in the node's data dir.
const handoffFileName = "HANDOFF"

type handoffFile struct {
	Epoch   uint64            `json:"epoch"`
	Members map[string]string `json:"members"`
}

// writeHandoff durably records v as the view whose handoff completed.
func writeHandoff(dataDir string, v *view) error {
	data, err := json.MarshalIndent(handoffFile{Epoch: v.epoch, Members: maps.Clone(v.members)}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	return fsutil.WriteFileAtomic(filepath.Join(dataDir, handoffFileName), data, 0o644)
}

// loadHandoff reads dataDir's HANDOFF file into a view for replication factor
// n. found is false if there is no file. An unreadable or invalid file is an
// error: the node must not guess what it has already handed off.
func loadHandoff(dataDir string, n int) (v *view, found bool, err error) {
	path := filepath.Join(dataDir, handoffFileName)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("handoff: read %s: %w", path, err)
	}
	var f handoffFile
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return nil, false, fmt.Errorf("handoff: %s is corrupt: %w", path, err)
	}
	if err := validateMembers(f.Members, n); err != nil {
		return nil, false, fmt.Errorf("%s: %w", path, err)
	}
	v, err = buildView(f.Epoch, f.Members, n)
	if err != nil {
		return nil, false, fmt.Errorf("handoff: %s: %w", path, err)
	}
	return v, true, nil
}
