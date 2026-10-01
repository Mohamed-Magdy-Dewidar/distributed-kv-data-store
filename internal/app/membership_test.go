package app

import (
	"bytes"
	"context"
	"errors"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"distributed-kv-datastore/internal/config"
	"distributed-kv-datastore/internal/node"
)

// syncBuffer is a log sink safe to write from Run's goroutines and read from
// the test.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func captureLog(t *testing.T) *syncBuffer {
	t.Helper()
	sink := &syncBuffer{}
	log.SetOutput(sink)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	return sink
}

// seedMembership leaves a MEMBERSHIP file at epoch with members in cfg's data
// dir, as an earlier run that had adopted it would have.
func seedMembership(t *testing.T, cfg *config.Config, epoch uint64, members map[string]string) {
	t.Helper()
	nd, err := node.New(cfg.NodeID, cfg.SelfAddress(), cfg.Cluster.N, cfg.Cluster.W, cfg.Cluster.R,
		cfg.NeighborAddrs(), cfg.DataDir, cfg.Storage.MemtableBytes)
	if err != nil {
		t.Fatalf("seed: open node: %v", err)
	}
	if _, err := nd.SetMembership(epoch, members); err != nil {
		nd.Close()
		t.Fatalf("seed: SetMembership: %v", err)
	}
	if err := nd.Close(); err != nil {
		t.Fatalf("seed: close: %v", err)
	}
}

// persistedMembership reopens cfg's data dir and returns the membership the
// node starts with.
func persistedMembership(t *testing.T, cfg *config.Config) (uint64, map[string]string) {
	t.Helper()
	nd, err := node.New(cfg.NodeID, cfg.SelfAddress(), cfg.Cluster.N, cfg.Cluster.W, cfg.Cluster.R,
		cfg.NeighborAddrs(), cfg.DataDir, cfg.Storage.MemtableBytes)
	if err != nil {
		t.Fatalf("reopen node: %v", err)
	}
	defer nd.Close()
	return nd.Membership()
}

// runUntilReadyThenStop runs cfg until the node is ready and then shuts it
// down, returning Run's error. If Run fails before becoming ready, that is
// what it returns.
func runUntilReadyThenStop(t *testing.T, cfg *config.Config) error {
	t.Helper()
	ready := make(chan struct{})
	var once sync.Once
	testHookStep = func(s string) {
		if s == "ready" {
			once.Do(func() { close(ready) })
		}
	}
	defer func() { testHookStep = nil }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg) }()

	select {
	case <-ready:
		cancel()
		return <-done
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("Run neither became ready nor failed")
		return nil
	}
}

// (h) The configured epoch equals the persisted one but the members differ:
// startup fails with the conflict.
func TestRunFailsOnConfigContradictingPersistedMembershipAtTheSameEpoch(t *testing.T) {
	cfg := testConfig(t, t.TempDir())
	seedMembership(t, cfg, 5, map[string]string{"kv-0": cfg.SelfAddress(), "kv-1": knownAddr()})

	cfg.Cluster.Epoch = 5 // same epoch, but the config lists only kv-0

	err := runUntilReadyThenStop(t, cfg)
	if !errors.Is(err, node.ErrMembershipConflict) {
		t.Fatalf("expected Run to fail with the membership conflict, got %v", err)
	}
	if !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("expected the error to name the conflict, got %q", err)
	}
	if epoch, members := persistedMembership(t, cfg); epoch != 5 || len(members) != 2 {
		t.Fatalf("the persisted membership changed: epoch %d %v", epoch, members)
	}
}

// (h) The configured epoch is older than the persisted one: the node starts
// and keeps what it persisted.
func TestRunKeepsPersistedMembershipWhenConfigEpochIsLower(t *testing.T) {
	logs := captureLog(t)
	cfg := testConfig(t, t.TempDir())
	absent := knownAddr() // kv-1 is only recorded, never started
	persisted := map[string]string{"kv-0": cfg.SelfAddress(), "kv-1": absent}
	seedMembership(t, cfg, 5, persisted)

	cfg.Cluster.Epoch = 3

	if err := runUntilReadyThenStop(t, cfg); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if want := "persisted membership epoch 5 is newer than the config's cluster.epoch 3"; !strings.Contains(logs.String(), want) {
		t.Fatalf("expected a log line containing %q, got:\n%s", want, logs.String())
	}
	epoch, members := persistedMembership(t, cfg)
	if epoch != 5 || len(members) != 2 || members["kv-1"] != absent {
		t.Fatalf("expected the persisted membership (epoch 5, kv-0 and kv-1) to be kept, got epoch %d %v", epoch, members)
	}
}

// A configured epoch newer than the persisted one is adopted and persisted.
func TestRunAdoptsAndPersistsAHigherConfigEpoch(t *testing.T) {
	cfg := testConfig(t, t.TempDir())
	seedMembership(t, cfg, 5, map[string]string{"kv-0": cfg.SelfAddress(), "kv-1": knownAddr()})

	cfg.Cluster.Epoch = 7

	if err := runUntilReadyThenStop(t, cfg); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	epoch, members := persistedMembership(t, cfg)
	if epoch != 7 || len(members) != 1 || members["kv-0"] != cfg.SelfAddress() {
		t.Fatalf("expected epoch 7 with only kv-0, got epoch %d %v", epoch, members)
	}
}

// A config whose member list is invalid for the node's N fails startup.
func TestRunFailsOnInvalidConfiguredMembership(t *testing.T) {
	cfg := testConfig(t, t.TempDir())
	cfg.Cluster.N, cfg.Cluster.W, cfg.Cluster.R = 2, 1, 1 // but only one member

	if err := runUntilReadyThenStop(t, cfg); err == nil || !strings.Contains(err.Error(), "replication factor") {
		t.Fatalf("expected Run to fail on fewer members than N, got %v", err)
	}
}
