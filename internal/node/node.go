package node

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"distributed-kv-datastore/internal/hints"
	"distributed-kv-datastore/internal/identity"
	"distributed-kv-datastore/internal/rpc"
	"distributed-kv-datastore/internal/storage/engine"
	"distributed-kv-datastore/internal/store"
)

// defaultVirtualNodesPerPhysical is kept at 150 (not bumped to something
// like 500) as a deliberate tradeoff, not an oversight: this project's
// current goal is proving partitioning/routing mechanics work correctly,
// not delivering tight load-balancing. Measured relative deviation in
// per-node primary-key share at 150 vnodes ranges roughly -58% to +91%
// depending on physical node count (see
// internal/hashring.TestNoNodeIsCatastrophicallyOverOrUnderloaded) — a
// real, known limitation of this default, not a bug. Revisit if load
// fairness becomes a goal; 500 vnodes measured closer to ±30-40%.
const defaultVirtualNodesPerPhysical = 150

// VirtualNodesPerPhysical is the vnode count every node's ring uses. Code
// that computes placement outside a node (tests, tools) must use the same
// value, with hashring.NewHashRingFromMembers, to agree with the nodes.
const VirtualNodesPerPhysical = defaultVirtualNodesPerPhysical

type Node struct {
	// ID is the node's identity in the ring, the membership, hint targets and
	// peer connections. Its writes are versioned under clockID instead: see
	// ClockID.
	ID           string
	clockID      string
	Store        *store.DataStore
	Address      string
	QuorumConfig QuorumConfig

	// membership is the node's current view of the cluster: who the members
	// are, where they listen, and the hash ring built from them. Readers load
	// it once per operation and use that one snapshot throughout; a published
	// view is never mutated (see view).
	membership atomic.Pointer[view]

	// membershipMu serializes SetMembership. persistView durably stores a
	// view before SetMembership publishes it.
	membershipMu sync.Mutex
	persistView  func(*view) error

	// Handoff state (see handoff.go), guarded by handoffMu. handoffBase is the
	// last view whose handoff completed; persistHandoff records it durably.
	handoffMu      sync.Mutex
	handoffBase    *view
	handoffTarget  *view // the newest view a handoff was started for
	handoffCancel  context.CancelFunc
	handoffGen     uint64
	handoffRunning bool
	handoffStatus  HandoffStatus
	handoffChanged chan struct{} // closed and replaced whenever a handoff completes
	persistHandoff func(*view) error
	// handoffStarted is when the running (or last) handoff started;
	// handoffLast and handoffTotal are the durations of the last completed
	// handoff and of every completed one, handoffsCompleted their number.
	handoffStarted    time.Time
	handoffLast       time.Duration
	handoffTotal      time.Duration
	handoffsCompleted uint64

	// Heartbeat state (see heartbeat.go). health holds what pings have shown
	// about each peer that has been pinged; a peer with no entry is alive.
	// conflictSeen remembers the last membership conflict logged per peer,
	// and mismatchSeen the last wrong address a peer's ping claimed.
	// pingingBack holds the peers a ping-back is running for (see pingBack).
	// adopting makes fetching-and-adopting a newer membership single-flight.
	aeTrigger           chan struct{} // capacity 1: see TriggerAntiEntropy
	healthMu            sync.RWMutex
	health              map[string]*peerHealth
	conflictSeen        map[string]string
	mismatchSeen        map[string]string
	pingingBack         map[string]bool
	adopting            atomic.Bool
	membershipConflicts atomic.Uint64 // conflicts noticed, for tests and diagnosis
	pingsReceived       atomic.Uint64

	// clients caches one connection per peer, with the address it was dialed
	// at. A connection whose peer's address has since changed is moved to
	// retired, not closed: an in-flight RPC (an anti-entropy round holds one
	// for its whole run) may still be using it. Close closes both. Once
	// clientsClosed is set, nothing dials. All guarded by clientsMu.
	clientsMu     sync.Mutex
	clients       map[string]peerClient
	retired       []*rpc.Client
	clientsClosed bool

	engine    *engine.StorageEngine
	hints     *hints.Store
	closeOnce sync.Once
	closeErr  error

	// Background loops — anti-entropy and hint delivery (see startLoop).
	// bgWG counts running loop goroutines — a round runs inline in its
	// loop's goroutine, so waiting on bgWG waits out any round in progress.
	// Once bgStopped is set, no new loop starts.
	bgMu      sync.Mutex
	bgCancels []context.CancelFunc
	bgStopped bool
	bgWG      sync.WaitGroup

	// Replication drains (see drainReplication): one per write whose
	// fan-out outlived its quorum decision. drainWG counts them; once
	// drainClosing is set, no new one starts.
	drainMu      sync.Mutex
	drainClosing bool
	drainWG      sync.WaitGroup

	// Counted where they happen, read by Stats (see stats.go).
	writeQuorumFailures, readQuorumFailures atomic.Uint64
	aeRounds, aePeerFailures                atomic.Uint64
	aeKeysPulled, aeKeysPushed              atomic.Uint64
	aeLastRound, aeRoundTotal               atomic.Int64 // nanoseconds
	// hintsPending is the number of undelivered hinted items as of the end
	// of the last hint-delivery round; hintsPendingKnown is false until a
	// round has recorded it.
	hintsPending      atomic.Uint64
	hintsPendingKnown atomic.Bool
}

// ClockID is the name this node's writes carry in vector clocks: its node ID
// plus an incarnation, "<id>#<16 hex chars>". The incarnation is created on
// the first boot of the node's data directory and kept in its IDENTITY
// file, so it is the same across restarts; a data directory that is replaced
// gets a new one. That keeps a replacement node that reuses an ID from
// restarting the counter of its predecessor's clock entry, which replicas
// would treat as an older write and drop.
func (n *Node) ClockID() string { return n.clockID }

// Node serves the client-facing gRPC API, the coordinator side of forwarded
// writes, and membership; rpc finds each of these by type assertion, so a
// change that stopped satisfying one would otherwise fail only at run time.
var (
	_ rpc.KV                = (*Node)(nil)
	_ rpc.WriteCoordinator  = (*Node)(nil)
	_ rpc.MembershipService = (*Node)(nil)
)

// New builds the node id, with its data kept in dataDir. Its hash ring holds
// itself plus every neighbor in neighborAddrs; every node in the cluster must
// be built with the same set of node IDs for all of them to compute the same
// preference lists.
//
// It opens (or creates) a StorageEngine at dataDir — replaying its WAL and
// loading its live SSTables, so a node reopened on the same directory gets
// its data back — and runs the node's DataStore on top of it. It also opens
// the node's hinted-handoff store, on its own engine at dataDir/hints (which
// the main engine ignores). maxMemtableBytes is both engines' flush
// threshold. opts may give the engines observers (WithStorageObservers).
// The caller must Close the node to close them cleanly.
//
// Once the engine holds the directory's lock, New loads the
// directory's incarnation from its IDENTITY file, or creates and durably
// stores one (see internal/identity) before anything can be written. It
// refuses to start, naming both IDs and the directory, if the file belongs to
// a different node ID.
//
// If the directory has a HANDOFF file, the node's data is taken to be in place
// for that view; if that is older than the current view, ResumeHandoff moves
// the rest. Without one, only the configured view counts as handed off.
//
// If the directory has a MEMBERSHIP file (written by SetMembership), that
// view is the node's initial one instead of the epoch-0 view built from
// neighborAddrs. A MEMBERSHIP file that is corrupt, invalid, or doesn't match
// its own fingerprint (or n) makes New fail rather than guess.
func New(id, address string, n, w, r int, neighborAddrs map[string]string, dataDir string, maxMemtableBytes int, opts ...Option) (*Node, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	v, err := initialView(id, address, neighborAddrs, n)
	if err != nil {
		return nil, fmt.Errorf("node %s: %w", id, err)
	}
	e, err := engine.Open(dataDir, maxMemtableBytes, append([]engine.Option{engine.WithObserver(o.dataObserver)}, o.engineOpts...)...)
	if err != nil {
		return nil, fmt.Errorf("node %s: open storage at %s: %w", id, dataDir, err)
	}
	incarnation, err := identity.LoadOrCreate(dataDir, id)
	if err != nil {
		e.Close()
		return nil, fmt.Errorf("node %s: %w", id, err)
	}
	// A persisted membership replaces the epoch-0 view built from the
	// configuration's member list: it is what this node last agreed to.
	// The last completed handoff is the base for moving data to that view;
	// without a HANDOFF file it is the configuration's view, since nothing
	// has been handed off yet.
	base := v
	if persisted, found, err := loadMembership(dataDir, n); err != nil {
		e.Close()
		return nil, fmt.Errorf("node %s: %w", id, err)
	} else if found {
		v = persisted
	}
	if handedOff, found, err := loadHandoff(dataDir, n); err != nil {
		e.Close()
		return nil, fmt.Errorf("node %s: %w", id, err)
	} else if found {
		base = handedOff
	}
	hs, err := hints.Open(filepath.Join(dataDir, "hints"), maxMemtableBytes, append([]engine.Option{engine.WithObserver(o.hintsObserver)}, o.engineOpts...)...)
	if err != nil {
		e.Close()
		return nil, fmt.Errorf("node %s: %w", id, err)
	}
	clockID := identity.ClockID(id, incarnation)
	nd := newNode(id, clockID, address, n, w, r, v, base, store.NewDataStoreWithPersisterAndClockID(id, clockID, e), e, hs)
	nd.persistView = func(v *view) error { return writeMembership(dataDir, v) }
	nd.persistHandoff = func(v *view) error { return writeHandoff(dataDir, v) }
	return nd, nil
}

func newNode(id, clockID, address string, n, w, r int, v, handoffBase *view, ds *store.DataStore, e *engine.StorageEngine, hs *hints.Store) *Node {
	nd := &Node{
		ID:             id,
		clockID:        clockID,
		Store:          ds,
		Address:        address,
		QuorumConfig:   NewQuorumConfig(n, w, r),
		clients:        make(map[string]peerClient),
		aeTrigger:      make(chan struct{}, 1),
		handoffBase:    handoffBase,
		handoffStatus:  HandoffStatus{Epoch: v.epoch, Done: handoffBase.epoch >= v.epoch},
		handoffChanged: make(chan struct{}),
		health:         make(map[string]*peerHealth),
		conflictSeen:   make(map[string]string),
		mismatchSeen:   make(map[string]string),
		pingingBack:    make(map[string]bool),
		engine:         e,
		hints:          hs,
	}
	nd.membership.Store(v)
	return nd
}

// Close releases what n owns, in an order that keeps anything still
// running from touching what's already closed:
//  1. Stop its background loops — anti-entropy and hint delivery — and
//     wait for any round in progress (StopBackgroundLoops).
//  2. Wait for every replication drain (see drainReplication): their
//     straggler RPCs finish, and any hints they owe are durably stored.
//     No new drain starts from here.
//  3. Close its cached peer connections, retired ones included, and refuse
//     to dial any more — nothing above uses them now.
//  4. Close the hint store, then the StorageEngine
//     (which waits for in-flight flushes, stops compaction, and closes the
//     Manifest and WAL). The two don't depend on each other.
//
// It's safe to call more than once; later calls return the first call's
// result. It does not stop n's gRPC listener — the caller owns that and
// must stop it first, so no inbound write reaches a closed engine. Using n
// after Close returns errors rather than panicking.
func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		n.StopBackgroundLoops()

		n.drainMu.Lock()
		n.drainClosing = true
		n.drainMu.Unlock()
		n.drainWG.Wait()

		var errs []error

		n.clientsMu.Lock()
		n.clientsClosed = true
		for peerID, pc := range n.clients {
			if err := pc.client.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close client for %q: %w", peerID, err))
			}
		}
		for _, client := range n.retired {
			if err := client.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close retired client: %w", err))
			}
		}
		n.clients = make(map[string]peerClient)
		n.retired = nil
		n.clientsMu.Unlock()

		if err := n.hints.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close hint store: %w", err))
		}
		if err := n.engine.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close storage engine: %w", err))
		}
		n.closeErr = errors.Join(errs...)
	})
	return n.closeErr
}

// StartCompactionLoop compacts the node's SSTables every interval until ctx
// is canceled (see StorageEngine.StartCompactionLoop).
func (n *Node) StartCompactionLoop(ctx context.Context, interval time.Duration) {
	n.engine.StartCompactionLoop(ctx, interval)
}
