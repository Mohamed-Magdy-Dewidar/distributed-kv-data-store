package chaos

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"distributed-kv-datastore/internal/hashring"
	"distributed-kv-datastore/internal/rpc"
	"distributed-kv-datastore/internal/rpc/pb"
)

// ReadbackEntry is one key's read-back: a quorum Get through Target and,
// if copies were counted, which of its owners hold the recorder's value in
// their own storage.
type ReadbackEntry struct {
	Key    string `json:"key"`
	Time   int64  `json:"time"` // Unix nanoseconds, when the Get was sent
	Target string `json:"target"`
	Result Result `json:"result"`
	Code   string `json:"code,omitempty"`
	Msg    string `json:"msg,omitempty"`
	// Owners is the key's preference list, set when copies were counted;
	// Holders the owners whose own copy (FetchItem: no quorum, no
	// forwarding) has the recorder's value, and Unreadable those that did
	// not answer.
	Owners     []string `json:"owners,omitempty"`
	Holders    []string `json:"holders,omitempty"`
	Unreadable []string `json:"unreadable,omitempty"`
}

// ReadbackConfig is a read-back run. Copies need Membership: the ring is
// built from its members with replication factor N, and must match its
// fingerprint.
type ReadbackConfig struct {
	Targets     []Target
	Copies      bool
	Membership  MembershipBody
	N           int
	Concurrency int
	Timeout     time.Duration
	ConnectWait time.Duration
}

// Readback reads back every key in keys, writing one JSON line per key to
// out, and returns the counts by result.
func Readback(ctx context.Context, cfg ReadbackConfig, keys []string, out io.Writer) (map[Result]int, error) {
	if cfg.Concurrency <= 0 || cfg.Timeout <= 0 {
		return nil, errors.New("readback: concurrency and timeout must be positive")
	}
	var ring *hashring.HashRing
	peers := map[string]*rpc.Client{}
	if cfg.Copies {
		var err error
		if ring, err = NewRing(cfg.Membership.Members, cfg.N, cfg.Membership.Fingerprint); err != nil {
			return nil, fmt.Errorf("readback: %w", err)
		}
		for id, addr := range cfg.Membership.Members {
			c, err := rpc.Dial(addr, 5*time.Second)
			if err != nil {
				return nil, fmt.Errorf("readback: %w", err)
			}
			defer c.Close()
			peers[id] = c
		}
	}
	p, err := dialPool(ctx, cfg.Targets, "", 0, cfg.ConnectWait)
	if err != nil {
		return nil, fmt.Errorf("readback: %w", err)
	}
	defer p.close()

	keyCh := make(chan string)
	entries := make(chan ReadbackEntry, 256)
	var wg sync.WaitGroup
	for range cfg.Concurrency {
		wg.Go(func() {
			for k := range keyCh {
				e := readOne(ctx, p.pick(), k, cfg.Timeout)
				if ring != nil {
					e.Owners = ring.GetPreferenceList(k, cfg.N)
					for _, o := range e.Owners {
						switch held, err := holds(ctx, peers[o], k, cfg.Timeout); {
						case err != nil:
							e.Unreadable = append(e.Unreadable, o)
						case held:
							e.Holders = append(e.Holders, o)
						}
					}
				}
				entries <- e
			}
		})
	}
	go func() {
		defer close(keyCh)
		for _, k := range keys {
			select {
			case keyCh <- k:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		wg.Wait()
		close(entries)
	}()

	counts := map[Result]int{}
	w := bufio.NewWriter(out)
	enc := json.NewEncoder(w)
	var werr error
	for e := range entries {
		counts[e.Result]++
		if err := enc.Encode(e); err != nil && werr == nil {
			werr = err
		}
	}
	if err := w.Flush(); werr == nil {
		werr = err
	}
	if werr == nil {
		werr = ctx.Err()
	}
	return counts, werr
}

func readOne(ctx context.Context, c *conn, key string, timeout time.Duration) ReadbackEntry {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	e := ReadbackEntry{Key: key, Time: time.Now().UnixNano(), Target: c.id}
	resp, err := c.kv.Get(ctx, &pb.GetRequest{Key: key})
	e.Result, e.Code, e.Msg = Classify(err)
	if err == nil {
		e.Result = getResult(key, resp.GetFound(), resp.GetValues())
	}
	return e
}

// holds reports whether the peer's own copy of key has the recorder's value.
func holds(ctx context.Context, c *rpc.Client, key string, timeout time.Duration) (bool, error) {
	if c == nil {
		return false, errors.New("no connection")
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	items, found, err := c.FetchItem(ctx, key)
	if err != nil || !found {
		return false, err
	}
	for _, it := range items {
		if v, ok := it.Value.(string); ok && !it.IsDeleted && ValueMatches(key, v) {
			return true, nil
		}
	}
	return false, nil
}

// ReadReadback reads a read-back's output.
func ReadReadback(r io.Reader) ([]ReadbackEntry, error) {
	var out []ReadbackEntry
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for line := 1; sc.Scan(); line++ {
		if len(strings.TrimSpace(sc.Text())) == 0 {
			continue
		}
		var e ReadbackEntry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			return nil, fmt.Errorf("readback line %d: %w", line, err)
		}
		out = append(out, e)
	}
	return out, sc.Err()
}
