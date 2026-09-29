package e2e

import (
	"fmt"
	"maps"
	"net/http"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"distributed-kv-datastore/internal/rpc/pb"
)

// valuesOf extracts the sorted values from a localSet (value|clock|deleted).
func valuesOf(set []string) []string {
	var out []string
	for _, s := range set {
		out = append(out, s[:strings.Index(s, "|")])
	}
	slices.Sort(out)
	return out
}

// 1. Every acknowledged write survives all three processes being killed at
// once: each node recovers it from its own WAL and SSTables.
func TestWholeClusterCrashRecovery(t *testing.T) {
	skipShort(t)
	ids := []string{"node-1", "node-2", "node-3"}
	c := newCluster(t, 3, 2, 2, ids...)

	const numKeys = 200
	for i := range numKeys {
		c.put(ids[i%3], fmt.Sprintf("key-%03d", i), fmt.Sprintf("value-%03d", i), nil)
	}

	for _, id := range ids {
		c.nodes[id].kill()
	}
	for _, id := range ids {
		c.nodes[id].restart()
	}
	c.waitPeersReachable(ids...)

	for i := range numKeys {
		key, want := fmt.Sprintf("key-%03d", i), fmt.Sprintf("value-%03d", i)
		if got := c.get(ids[(i+1)%3], key); !slices.Equal(got, []string{want}) {
			t.Fatalf("after the crash, %s reads %q, want [%q]", key, got, want)
		}
	}
}

// 2. Writes a crashed replica missed reach it through hints after it comes
// back on the same data dir, with anti-entropy out of the picture. The check
// is on node-3's own copy, not a quorum read.
func TestHintedHandoffAcrossProcessCrash(t *testing.T) {
	skipShort(t)
	ids := []string{"node-1", "node-2", "node-3"}
	c := newCluster(t, 3, 2, 2, ids...)

	c.nodes["node-3"].kill()
	const numKeys = 100
	keys := make([]string, numKeys)
	for i := range numKeys {
		keys[i] = fmt.Sprintf("hinted-%03d", i)
		c.put("node-1", keys[i], fmt.Sprintf("v-%03d", i), nil)
	}
	c.nodes["node-3"].restart()

	missing := keys
	eventually(t, 20*time.Second, "node-3's local copies to match node-1's", func() bool {
		var still []string
		for _, key := range missing {
			want := c.localSet("node-1", key)
			if len(want) == 0 || !slices.Equal(c.localSet("node-3", key), want) {
				still = append(still, key)
			}
		}
		missing = still
		return len(missing) == 0
	})
}

// 3 and 4. Growing the cluster from four nodes to five, then shrinking it to
// four by removing a different node, moves every key to its new owners with
// exactly the versions it had, concurrent siblings included, and the removed
// node reports not ready and drains before it is stopped.
func TestScaleUpThenDown(t *testing.T) {
	skipShort(t)
	const n = 3
	four := []string{"node-1", "node-2", "node-3", "node-4"}
	c := newCluster(t, n, 2, 2, four...)

	// 300 keys; every 15th has two concurrent versions, written with an empty
	// context through two different owners so each versions it on itself.
	const numKeys = 300
	keys := make([]string, numKeys)
	siblings := 0
	for i := range numKeys {
		keys[i] = fmt.Sprintf("key-%03d", i)
		if i%15 == 0 {
			own := owners(t, four, n, keys[i])
			c.put(own[0], keys[i], "left-"+keys[i], &pb.VectorContext{})
			c.put(own[1], keys[i], "right-"+keys[i], &pb.VectorContext{})
			siblings++
		} else {
			c.put(four[i%4], keys[i], "v-"+keys[i], nil)
		}
	}

	// Record each key's sibling set once all of its owners agree on it (the
	// replica outside W catches up in the background).
	expected := map[string][]string{}
	eventually(t, 15*time.Second, "every owner in the 4-node ring to hold the same versions", func() bool {
		for i, key := range keys {
			if _, done := expected[key]; done {
				continue
			}
			own := owners(t, four, n, key)
			first := c.localSet(own[0], key)
			wantLen := 1
			if i%15 == 0 {
				wantLen = 2
			}
			if len(first) != wantLen || !slices.Equal(c.localSet(own[1], key), first) || !slices.Equal(c.localSet(own[2], key), first) {
				return false
			}
			expected[key] = first
		}
		return true
	})
	if siblings != 20 {
		t.Fatalf("setup: %d keys with siblings, want 20", siblings)
	}

	// ---- 3. Scale up: node-5 starts at epoch 1 listing all five and
	// announces itself.
	five := append(slices.Clone(four), "node-5")
	c.addNode("node-5", 1, four...)
	for _, id := range five {
		p := c.nodes[id]
		eventually(t, 15*time.Second, id+" to report epoch 1", func() bool {
			st, err := p.membership()
			return err == nil && st.Epoch == 1 && len(st.Members) == 5
		})
	}
	for _, id := range five {
		c.nodes[id].waitHandoff(1, 30*time.Second)
	}
	c.checkOwners(t, five, n, keys, expected, "after scale-up")
	movedToNode5 := 0
	for _, key := range keys {
		if slices.Contains(owners(t, five, n, key), "node-5") {
			movedToNode5++
		}
	}
	if movedToNode5 == 0 {
		t.Fatal("no key has node-5 as an owner: scale-up exercised nothing")
	}

	// ---- 4. Scale down: remove node-2 at epoch 2, through node-1.
	const leaving = "node-2"
	remaining := slices.DeleteFunc(slices.Clone(five), func(id string) bool { return id == leaving })
	members := c.memberAddrs()
	delete(members, leaving)
	c.nodes["node-1"].postMembership(2, members)

	x := c.nodes[leaving]
	eventually(t, 15*time.Second, leaving+"'s /readyz to answer 503", func() bool { return x.readyz() == http.StatusServiceUnavailable })
	x.waitHandoff(2, 60*time.Second) // drained: pushed, and every remaining member seen at epoch 2
	x.kill()
	for _, id := range remaining {
		c.nodes[id].waitHandoff(2, 30*time.Second)
	}
	c.checkOwners(t, remaining, n, keys, expected, "after scale-down")

	for _, id := range remaining {
		for _, key := range keys {
			if got, want := c.get(id, key), valuesOf(expected[key]); !slices.Equal(got, want) {
				t.Fatalf("after scale-down, Get %s via %s = %q, want %q", key, id, got, want)
			}
		}
	}
}

// checkOwners requires every owner of every key in the ring of ids to hold
// exactly the expected versions, read from its own store.
func (c *cluster) checkOwners(t *testing.T, ids []string, n int, keys []string, expected map[string][]string, when string) {
	t.Helper()
	for _, key := range keys {
		for _, id := range owners(t, ids, n, key) {
			if got := c.localSet(id, key); !slices.Equal(got, expected[key]) {
				statuses := map[string]membershipStatus{}
				for _, other := range ids {
					statuses[other], _ = c.nodes[other].membership()
				}
				t.Fatalf("%s: owner %s of %s holds %q, want %q\nhandoff statuses: %+v", when, id, key, got, expected[key], statuses)
			}
		}
	}
}

// 5. SIGTERM shuts a node down cleanly (exit code 0), and it comes back on
// the same data dir with its data.
func TestGracefulShutdown(t *testing.T) {
	skipShort(t)
	if runtime.GOOS == "windows" {
		t.Skip("SIGTERM cannot be sent to a process on Windows")
	}
	ids := []string{"node-1", "node-2", "node-3"}
	c := newCluster(t, 3, 2, 2, ids...)
	keys := map[string]string{}
	for i := range 50 {
		key := fmt.Sprintf("graceful-%02d", i)
		keys[key] = fmt.Sprintf("v-%02d", i)
		c.put("node-1", key, keys[key], nil)
	}

	p := c.nodes["node-1"]
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.exited:
	case <-time.After(15 * time.Second):
		t.Fatal("node-1 did not exit within 15s of SIGTERM")
	}
	if code := p.cmd.ProcessState.ExitCode(); code != 0 {
		t.Fatalf("node-1 exited with code %d after SIGTERM, want 0:\n%s", code, p.tail(20))
	}

	p.restart()
	c.waitPeersReachable(ids...)
	for _, key := range slices.Sorted(maps.Keys(keys)) {
		if got := c.localSet("node-1", key); len(got) != 1 || valuesOf(got)[0] != keys[key] {
			t.Fatalf("after a graceful restart, node-1's own copy of %s is %q, want %q", key, got, keys[key])
		}
	}
}
