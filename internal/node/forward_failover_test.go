package node

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// forwardCluster is three nodes with N=2, W=1, of which forwarder is not an
// owner of the key it returns; replicas is the key's preference list, in the
// order the forwarder tries it. Only the nodes in serve are listening.
type forwardCluster struct {
	nodes    map[string]*Node
	addrs    map[string]string
	forward  *Node
	key      string
	replicas []string
}

func newForwardCluster(t *testing.T, base int, serve func(replicas []string) []string, pick func(key string, replicas []string) bool) *forwardCluster {
	t.Helper()
	ids := []string{"node-1", "node-2", "node-3"}
	addrs := map[string]string{}
	for i, id := range ids {
		addrs[id] = fmt.Sprintf("localhost:%d", base+i)
	}
	nodes := map[string]*Node{}
	for _, id := range ids {
		nodes[id] = New(id, addrs[id], 2, 1, 1, neighborsOf(addrs, id))
		nodes[id].QuorumConfig.MaxReconnectBackoff = 50 * time.Millisecond
	}
	forwarder := nodes["node-3"]

	var key string
	var replicas []string
	for i := 0; ; i++ {
		key = fmt.Sprintf("fwd-key-%d", i)
		replicas = owners(forwarder, key, 2)
		if slices.Contains(replicas, "node-3") {
			continue
		}
		if pick == nil || pick(key, replicas) {
			break
		}
	}
	for _, id := range serve(replicas) {
		serveNode(t, nodes[id], addrs[id])
	}
	return &forwardCluster{nodes: nodes, addrs: addrs, forward: forwarder, key: key, replicas: replicas}
}

func (c *forwardCluster) put(t *testing.T, timeout time.Duration) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.forward.Put(ctx, c.key, "v", nil)
}

// (c) A replica whose membership no longer includes it doesn't think it owns
// the key and refuses with FailedPrecondition before applying anything; the
// forwarder, whose view still lists it first, tries the next replica. No
// heartbeats run, so the two views stay different.
func TestForwardFailsOverFromAReplicaThatIsNotAnOwner(t *testing.T) {
	c := newForwardCluster(t, 61411, func([]string) []string { return []string{"node-1", "node-2", "node-3"} }, nil)
	first, second := c.nodes[c.replicas[0]], c.nodes[c.replicas[1]]

	// The first replica's view: the other two nodes only (it has been removed).
	if _, err := first.SetMembership(1, membersOf(c.addrs, c.replicas[1], "node-3")); err != nil {
		t.Fatal(err)
	}
	if first.IsMember() {
		t.Fatal("setup: the first replica should be outside its own view")
	}
	if got := c.forward.owners(t, c.key); got[0] != first.ID {
		t.Fatalf("setup: the forwarder should try %s first, its list is %v", first.ID, got)
	}

	if err := c.put(t, 5*time.Second); err != nil {
		t.Fatalf("the write should have failed over to %s: %v", second.ID, err)
	}
	if got := heldBy(t, second, c.key); len(got) != 1 {
		t.Fatalf("%s should hold the forwarded write, holds %v", second.ID, got)
	}
}

func (n *Node) owners(t *testing.T, key string) []string {
	t.Helper()
	return owners(n, key, n.QuorumConfig.N)
}

// (d) A replica the forwarder can't even connect to, or can't resolve, is
// skipped too.
func TestForwardFailsOverFromAReplicaItCannotDial(t *testing.T) {
	for name, badAddr := range map[string]string{
		"dial error":       "bad\x7faddr:1", // rejected while creating the connection
		"unresolvable":     "no-such-host.invalid:7000",
		"nothing listens":  "localhost:61439",
		"unsupported port": "localhost:1",
	} {
		t.Run(name, func(t *testing.T) {
			c := newForwardCluster(t, 61421, func(replicas []string) []string { return []string{replicas[1], "node-3"} }, nil)
			members := maps2(c.addrs)
			members[c.replicas[0]] = badAddr
			if _, err := c.forward.SetMembership(1, members); err != nil {
				t.Fatal(err)
			}
			if err := c.put(t, 10*time.Second); err != nil {
				t.Fatalf("the write should have failed over to %s: %v", c.replicas[1], err)
			}
			if got := heldBy(t, c.nodes[c.replicas[1]], c.key); len(got) != 1 {
				t.Fatalf("%s should hold the forwarded write, holds %v", c.replicas[1], got)
			}
		})
	}
}

func maps2(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// (e) When every replica fails, the write was applied nowhere, and the error
// says so with codes.Unavailable. A joined error would only keep that code
// when one of its parts is a gRPC status; replicas skipped as dead, or that
// can't be dialed, produce plain errors and the join would read as Unknown.
func TestForwardWithNoReachableReplicaIsUnavailable(t *testing.T) {
	for name, prepare := range map[string]func(c *forwardCluster){
		"all down": func(*forwardCluster) {},
		"all marked dead": func(c *forwardCluster) {
			for _, id := range c.replicas {
				for range c.forward.QuorumConfig.MaxMissedHeartbeats {
					c.forward.recordHeartbeat(id, false)
				}
			}
		},
		"all undialable": func(c *forwardCluster) {
			members := maps2(c.addrs)
			for i, id := range c.replicas {
				members[id] = fmt.Sprintf("badaddr:%d", i+1)
			}
			if _, err := c.forward.SetMembership(1, members); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := newForwardCluster(t, 61431, func([]string) []string { return []string{"node-3"} }, nil)
			prepare(c)

			err := c.put(t, 10*time.Second)
			if err == nil {
				t.Fatal("expected the write to fail with every replica down")
			}
			if got := status.Code(err); got != codes.Unavailable {
				t.Fatalf("error code = %v, want Unavailable: %v", got, err)
			}
			for _, id := range c.replicas {
				if !strings.Contains(err.Error(), id) {
					t.Errorf("the error should say what happened with %s: %v", id, err)
				}
			}
		})
	}
}

// A replica heartbeats have marked dead is not tried: with a hung first
// replica, the write still completes at once.
func TestForwardSkipsAReplicaMarkedDead(t *testing.T) {
	c := newForwardCluster(t, 61441, func(replicas []string) []string { return []string{replicas[1], "node-3"} }, nil)
	startHungPeer(t, c.addrs[c.replicas[0]])
	for range c.forward.QuorumConfig.MaxMissedHeartbeats {
		c.forward.recordHeartbeat(c.replicas[0], false)
	}
	if !c.forward.isDead(c.replicas[0]) {
		t.Fatal("setup: the replica should be marked dead")
	}

	start := time.Now()
	if err := c.put(t, 5*time.Second); err != nil {
		t.Fatalf("the write should have skipped the dead replica: %v", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("the write took %v: it waited on the dead replica", took)
	}
}
