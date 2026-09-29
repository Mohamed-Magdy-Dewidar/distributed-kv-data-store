package node

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	reflectionpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/status"

	"distributed-kv-datastore/internal/rpc/pb"
)

// kvClusterClients starts len(ids) served nodes with N=3, W=3, R=1 — every
// acknowledged write is on all three replicas, so a read from any replica sees
// it — and a KVClient connection to each.
func kvClusterClients(t *testing.T, ids ...string) (map[string]*Node, map[string]pb.KVClientClient) {
	t.Helper()
	addrs := reserveAddrs(t, ids...)
	nodes := map[string]*Node{}
	clients := map[string]pb.KVClientClient{}
	for _, id := range ids {
		nodes[id] = New(id, addrs[id], 3, 3, 1, neighborsOf(addrs, id))
		serveNode(t, nodes[id], addrs[id])
		conn, err := grpc.NewClient(addrs[id], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		clients[id] = pb.NewKVClientClient(conn)
	}
	return nodes, clients
}

func ctx5(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func mustPut(t *testing.T, c pb.KVClientClient, key, value string, vc *pb.VectorContext) {
	t.Helper()
	if _, err := c.Put(ctx5(t), &pb.PutRequest{Key: key, Value: value, Context: vc}); err != nil {
		t.Fatalf("Put(%q, %q): %v", key, value, err)
	}
}

func mustGet(t *testing.T, c pb.KVClientClient, key string) *pb.GetResponse {
	t.Helper()
	resp, err := c.Get(ctx5(t), &pb.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	return resp
}

// 1. A value comes back byte-identical, non-ASCII UTF-8 included.
func TestClientPutGetRoundTrip(t *testing.T) {
	_, clients := kvClusterClients(t, "node-1", "node-2", "node-3")
	const value = "héllo ✓"

	mustPut(t, clients["node-1"], "greeting", value, nil)
	for id, c := range clients {
		resp := mustGet(t, c, "greeting")
		if !resp.Found || len(resp.Values) != 1 || resp.Values[0] != value {
			t.Errorf("Get via %s: found=%v values=%q, want [%q]", id, resp.Found, resp.Values, value)
		}
	}
}

// 2. Two writes that don't know of each other are siblings; a write carrying
// the context Get returned collapses them.
func TestClientSiblingsCollapseWithTheReturnedContext(t *testing.T) {
	_, clients := kvClusterClients(t, "node-1", "node-2", "node-3")
	stale := mustGet(t, clients["node-1"], "k").Context // the key doesn't exist yet

	// Same stale context, coordinated by different nodes: concurrent versions.
	mustPut(t, clients["node-1"], "k", "from-node-1", stale)
	mustPut(t, clients["node-2"], "k", "from-node-2", stale)

	resp := mustGet(t, clients["node-3"], "k")
	got := slices.Clone(resp.Values)
	slices.Sort(got)
	if !slices.Equal(got, []string{"from-node-1", "from-node-2"}) {
		t.Fatalf("expected both siblings, got %q", resp.Values)
	}

	mustPut(t, clients["node-3"], "k", "resolved", resp.Context)
	after := mustGet(t, clients["node-1"], "k")
	if len(after.Values) != 1 || after.Values[0] != "resolved" {
		t.Fatalf("a Put with Get's context should leave exactly the new value, got %q", after.Values)
	}
}

// 3. A key that was never written is not an error.
func TestClientGetOfAMissingKey(t *testing.T) {
	_, clients := kvClusterClients(t, "node-1", "node-2", "node-3")

	resp, err := clients["node-1"].Get(ctx5(t), &pb.GetRequest{Key: "never-written"})
	if err != nil {
		t.Fatalf("Get of a missing key: %v", err)
	}
	if resp.Found || len(resp.Values) != 0 {
		t.Fatalf("found=%v values=%q, want not found", resp.Found, resp.Values)
	}
	if resp.Context == nil {
		t.Fatal("the context must be present even when the key is missing")
	}
}

// A deleted key is not found, but its tombstone is in the context, so a Put
// with that context supersedes it.
func TestClientGetHidesTombstonesButKeepsTheirClocks(t *testing.T) {
	nodes, clients := kvClusterClients(t, "node-1", "node-2", "node-3")
	mustPut(t, clients["node-1"], "doomed", "alive", nil)

	// Delete on node-1 (there is no delete in the client API) and copy the
	// tombstone to the other replicas.
	if ok, msg := nodes["node-1"].Store.Delete("doomed", nil); !ok {
		t.Fatal(msg)
	}
	tombstones, _, _ := nodes["node-1"].Store.Get("doomed")
	for _, id := range []string{"node-2", "node-3"} {
		for _, it := range tombstones {
			nodes[id].Store.MergeReplicated("doomed", it)
		}
	}

	resp := mustGet(t, clients["node-2"], "doomed")
	if resp.Found || len(resp.Values) != 0 {
		t.Fatalf("a deleted key: found=%v values=%q", resp.Found, resp.Values)
	}
	if len(resp.Context.Entries) == 0 {
		t.Fatal("the tombstone's clock should be in the context")
	}

	mustPut(t, clients["node-3"], "doomed", "reborn", resp.Context)
	after := mustGet(t, clients["node-1"], "doomed")
	if !after.Found || len(after.Values) != 1 || after.Values[0] != "reborn" {
		t.Fatalf("after re-putting with the tombstone's context: found=%v values=%q", after.Found, after.Values)
	}
}

// Values that weren't written as strings come back as their JSON encoding.
func TestClientGetRendersNonStringValuesAsJSON(t *testing.T) {
	nodes, clients := kvClusterClients(t, "node-1", "node-2", "node-3")
	for _, nd := range nodes {
		nd.Store.Put("number", 42, nil)
	}
	// Each node wrote its own version; any of them shows the same rendering.
	resp := mustGet(t, clients["node-1"], "number")
	if !resp.Found || len(resp.Values) == 0 || resp.Values[0] != "42" {
		t.Fatalf("found=%v values=%q, want \"42\"", resp.Found, resp.Values)
	}
}

// 4. A node that isn't a replica for the key forwards the write.
func TestClientPutThroughANonReplicaIsForwarded(t *testing.T) {
	nodes, clients := kvClusterClients(t, "node-1", "node-2", "node-3", "node-4")
	var key string
	for i := 0; ; i++ {
		key = fmt.Sprintf("fwd-%d", i)
		if !slices.Contains(owners(nodes["node-4"], key, 3), "node-4") {
			break
		}
	}

	mustPut(t, clients["node-4"], key, "forwarded", nil)

	replica := owners(nodes["node-4"], key, 3)[0]
	resp := mustGet(t, clients[replica], key)
	if !resp.Found || len(resp.Values) != 1 || resp.Values[0] != "forwarded" {
		t.Fatalf("Get via replica %s: found=%v values=%q", replica, resp.Found, resp.Values)
	}
	if held := heldBy(t, nodes["node-4"], key); len(held) != 0 {
		t.Fatalf("node-4 is not a replica and should not hold the key, holds %v", held)
	}
}

// 5. An empty key is the caller's mistake.
func TestClientEmptyKeyIsInvalidArgument(t *testing.T) {
	_, clients := kvClusterClients(t, "node-1", "node-2", "node-3")

	_, err := clients["node-1"].Put(ctx5(t), &pb.PutRequest{Key: "", Value: "v"})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Errorf("Put with an empty key: code %v (%v), want InvalidArgument", got, err)
	}
	_, err = clients["node-1"].Get(ctx5(t), &pb.GetRequest{Key: ""})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Errorf("Get with an empty key: code %v (%v), want InvalidArgument", got, err)
	}
}

// Errors other than the caller's map to Unavailable, and an expired call to
// its own code.
func TestClientErrorCodes(t *testing.T) {
	// Only node-1 is up; W=3 can't be met.
	addrs := reserveAddrs(t, "node-1")
	maps.Copy(addrs, knownAddrs("node-2", "node-3")) // down
	nd := New("node-1", addrs["node-1"], 3, 3, 1, neighborsOf(addrs, "node-1"))
	nd.QuorumConfig.ReplicationTimeout = time.Second
	nd.QuorumConfig.MaxReconnectBackoff = 50 * time.Millisecond
	serveNode(t, nd, addrs["node-1"])
	conn, err := grpc.NewClient(addrs["node-1"], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := pb.NewKVClientClient(conn)

	_, err = client.Put(ctx5(t), &pb.PutRequest{Key: "k", Value: "v"})
	if got := status.Code(err); got != codes.Unavailable {
		t.Errorf("Put that cannot reach its quorum: code %v (%v), want Unavailable", got, err)
	}

	expired, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond)
	_, err = client.Get(expired, &pb.GetRequest{Key: "k"})
	if got := status.Code(err); got != codes.DeadlineExceeded {
		t.Errorf("Get with an expired context: code %v (%v), want DeadlineExceeded", got, err)
	}
}

// 6. grpcurl and friends can find the service without the .proto files.
func TestReflectionListsTheClientService(t *testing.T) {
	addr := reserveAddr(t)
	serveNode(t, New("node-1", addr, 1, 1, 1, nil), addr)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	stream, err := reflectionpb.NewServerReflectionClient(conn).ServerReflectionInfo(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&reflectionpb.ServerReflectionRequest{
		MessageRequest: &reflectionpb.ServerReflectionRequest_ListServices{ListServices: ""},
	}); err != nil {
		t.Fatal(err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, s := range resp.GetListServicesResponse().GetService() {
		names = append(names, s.GetName())
	}
	for _, want := range []string{"kvstore.KVClient", "kvstore.KVReplication"} {
		if !slices.Contains(names, want) {
			t.Errorf("reflection lists %q, missing %s", names, want)
		}
	}
}
