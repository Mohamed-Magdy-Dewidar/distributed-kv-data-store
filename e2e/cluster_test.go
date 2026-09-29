package e2e

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"

	"distributed-kv-datastore/internal/hashring"
	"distributed-kv-datastore/internal/node"
	"distributed-kv-datastore/internal/rpc"
	"distributed-kv-datastore/internal/rpc/pb"
)

// nodeBinary is cmd/node, built once by TestMain from the working tree.
var nodeBinary string

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run()) // every test skips itself
	}
	dir, err := os.MkdirTemp("", "kv-e2e-bin-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2e: temp dir:", err)
		os.Exit(1)
	}
	nodeBinary = filepath.Join(dir, "node")
	if runtime.GOOS == "windows" {
		nodeBinary += ".exe"
	}
	build := exec.Command("go", "build", "-o", nodeBinary, "distributed-kv-datastore/cmd/node")
	build.Stdout, build.Stderr = os.Stdout, os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "e2e: building cmd/node:", err)
		os.RemoveAll(dir)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

func skipShort(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("e2e: skipped with -short")
	}
}

// Test timings: fast heartbeats and hint delivery, and anti-entropy far
// enough away that it never runs during a test, so handoff and hints are the
// only things moving data. (startLoop runs a loop's first round after a
// random delay in [0, interval): at 1h that would land inside a test run a
// few percent of the time, so the tests use 24h.)
const configTemplate = `nodeId: %[1]s
listen:
  grpc: %[2]q
  http: %[3]q
dataDir: %[4]q
cluster:
  n: %[5]d
  w: %[6]d
  r: %[7]d
  epoch: %[8]d
  members:
%[9]s
storage:
  memtableBytes: 1048576
intervals:
  compaction: 24h
  antiEntropy: 24h
  hintDelivery: 500ms
  heartbeat: 100ms
timeouts:
  replication: 2s
  maxReconnectBackoff: 200ms
  shutdown: 5s
  heartbeat: 50ms
health:
  maxMissedHeartbeats: 3
`

// proc is one node: its config, data dir and, while running, its process.
type proc struct {
	t          *testing.T
	id         string
	grpcAddr   string
	httpAddr   string
	dataDir    string
	configPath string
	logPath    string

	cmd    *exec.Cmd
	exited chan struct{} // closed once cmd has been waited for
	log    *os.File
}

// cluster is a set of node processes with shared N/W/R.
type cluster struct {
	t       *testing.T
	n, w, r int
	nodes   map[string]*proc
	kv      map[string]pb.KVClientClient
	local   map[string]*rpc.Client
}

// Node processes bind their addresses from their configs, and a restarted
// node must come back on the same ones, so they can't be handed a listener.
// They get known ports instead (see internal/node's ports_test.go): below
// 32768, which neither Linux nor Windows hands out as an ephemeral port, from
// a range only this package uses (node 20001-24999, app 25001-25999, httpapi
// 26001, rpc 26501, e2e 27001-27999). Ports are never reused within a
// process, and running out of the range panics rather than spill into
// another package's.
const (
	knownPortBase  = 27000
	knownPortCount = 999
)

var nextKnownPort atomic.Int32

func knownAddr() string {
	n := int(nextKnownPort.Add(1))
	if n > knownPortCount {
		panic(fmt.Sprintf("knownAddr: this package's %d known ports (%d-%d) are used up; run fewer -count repetitions, or widen the range",
			knownPortCount, knownPortBase+1, knownPortBase+knownPortCount))
	}
	return fmt.Sprintf("127.0.0.1:%d", knownPortBase+n)
}

// newCluster starts len(ids) nodes that all list each other at epoch 0, and
// waits for every one to be ready. If a node can't bind its ports (some
// unrelated program holds one), the whole cluster is stopped and started
// again on fresh ports.
func newCluster(t *testing.T, n, w, r int, ids ...string) *cluster {
	t.Helper()
	c := &cluster{t: t, n: n, w: w, r: r, nodes: map[string]*proc{}, kv: map[string]pb.KVClientClient{}, local: map[string]*rpc.Client{}}
	for attempt := 1; ; attempt++ {
		for _, id := range ids {
			c.nodes[id] = c.newProc(id)
		}
		members := c.memberAddrs()
		bindFailed := false
		for _, id := range ids {
			p := c.nodes[id]
			p.writeConfig(n, w, r, 0, members)
			if err := p.start(); err != nil {
				if isBindFailure(err) && attempt < 3 {
					bindFailed = true
					break
				}
				t.Fatalf("starting %s: %v", id, err)
			}
		}
		if !bindFailed {
			break
		}
		t.Logf("e2e: a port was taken; restarting the cluster on new ports (attempt %d)", attempt+1)
		for _, p := range c.nodes {
			p.kill()
		}
	}
	for _, id := range ids {
		c.nodes[id].waitReady()
	}
	c.waitPeersReachable(ids...)
	return c
}

// waitPeersReachable waits until each of ids reports every other of ids
// reachable: its latest ping to it was answered. /readyz alone isn't enough.
// A node that started before its peers failed some pings to them; after even
// one, its connection to that peer is in reconnect backoff and writes to it
// fail fast, and after maxMissedHeartbeats it is marked dead. Not being dead
// isn't enough either, for the first reason.
func (c *cluster) waitPeersReachable(ids ...string) {
	c.t.Helper()
	for _, id := range ids {
		eventually(c.t, 15*time.Second, id+" to reach its peers", func() bool {
			st, err := c.nodes[id].membership()
			if err != nil {
				return false
			}
			for _, other := range ids {
				if other != id && !st.Peers[other].Reachable {
					return false
				}
			}
			return true
		})
	}
}

// newProc allocates ports and a data dir for id. Cleanups run last-in first
// out, so the process is killed (and waited for) before its data dir, created
// first, is removed.
func (c *cluster) newProc(id string) *proc {
	t := c.t
	dir := t.TempDir()
	p := &proc{
		t:          t,
		id:         id,
		grpcAddr:   knownAddr(),
		httpAddr:   knownAddr(),
		dataDir:    filepath.Join(dir, "data"),
		configPath: filepath.Join(dir, "config.yaml"),
		logPath:    filepath.Join(dir, "node.log"),
	}
	t.Cleanup(func() {
		p.kill()
		if t.Failed() {
			p.dumpLog()
		}
	})
	return p
}

// addNode starts a new node whose configuration lists members (itself
// included) at epoch: the way to grow the cluster.
func (c *cluster) addNode(id string, epoch uint64, others ...string) *proc {
	c.t.Helper()
	p := c.newProc(id)
	c.nodes[id] = p
	members := map[string]string{id: p.grpcAddr}
	for _, o := range others {
		members[o] = c.nodes[o].grpcAddr
	}
	p.writeConfig(c.n, c.w, c.r, epoch, members)
	if err := p.start(); err != nil {
		c.t.Fatalf("starting %s: %v", id, err)
	}
	p.waitReady()
	return p
}

func (c *cluster) memberAddrs() map[string]string {
	m := map[string]string{}
	for id, p := range c.nodes {
		m[id] = p.grpcAddr
	}
	return m
}

func (p *proc) writeConfig(n, w, r int, epoch uint64, members map[string]string) {
	p.t.Helper()
	var b strings.Builder
	for _, id := range slices.Sorted(maps.Keys(members)) {
		fmt.Fprintf(&b, "    - id: %s\n      address: %q\n", id, members[id])
	}
	cfg := fmt.Sprintf(configTemplate, p.id, p.grpcAddr, p.httpAddr, filepath.ToSlash(p.dataDir), n, w, r, epoch, strings.TrimRight(b.String(), "\n"))
	if err := os.WriteFile(p.configPath, []byte(cfg), 0o644); err != nil {
		p.t.Fatal(err)
	}
}

// errBind marks a start that failed because a port was taken.
type errBind struct{ log string }

func (e errBind) Error() string { return "a listen address was already in use:\n" + e.log }

func isBindFailure(err error) bool {
	_, ok := err.(errBind)
	return ok
}

// start runs the node process with its config, appending its output to its
// log. It returns once the process is running; waitReady waits for it to
// serve. A process that exits at once because it couldn't listen is reported
// as errBind, after a few retries on the same ports (a restart must keep them:
// they are in the membership).
func (p *proc) start() error {
	for try := 1; ; try++ {
		if err := p.startOnce(); err != nil {
			return err
		}
		select {
		case <-p.exited:
			out := p.tail(20)
			if !strings.Contains(out, "listen on") {
				return fmt.Errorf("%s exited during startup:\n%s", p.id, out)
			}
			if try >= 5 {
				return errBind{log: out}
			}
			time.Sleep(time.Duration(try) * 100 * time.Millisecond) // back off before binding again
		case <-time.After(300 * time.Millisecond):
			return nil // still running: it got past binding
		}
	}
}

func (p *proc) startOnce() error {
	f, err := os.OpenFile(p.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	fmt.Fprintf(f, "===== start %s at %s =====\n", p.id, time.Now().Format(time.RFC3339Nano))
	cmd := exec.Command(nodeBinary, "-config", p.configPath)
	cmd.Stdout, cmd.Stderr = f, f
	if err := cmd.Start(); err != nil {
		f.Close()
		return fmt.Errorf("start %s: %w", p.id, err)
	}
	p.cmd, p.log, p.exited = cmd, f, make(chan struct{})
	exited := p.exited
	go func() {
		cmd.Wait()
		f.Close()
		close(exited)
	}()
	return nil
}

// waitReady polls /readyz until it answers 200, failing the test if the
// process exits or 15s pass.
func (p *proc) waitReady() {
	p.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		select {
		case <-p.exited:
			p.t.Fatalf("%s exited before becoming ready:\n%s", p.id, p.tail(30))
		default:
		}
		if p.readyz() == http.StatusOK {
			return
		}
		if time.Now().After(deadline) {
			p.t.Fatalf("%s not ready after 15s:\n%s", p.id, p.tail(30))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (p *proc) readyz() int {
	client := http.Client{Timeout: time.Second}
	resp, err := client.Get("http://" + p.httpAddr + "/readyz")
	if err != nil {
		return 0
	}
	resp.Body.Close()
	return resp.StatusCode
}

// kill stops the process at once, like a crash, and waits until it is gone:
// its data dir stays locked until then.
func (p *proc) kill() {
	if p.cmd == nil {
		return
	}
	select {
	case <-p.exited:
	default:
		p.cmd.Process.Kill()
		<-p.exited
	}
	p.cmd = nil
}

// restart starts the process again with the same config and data dir.
func (p *proc) restart() {
	p.t.Helper()
	p.kill()
	if err := p.start(); err != nil {
		p.t.Fatalf("restarting %s: %v", p.id, err)
	}
	p.waitReady()
}

func (p *proc) tail(lines int) string {
	data, _ := os.ReadFile(p.logPath)
	all := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	return strings.Join(all[max(0, len(all)-lines):], "\n")
}

func (p *proc) dumpLog() {
	data, _ := os.ReadFile(p.logPath)
	p.t.Logf("----- log of %s -----\n%s", p.id, data)
}

// ---- clients ----

func dialOpts() []grpc.DialOption {
	cp := grpc.ConnectParams{Backoff: backoff.DefaultConfig}
	cp.Backoff.MaxDelay = 200 * time.Millisecond // nodes restart; reconnect quickly
	return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithConnectParams(cp)}
}

// kvc is a KVClient connection to id.
func (c *cluster) kvc(id string) pb.KVClientClient {
	c.t.Helper()
	if k, ok := c.kv[id]; ok {
		return k
	}
	conn, err := grpc.NewClient(c.nodes[id].grpcAddr, dialOpts()...)
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(func() { conn.Close() })
	c.kv[id] = pb.NewKVClientClient(conn)
	return c.kv[id]
}

// localClient reads one node's own copy of a key (KVReplication FetchItem),
// with no quorum and no forwarding: it shows where data physically is.
func (c *cluster) localClient(id string) *rpc.Client {
	c.t.Helper()
	if l, ok := c.local[id]; ok {
		return l
	}
	l, err := rpc.Dial(c.nodes[id].grpcAddr, 200*time.Millisecond)
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(func() { l.Close() })
	c.local[id] = l
	return l
}

func ctxFor(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func (c *cluster) put(via, key, value string, vc *pb.VectorContext) {
	c.t.Helper()
	if _, err := c.kvc(via).Put(ctxFor(c.t, 5*time.Second), &pb.PutRequest{Key: key, Value: value, Context: vc}); err != nil {
		c.t.Fatalf("Put %q via %s: %v", key, via, err)
	}
}

// get reads key through via, retrying for a few seconds on an error (nodes
// may have just restarted), and returns its values sorted.
func (c *cluster) get(via, key string) []string {
	c.t.Helper()
	var last error
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := c.kvc(via).Get(ctxFor(c.t, 3*time.Second), &pb.GetRequest{Key: key})
		if err == nil {
			vals := slices.Clone(resp.Values)
			sort.Strings(vals)
			return vals
		}
		last = err
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("Get %q via %s kept failing: %v", key, via, last)
	return nil
}

// localSet is id's own sibling set for key, each version as value|clock|deleted,
// sorted: two nodes hold the same versions exactly when their sets are equal.
func (c *cluster) localSet(id, key string) []string {
	c.t.Helper()
	items, _, err := c.localClient(id).FetchItem(ctxFor(c.t, 3*time.Second), key)
	if err != nil {
		c.t.Fatalf("local read of %q on %s: %v", key, id, err)
	}
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, fmt.Sprintf("%v|%v|%t", it.Value, it.VectorClock.Snapshot(), it.IsDeleted))
	}
	sort.Strings(out)
	return out
}

// owners is key's preference list in a ring of ids, computed the way every
// node computes it.
func owners(t *testing.T, ids []string, n int, key string) []string {
	t.Helper()
	ring, err := hashring.NewHashRingFromMembers(node.VirtualNodesPerPhysical, ids)
	if err != nil {
		t.Fatal(err)
	}
	return ring.GetPreferenceList(key, n)
}

// ---- admin ----

type membershipStatus struct {
	Epoch   uint64            `json:"epoch"`
	Members map[string]string `json:"members"`
	Handoff struct {
		Done    bool `json:"done"`
		Pushed  int  `json:"pushed"`
		Hinted  int  `json:"hinted"`
		Pending int  `json:"pending"`
	} `json:"handoff"`
	Peers map[string]struct {
		Alive     bool `json:"alive"`
		Reachable bool `json:"reachable"`
	} `json:"peers"`
}

func (p *proc) membership() (membershipStatus, error) {
	var st membershipStatus
	resp, err := (&http.Client{Timeout: 2 * time.Second}).Get("http://" + p.httpAddr + "/admin/membership")
	if err != nil {
		return st, err
	}
	defer resp.Body.Close()
	return st, json.NewDecoder(resp.Body).Decode(&st)
}

// postMembership applies a membership through id's admin API.
func (p *proc) postMembership(epoch uint64, members map[string]string) {
	p.t.Helper()
	body, _ := json.Marshal(map[string]any{"epoch": epoch, "members": members})
	resp, err := http.Post("http://"+p.httpAddr+"/admin/membership", "application/json", strings.NewReader(string(body)))
	if err != nil {
		p.t.Fatal(err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		p.t.Fatalf("POST /admin/membership to %s: %d %s", p.id, resp.StatusCode, out)
	}
}

// waitHandoff polls GET /admin/handoff?epoch=E on p until it answers 200,
// for at most within. Each request blocks server-side for up to 5s.
func (p *proc) waitHandoff(epoch uint64, within time.Duration) {
	p.t.Helper()
	deadline := time.Now().Add(within)
	url := fmt.Sprintf("http://%s/admin/handoff?epoch=%d&timeout=5s", p.httpAddr, epoch)
	var last string
	for time.Now().Before(deadline) {
		resp, err := (&http.Client{Timeout: 10 * time.Second}).Get(url)
		if err != nil {
			last = err.Error()
			time.Sleep(50 * time.Millisecond)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return
		}
		last = fmt.Sprintf("%d %s", resp.StatusCode, body)
	}
	p.t.Fatalf("%s: handoff to epoch %d not done after %v; last answer: %s", p.id, epoch, within, last)
}

// eventually polls cond every 50ms until it holds or within passes.
func eventually(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", within, what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
