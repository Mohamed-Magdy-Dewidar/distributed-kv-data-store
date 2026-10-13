package chaos

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"distributed-kv-datastore/internal/rpc/pb"
)

// fakeKV is a pod's client API: a map, with optional delay and errors.
type fakeKV struct {
	pb.UnimplementedKVClientServer
	mu     sync.Mutex
	data   map[string]string
	delay  time.Duration
	putErr error
}

func (f *fakeKV) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	if f.putErr != nil {
		return nil, f.putErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[req.Key] = req.Value
	return &pb.PutResponse{}, nil
}

func (f *fakeKV) Get(_ context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.data[req.Key]
	if !ok {
		return &pb.GetResponse{}, nil
	}
	return &pb.GetResponse{Values: []string{v}, Found: true}, nil
}

// fakeReplica is a pod's own storage, for FetchItem.
type fakeReplica struct {
	pb.UnimplementedKVReplicationServer
	held map[string]string
}

func (f *fakeReplica) FetchItem(_ context.Context, req *pb.FetchItemRequest) (*pb.FetchItemResponse, error) {
	v, ok := f.held[req.Key]
	if !ok {
		return &pb.FetchItemResponse{}, nil
	}
	b, _ := json.Marshal(v)
	return &pb.FetchItemResponse{Found: true, Items: []*pb.DataItem{{Value: b, VectorClock: map[string]uint32{"kv-0#1": 1}}}}, nil
}

type fakePod struct {
	kv   *fakeKV
	repl *fakeReplica
	srv  *grpc.Server
	addr string
}

func startPod(t *testing.T, data map[string]string) *fakePod {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	own := map[string]string{} // each pod its own copy: they store independently
	maps.Copy(own, data)
	p := &fakePod{kv: &fakeKV{data: own}, repl: &fakeReplica{held: map[string]string{}}, srv: grpc.NewServer(), addr: lis.Addr().String()}
	pb.RegisterKVClientServer(p.srv, p.kv)
	pb.RegisterKVReplicationServer(p.srv, p.repl)
	go p.srv.Serve(lis)
	t.Cleanup(p.srv.Stop)
	return p
}

func baseConfig(test string, targets ...Target) RecordConfig {
	return RecordConfig{
		Test: test, Targets: targets, WritePrefix: "chaos-" + test, ValueSize: 32,
		Concurrency: 16, Timeout: time.Second, ConnectWait: 5 * time.Second, Seed: 7,
	}
}

func runRecord(t *testing.T, cfg RecordConfig) ([]Request, Counts) {
	t.Helper()
	var out bytes.Buffer
	counts, err := Record(context.Background(), cfg, &out)
	if err != nil {
		t.Fatal(err)
	}
	reqs, err := ReadRecords(&out)
	if err != nil {
		t.Fatal(err)
	}
	return reqs, counts
}

func TestRecordWritesAndReads(t *testing.T) {
	pre := map[string]string{}
	for i := range 10 {
		k := KeyName("chaos-pre", i)
		pre[k] = Value(k, 32)
	}
	a, b := startPod(t, pre), startPod(t, pre)
	cfg := baseConfig("t1", Target{"kv-0", a.addr}, Target{"kv-1", b.addr})
	cfg.WriteRate, cfg.ReadRate, cfg.ReadPrefix, cfg.ReadKeys = 200, 200, "chaos-pre", 10
	cfg.Duration = 500 * time.Millisecond
	reqs, counts := runRecord(t, cfg)

	if counts[OpPut][ResultOK] < 50 || counts[OpGet][ResultOK] < 50 {
		t.Fatalf("counts %v: want about 100 of each, all ok", counts)
	}
	targets := map[string]int{}
	seqs := map[int64]bool{}
	for _, r := range reqs {
		if r.Result != ResultOK || r.Test != "t1" || r.End < r.Start {
			t.Fatalf("request %+v", r)
		}
		if seqs[r.Seq] {
			t.Fatalf("seq %d twice", r.Seq)
		}
		seqs[r.Seq] = true
		targets[r.Target]++
	}
	if targets["kv-0"] == 0 || targets["kv-1"] == 0 {
		t.Errorf("targets %v: want both pods used", targets)
	}
	k0 := KeyName("chaos-t1", 0)
	v, ok := a.kv.data[k0]
	if !ok {
		v = b.kv.data[k0]
	}
	if !ValueMatches(k0, v) || len(v) != 32 {
		t.Errorf("first write's value %q", v)
	}
}

func TestRecordPreloadWritesEveryKeyOnce(t *testing.T) {
	a := startPod(t, nil)
	cfg := baseConfig("pre", Target{"kv-0", a.addr})
	cfg.WritePrefix, cfg.Writes, cfg.WriteRate, cfg.Concurrency = "chaos-pre", 50, 2000, 2
	reqs, counts := runRecord(t, cfg)
	if counts[OpPut][ResultOK] != 50 || len(reqs) != 50 {
		t.Fatalf("counts %v, %d records: want 50 ok writes", counts, len(reqs))
	}
	for i := range 50 {
		if _, ok := a.kv.data[KeyName("chaos-pre", i)]; !ok {
			t.Fatalf("key %d not written", i)
		}
	}
}

func TestRecordClassifiesFailuresAndDrops(t *testing.T) {
	failing := startPod(t, nil)
	failing.kv.putErr = status.Error(codes.Unavailable, `write quorum not reached for key "x": 1/3 acks, need W=2`)
	cfg := baseConfig("f", Target{"kv-0", failing.addr})
	cfg.WriteRate, cfg.Duration = 100, 200*time.Millisecond
	_, counts := runRecord(t, cfg)
	if counts[OpPut][ResultFailed] == 0 || counts[OpPut][ResultOK] != 0 {
		t.Errorf("a node's quorum error: %v, want failed", counts)
	}

	slow := startPod(t, nil)
	slow.kv.delay = time.Second
	cfg = baseConfig("s", Target{"kv-0", slow.addr})
	cfg.WriteRate, cfg.Duration, cfg.Timeout, cfg.Concurrency = 100, 300*time.Millisecond, 100*time.Millisecond, 2
	_, counts = runRecord(t, cfg)
	if counts[OpPut][ResultUnknown] == 0 || counts[OpPut][ResultDropped] == 0 {
		t.Errorf("timeouts with two workers: %v, want unknown and dropped", counts)
	}
}

func TestRecordStopsOnTheStopFile(t *testing.T) {
	a := startPod(t, nil)
	stop := filepath.Join(t.TempDir(), "STOP")
	cfg := baseConfig("s", Target{"kv-0", a.addr})
	cfg.WriteRate, cfg.StopFile = 50, stop
	time.AfterFunc(300*time.Millisecond, func() { os.WriteFile(stop, nil, 0o644) })
	start := time.Now()
	runRecord(t, cfg)
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("run took %v after the stop file", d)
	}
}

// Once a pod's connection breaks, requests go to the others: the recorder
// is a client that avoids pods it cannot reach.
func TestRecordAvoidsAPodThatIsGone(t *testing.T) {
	a, b := startPod(t, nil), startPod(t, nil)
	cfg := baseConfig("g", Target{"kv-0", a.addr}, Target{"kv-1", b.addr})
	cfg.WriteRate, cfg.Duration = 200, 1200*time.Millisecond
	gone := time.Now().Add(400 * time.Millisecond)
	time.AfterFunc(time.Until(gone), b.srv.Stop)
	reqs, _ := runRecord(t, cfg)
	late := 0
	for _, r := range reqs {
		if r.Start < gone.Add(300*time.Millisecond).UnixNano() {
			continue
		}
		late++
		if r.Target != "kv-0" || r.Result != ResultOK {
			t.Errorf("after kv-1 went away: %+v", r)
		}
	}
	if late == 0 {
		t.Fatal("no requests after kv-1 went away")
	}
}

func TestRecordNeedsEveryConnectionAtTheStart(t *testing.T) {
	a := startPod(t, nil)
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	dead := lis.Addr().String()
	lis.Close()
	cfg := baseConfig("c", Target{"kv-0", a.addr}, Target{"kv-1", dead})
	cfg.WriteRate, cfg.Duration, cfg.ConnectWait = 10, time.Second, 500*time.Millisecond
	if _, err := Record(context.Background(), cfg, &bytes.Buffer{}); err == nil {
		t.Fatal("recorded with a pod that never connected")
	}
}

func TestClassify(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want Result
	}{
		{nil, ResultOK},
		{status.Error(codes.Unavailable, `write quorum not reached for key "k": 1/3 acks, need W=2`), ResultFailed},
		{status.Error(codes.Unavailable, `read quorum not reached for key "k": 1/3 responses, need R=2`), ResultFailed},
		{status.Error(codes.Unavailable, `forward put for key "k": no replica accepted the write: …`), ResultFailed},
		{status.Error(codes.Unavailable, "connection error: desc = \"transport: Error while dialing\""), ResultUnknown},
		{status.Error(codes.DeadlineExceeded, "context deadline exceeded"), ResultUnknown},
		{status.Error(codes.Canceled, "context canceled"), ResultUnknown},
		{status.Error(codes.Internal, "value cannot be returned"), ResultFailed},
		{errors.New("not a status"), ResultFailed},
	} {
		if got, _, _ := Classify(tc.err); got != tc.want {
			t.Errorf("Classify(%v) = %s, want %s", tc.err, got, tc.want)
		}
	}
}

func TestValue(t *testing.T) {
	if v := Value("k", 8); v != "k|xxxxxx" || !ValueMatches("k", v) || ValueMatches("k2", v) {
		t.Errorf("Value = %q", v)
	}
	if v := Value("a-long-key", 4); v != "a-long-key|" {
		t.Errorf("a value shorter than its key: %q", v)
	}
	if getResult("k", true, []string{"other", Value("k", 5)}) != ResultOK || getResult("k", true, []string{"x"}) != ResultMismatch || getResult("k", false, nil) != ResultNotFound {
		t.Error("getResult")
	}
}

func TestPutKeys(t *testing.T) {
	reqs := []Request{
		{Op: OpPut, Key: "a", Result: ResultOK}, {Op: OpGet, Key: "b", Result: ResultOK},
		{Op: OpPut, Key: "c", Result: ResultDropped}, {Op: OpPut, Key: "d", Result: ResultUnknown},
		{Op: OpPut, Key: "a", Result: ResultFailed},
	}
	if got := fmt.Sprint(PutKeys(reqs)); got != "[a d]" {
		t.Errorf("PutKeys = %s", got)
	}
}
