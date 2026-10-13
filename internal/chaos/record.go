package chaos

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"distributed-kv-datastore/internal/rpc/pb"
)

// Result is what a request got, as the client saw it.
type Result string

const (
	// ResultOK: a Put acknowledged, or a Get that returned the key's value.
	ResultOK Result = "ok"
	// ResultFailed: the server answered with an error (a missed quorum, no
	// replica accepting a forwarded write). The write may still persist: a
	// replica that applied it keeps it (docs/known-limitations.md).
	ResultFailed Result = "failed"
	// ResultUnknown: no answer (a deadline, a broken connection). The write
	// may or may not persist.
	ResultUnknown Result = "unknown"
	// ResultNotFound: a Get that found no value.
	ResultNotFound Result = "notfound"
	// ResultMismatch: a Get whose values do not include the key's.
	ResultMismatch Result = "mismatch"
	// ResultDropped: never sent; every worker was busy at its scheduled
	// time (open model).
	ResultDropped Result = "dropped"
)

// Request is one line of a recorder's output.
type Request struct {
	Test   string `json:"test"`
	Seq    int64  `json:"seq"`
	Op     Op     `json:"op"`
	Key    string `json:"key"`
	Target string `json:"target,omitempty"` // a pod ID, or svc-<i> for a connection through the Service
	Start  int64  `json:"start"`            // Unix nanoseconds
	End    int64  `json:"end"`
	Result Result `json:"result"`
	Code   string `json:"code,omitempty"`
	Msg    string `json:"msg,omitempty"`
}

// serverPhrases are the messages of the errors a node returns itself (see
// internal/node: Put, putAsReplica, forwardPut, Get). A codes.Unavailable
// with one of them is an answer; any other is a connection that failed.
var serverPhrases = []string{"quorum not reached", "no replica accepted", "local write failed", "local read failed"}

// maxMsg bounds the error message kept per request.
const maxMsg = 160

// Classify is the result of a request that returned err (nil: ok), with the
// gRPC code and message when err is not nil.
//
// Canceled and DeadlineExceeded are unknown, as is an Unavailable that is
// not one of the nodes' own errors: gRPC reports a broken or refused
// connection as Unavailable too, and the nodes map every error of their own
// that is not a context's to Unavailable (internal/rpc, clientError), so
// the message is what tells them apart. Any other code is failed.
func Classify(err error) (Result, string, string) {
	if err == nil {
		return ResultOK, "", ""
	}
	st := status.Convert(err)
	msg := st.Message()
	if len(msg) > maxMsg {
		msg = msg[:maxMsg]
	}
	switch st.Code() {
	case codes.Canceled, codes.DeadlineExceeded:
		return ResultUnknown, st.Code().String(), msg
	case codes.Unavailable:
		for _, p := range serverPhrases {
			if strings.Contains(st.Message(), p) {
				return ResultFailed, st.Code().String(), msg
			}
		}
		return ResultUnknown, st.Code().String(), msg
	}
	return ResultFailed, st.Code().String(), msg
}

// Value is the value the recorder writes under key: the key, a "|", and
// "x" padding to size bytes in all.
func Value(key string, size int) string {
	v := key + "|"
	if pad := size - len(v); pad > 0 {
		v += strings.Repeat("x", pad)
	}
	return v
}

// ValueMatches reports whether v is a value the recorder writes under key,
// whatever its size.
func ValueMatches(key, v string) bool { return strings.HasPrefix(v, key+"|") }

// ReadRecords reads a recorder's output.
func ReadRecords(r io.Reader) ([]Request, error) {
	var out []Request
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for line := 1; sc.Scan(); line++ {
		if len(strings.TrimSpace(sc.Text())) == 0 {
			continue
		}
		var req Request
		if err := json.Unmarshal(sc.Bytes(), &req); err != nil {
			return nil, fmt.Errorf("records line %d: %w", line, err)
		}
		out = append(out, req)
	}
	return out, sc.Err()
}

// PutKeys is every key a Put was sent for (dropped ones excluded), once
// each, in order of first appearance.
func PutKeys(reqs []Request) []string {
	seen := map[string]bool{}
	var keys []string
	for _, r := range reqs {
		if r.Op == OpPut && r.Result != ResultDropped && !seen[r.Key] {
			seen[r.Key] = true
			keys = append(keys, r.Key)
		}
	}
	return keys
}

// Target is a pod the recorder sends requests to.
type Target struct {
	ID, Address string
}

// FetchMembership GETs /admin/membership from each URL in turn and returns
// the first answer.
func FetchMembership(ctx context.Context, urls []string) (MembershipBody, error) {
	var errs []error
	client := &http.Client{Timeout: 3 * time.Second}
	for _, u := range urls {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return MembershipBody{}, err
		}
		resp, err := client.Do(req)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		var m MembershipBody
		err = json.NewDecoder(resp.Body).Decode(&m)
		resp.Body.Close()
		if err == nil && resp.StatusCode != http.StatusOK {
			err = fmt.Errorf("%s: %s", u, resp.Status)
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
		return m, nil
	}
	return MembershipBody{}, fmt.Errorf("no membership from %v: %w", urls, errors.Join(errs...))
}

// Targets is m's members as targets, in ID order.
func (m MembershipBody) Targets() []Target {
	var ts []Target
	for _, id := range slices.Sorted(maps.Keys(m.Members)) {
		ts = append(ts, Target{ID: id, Address: m.Members[id]})
	}
	return ts
}

// conn is one client connection: to a pod, or through the Service.
type conn struct {
	id string
	cc *grpc.ClientConn
	kv pb.KVClientClient
}

// pool is the recorder's connections, picked in turn.
type pool struct {
	conns []*conn
	next  atomic.Uint64
	// direct: a pick skips a connection that is not ready (a client-side
	// balancer that avoids pods it cannot reach); a Service connection is
	// used whatever its state, as a naive client would.
	direct bool
}

// Keepalive is a direct connection's gRPC keepalive: a connection with a
// request outstanding and nothing received for Time is pinged, and closed
// if the ping is not answered within Timeout. Without it a connection to a
// VM that vanished (no FIN, no RST) hangs until TCP gives up. grpc-go
// raises a Time under 10s to 10s, so a vanished pod's connection is closed
// about 13s after its last answer; until then the pod is still picked, and
// its requests time out. They are recorded as unknown, and the check, which
// predicts every request sent to a pod that is down as target-down, counts
// them as predicted.
var Keepalive = keepalive.ClientParameters{Time: 10 * time.Second, Timeout: 3 * time.Second}

// dialPool opens a connection to every target (direct), or conns connections
// to service, and waits until all are ready: every connection is open before
// any fault.
func dialPool(ctx context.Context, targets []Target, service string, conns int, wait time.Duration) (*pool, error) {
	p := &pool{direct: service == ""}
	reconnect := backoff.DefaultConfig
	reconnect.MaxDelay = 5 * time.Second // the nodes' timeouts.maxReconnectBackoff
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: reconnect, MinConnectTimeout: 5 * time.Second}),
	}
	add := func(id, addr string, extra ...grpc.DialOption) error {
		cc, err := grpc.NewClient(addr, append(slices.Clone(opts), extra...)...)
		if err != nil {
			return fmt.Errorf("dial %s at %s: %w", id, addr, err)
		}
		p.conns = append(p.conns, &conn{id: id, cc: cc, kv: pb.NewKVClientClient(cc)})
		return nil
	}
	if p.direct {
		for _, t := range targets {
			if err := add(t.ID, t.Address, grpc.WithKeepaliveParams(Keepalive)); err != nil {
				p.close()
				return nil, err
			}
		}
	} else {
		for i := range conns {
			if err := add(fmt.Sprintf("svc-%d", i), service); err != nil {
				p.close()
				return nil, err
			}
		}
	}
	if len(p.conns) == 0 {
		return nil, errors.New("no targets")
	}
	wctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	for _, c := range p.conns {
		c.cc.Connect()
		for s := c.cc.GetState(); s != connectivity.Ready; s = c.cc.GetState() {
			if !c.cc.WaitForStateChange(wctx, s) {
				p.close()
				return nil, fmt.Errorf("connection %s not ready within %v (state %v)", c.id, wait, s)
			}
		}
	}
	return p, nil
}

func (p *pool) pick() *conn {
	start := int(p.next.Add(1) - 1)
	first := p.conns[start%len(p.conns)]
	if !p.direct {
		return first
	}
	for i := range p.conns {
		c := p.conns[(start+i)%len(p.conns)]
		switch c.cc.GetState() {
		case connectivity.Ready:
			return c
		case connectivity.Idle:
			c.cc.Connect() // reconnect, and use it once it is ready
		}
	}
	return first // none ready: send anyway, and record what happens
}

func (p *pool) close() {
	for _, c := range p.conns {
		c.cc.Close()
	}
}

// RecordConfig is a recorder run.
type RecordConfig struct {
	Test string
	// Targets are the pods to send to directly, in turn; or, if Service is
	// set, ServiceConns connections to the Service instead.
	Targets      []Target
	Service      string
	ServiceConns int

	// Writes: Puts of KeyName(WritePrefix, 0), (…, 1), …, at WriteRate per
	// second. Writes > 0 writes that many and then stops, never dropping
	// one (a preload); 0 writes until the run ends.
	WritePrefix string
	Writes      int
	WriteRate   float64
	// Reads: Gets of random keys among KeyName(ReadPrefix, 0 … ReadKeys-1),
	// at ReadRate per second, each checked for its value.
	ReadPrefix string
	ReadKeys   int
	ReadRate   float64

	ValueSize   int
	Concurrency int           // requests in flight at most; more are dropped
	Timeout     time.Duration // per request
	ConnectWait time.Duration // for every connection to be ready at the start

	// The run ends at Duration (0: no limit), when StopFile exists, when
	// ctx is done, or when a fixed number of writes is done and there are
	// no reads.
	Duration time.Duration
	StopFile string
	Seed     uint64

	// Ready, if set, is called once every connection is ready, before the
	// first request, with their names.
	Ready func(conns []string)
}

// Counts counts requests by op and result.
type Counts map[Op]map[Result]int

func (c Counts) add(op Op, r Result) {
	if c[op] == nil {
		c[op] = map[Result]int{}
	}
	c[op][r]++
}

type job struct {
	seq int64
	op  Op
	key string
}

// Record runs cfg's load, writing every request to out as a JSON line as it
// completes, and returns the counts. In-flight requests finish after the
// run ends; they are not canceled.
func Record(ctx context.Context, cfg RecordConfig, out io.Writer) (Counts, error) {
	if cfg.WriteRate <= 0 && cfg.ReadRate <= 0 {
		return nil, errors.New("record: no writes and no reads")
	}
	if cfg.ReadRate > 0 && (cfg.ReadPrefix == "" || cfg.ReadKeys <= 0) {
		return nil, errors.New("record: reads need a read prefix and a key count")
	}
	if cfg.WriteRate > 0 && cfg.WritePrefix == "" {
		return nil, errors.New("record: writes need a write prefix")
	}
	if cfg.Concurrency <= 0 || cfg.Timeout <= 0 {
		return nil, errors.New("record: concurrency and timeout must be positive")
	}
	p, err := dialPool(ctx, cfg.Targets, cfg.Service, cfg.ServiceConns, cfg.ConnectWait)
	if err != nil {
		return nil, fmt.Errorf("record: %w", err)
	}
	defer p.close()
	if cfg.Ready != nil {
		names := make([]string, len(p.conns))
		for i, c := range p.conns {
			names[i] = c.id
		}
		cfg.Ready(names)
	}

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	if cfg.Duration > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(runCtx, cfg.Duration)
		defer cancel()
	}
	if cfg.StopFile != "" {
		go func() {
			tick := time.NewTicker(time.Second)
			defer tick.Stop()
			for {
				select {
				case <-runCtx.Done():
					return
				case <-tick.C:
					if _, err := os.Stat(cfg.StopFile); err == nil {
						stop()
						return
					}
				}
			}
		}()
	}

	results := make(chan Request, 1024)
	jobs := make(chan job)
	var seq atomic.Int64
	reqCtx := context.WithoutCancel(ctx) // the end of the run does not cancel requests

	var workers sync.WaitGroup
	for range cfg.Concurrency {
		workers.Go(func() {
			for j := range jobs {
				results <- send(reqCtx, p.pick(), cfg, j)
			}
		})
	}

	var gens sync.WaitGroup
	start := time.Now()
	stream := func(op Op, rate float64, fixed int, key func(i int) string) {
		interval := time.Duration(float64(time.Second) / rate)
		timer := time.NewTimer(0)
		defer timer.Stop()
		for i := 0; fixed == 0 || i < fixed; i++ {
			timer.Reset(time.Until(start.Add(time.Duration(i) * interval)))
			select {
			case <-runCtx.Done():
				return
			case <-timer.C:
			}
			j := job{seq: seq.Add(1), op: op, key: key(i)}
			if fixed > 0 {
				select {
				case jobs <- j:
				case <-runCtx.Done():
					return
				}
				continue
			}
			select {
			case jobs <- j:
			default:
				now := time.Now().UnixNano()
				results <- Request{Test: cfg.Test, Seq: j.seq, Op: op, Key: j.key, Start: now, End: now, Result: ResultDropped}
			}
		}
	}
	if cfg.WriteRate > 0 {
		gens.Go(func() {
			stream(OpPut, cfg.WriteRate, cfg.Writes, func(i int) string { return KeyName(cfg.WritePrefix, i) })
			if cfg.Writes > 0 && cfg.ReadRate <= 0 {
				stop() // the preload is done
			}
		})
	}
	if cfg.ReadRate > 0 {
		rng := rand.New(rand.NewPCG(cfg.Seed, 1))
		gens.Go(func() {
			stream(OpGet, cfg.ReadRate, 0, func(int) string { return KeyName(cfg.ReadPrefix, rng.IntN(cfg.ReadKeys)) })
		})
	}

	counts := Counts{}
	written := make(chan error, 1)
	go func() {
		w := bufio.NewWriter(out)
		enc := json.NewEncoder(w)
		flush := time.NewTicker(time.Second)
		defer flush.Stop()
		var werr error
		for {
			select {
			case r, ok := <-results:
				if !ok {
					if err := w.Flush(); werr == nil {
						werr = err
					}
					written <- werr
					return
				}
				counts.add(r.Op, r.Result)
				if err := enc.Encode(r); err != nil && werr == nil {
					werr = err
				}
			case <-flush.C:
				if err := w.Flush(); err != nil && werr == nil {
					werr = err
				}
			}
		}
	}()

	gens.Wait()
	close(jobs)
	workers.Wait()
	close(results)
	if err := <-written; err != nil {
		return counts, fmt.Errorf("record: writing records: %w", err)
	}
	return counts, nil
}

// send sends j on c and returns its record.
func send(ctx context.Context, c *conn, cfg RecordConfig, j job) Request {
	r := Request{Test: cfg.Test, Seq: j.seq, Op: j.op, Key: j.key, Target: c.id}
	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	r.Start = time.Now().UnixNano()
	switch j.op {
	case OpPut:
		_, err := c.kv.Put(ctx, &pb.PutRequest{Key: j.key, Value: Value(j.key, cfg.ValueSize)})
		r.End = time.Now().UnixNano()
		r.Result, r.Code, r.Msg = Classify(err)
	case OpGet:
		resp, err := c.kv.Get(ctx, &pb.GetRequest{Key: j.key})
		r.End = time.Now().UnixNano()
		r.Result, r.Code, r.Msg = Classify(err)
		if err == nil {
			r.Result = getResult(j.key, resp.GetFound(), resp.GetValues())
		}
	}
	return r
}

// getResult is a successful Get's result: ok if one of values is the key's.
func getResult(key string, found bool, values []string) Result {
	if !found {
		return ResultNotFound
	}
	for _, v := range values {
		if ValueMatches(key, v) {
			return ResultOK
		}
	}
	return ResultMismatch
}
