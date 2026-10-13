// Command kvchaos prepares, records and checks the failure tests of
// docs/gke.md (session 3). It ships in the node image, next to /kvnode.
//
//	kvchaos topology  capture membership, zones and quorum settings (offline,
//	                  from kubectl's JSON and each pod's /admin/membership)
//	kvchaos predict   which keys each fault should make fail
//	kvchaos record    run a load and record every request (in the cluster)
//	kvchaos readback  read back every write, optionally counting copies (in
//	                  the cluster)
//	kvchaos check     compare the records with the prediction, and the
//	                  read-back with the acknowledged writes
//	kvchaos cat FILE  print a file (the image has no shell: kubectl exec
//	                  uses this to fetch a run's output)
//	kvchaos stop FILE create FILE, which ends a record run watching it
//	kvchaos hold      wait for SIGTERM (keeps a pod up after a run)
//
// Run kvchaos <command> -h for each command's flags.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"distributed-kv-datastore/internal/chaos"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cmds := map[string]func(context.Context, []string) error{
		"topology": topology, "predict": predict, "record": record, "readback": readback,
		"check": check, "cat": cat, "stop": stopFile, "hold": hold,
	}
	run, ok := cmds[os.Args[1]]
	if !ok {
		usage()
	}
	if err := run(ctx, os.Args[2:]); err != nil {
		fmt.Fprintf(os.Stderr, "kvchaos %s: %v\n", os.Args[1], err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: kvchaos topology|predict|record|readback|check|cat|stop|hold [flags]")
	os.Exit(2)
}

// errFailed is a check that ran and did not pass: exit 1, without a message
// beyond the report's.
var errFailed = errors.New("check failed")

func topology(_ context.Context, args []string) error {
	fs := flag.NewFlagSet("topology", flag.ExitOnError)
	pods := fs.String("pods", "", "kubectl get pods -o json of the cluster's pods")
	nodes := fs.String("nodes", "", "kubectl get nodes -o json")
	cm := fs.String("configmap", "", "kubectl get configmap <cluster>-config -o json")
	dir := fs.String("membership-dir", "", "a directory of <member>.json files, each that pod's GET /admin/membership")
	out := fs.String("out", "-", "where to write the topology (JSON)")
	fs.Parse(args)

	var in chaos.TopologyInputs
	var err error
	for _, f := range []struct {
		dst  *[]byte
		path string
		flag string
	}{{&in.Pods, *pods, "pods"}, {&in.Nodes, *nodes, "nodes"}, {&in.ConfigMap, *cm, "configmap"}} {
		if f.path == "" {
			return fmt.Errorf("-%s is required", f.flag)
		}
		if *f.dst, err = os.ReadFile(f.path); err != nil {
			return err
		}
	}
	if *dir == "" {
		return errors.New("-membership-dir is required")
	}
	files, err := filepath.Glob(filepath.Join(*dir, "*.json"))
	if err != nil {
		return err
	}
	in.Memberships = map[string][]byte{}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		in.Memberships[strings.TrimSuffix(filepath.Base(f), ".json")] = b
	}
	t, err := chaos.BuildTopology(in)
	if err != nil {
		return err
	}
	b, err := t.JSON()
	if err != nil {
		return err
	}
	return writeOut(*out, func(w io.Writer) error {
		_, err := w.Write(append(b, '\n'))
		return err
	})
}

func loadTopology(path string) (*chaos.Topology, error) {
	if path == "" {
		return nil, errors.New("-topology is required")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return chaos.LoadTopology(b)
}

func predict(_ context.Context, args []string) error {
	fs := flag.NewFlagSet("predict", flag.ExitOnError)
	topo := fs.String("topology", "", "the topology (kvchaos topology)")
	keySpec := fs.String("keys", "", "the keys, PREFIX:COUNT (KeyName(PREFIX, 0 … COUNT-1))")
	keysFile := fs.String("keys-file", "", "the keys, one per line, instead of -keys")
	faultSpec := fs.String("fault", "", "one fault (none, down:, zone:, partition:, partition-zone:); default: every pod, zone and zone partition")
	perKey := fs.String("per-key", "", "with -fault: write every key's prediction here (JSON lines)")
	fs.Parse(args)

	t, err := loadTopology(*topo)
	if err != nil {
		return err
	}
	keys, err := readKeys(*keySpec, *keysFile)
	if err != nil {
		return err
	}
	faults := t.StandardFaults()
	if *faultSpec != "" {
		f, err := chaos.ParseFault(*faultSpec, t)
		if err != nil {
			return err
		}
		faults = []chaos.Fault{f}
	} else if *perKey != "" {
		return errors.New("-per-key needs -fault")
	}

	w := os.Stdout
	fmt.Fprintf(w, "topology: epoch %d, %d pods, N=%d W=%d R=%d\n", t.Epoch, len(t.Pods), t.N, t.W, t.R)
	zones := t.Zones()
	for _, z := range t.SortedZones() {
		fmt.Fprintf(w, "  zone %s: %s\n", z, strings.Join(zones[z], " "))
	}
	fmt.Fprintf(w, "keys: %d (%s … %s)\n", len(keys), keys[0], keys[len(keys)-1])
	share := t.Share(keys)
	for _, p := range t.Pods {
		fmt.Fprintf(w, "  %s holds %5.1f%%\n", p.ID, 100*share[p.ID])
	}
	pct := func(n int) string { return fmt.Sprintf("%6d (%5.1f%%)", n, 100*float64(n)/float64(len(keys))) }
	fmt.Fprintf(w, "%-44s %-16s %-16s %s\n", "fault", "puts fail", "gets fail", "from")
	for _, f := range faults {
		s := t.Summarize(f, keys)
		for _, side := range slices.Sorted(maps.Keys(s.Fails)) {
			fmt.Fprintf(w, "%-44s %-16s %-16s %s\n", f, pct(s.Fails[side][chaos.OpPut]), pct(s.Fails[side][chaos.OpGet]), side)
		}
		if f.Kind == chaos.FaultPartition {
			fmt.Fprintf(w, "%-44s %-16s %-16s %s\n", f, pct(s.Nowhere[chaos.OpPut]), pct(s.Nowhere[chaos.OpGet]), "both sides")
		}
	}
	if *perKey == "" {
		return nil
	}
	return writeOut(*perKey, func(out io.Writer) error {
		enc := json.NewEncoder(out)
		for _, k := range keys {
			if err := enc.Encode(t.PredictKey(faults[0], k)); err != nil {
				return err
			}
		}
		return nil
	})
}

func readKeys(spec, file string) ([]string, error) {
	switch {
	case spec != "" && file != "":
		return nil, errors.New("give -keys or -keys-file, not both")
	case spec != "":
		return chaos.ParseKeySpec(spec)
	case file != "":
		b, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		keys := strings.Fields(string(b))
		if len(keys) == 0 {
			return nil, fmt.Errorf("%s: no keys", file)
		}
		return keys, nil
	}
	return nil, errors.New("-keys or -keys-file is required")
}

// defaultAdmin is the membership of the KVCluster kv in the namespace
// kvstore, from kv-0 or else kv-1.
const defaultAdmin = "http://kv-0.kv.kvstore.svc.cluster.local:8080/admin/membership," +
	"http://kv-1.kv.kvstore.svc.cluster.local:8080/admin/membership"

// targetFlags are how record and readback find the pods: listed, or from a
// member's GET /admin/membership.
type targetFlags struct {
	targets, admin *string
}

func addTargetFlags(fs *flag.FlagSet) targetFlags {
	return targetFlags{
		targets: fs.String("targets", "", "the pods, id=host:port,… (default: the members, from -admin)"),
		admin:   fs.String("admin", defaultAdmin, "GET /admin/membership URLs, comma-separated, tried in turn"),
	}
}

func (tf targetFlags) resolve(ctx context.Context) ([]chaos.Target, chaos.MembershipBody, error) {
	if *tf.targets != "" {
		var ts []chaos.Target
		for kv := range strings.SplitSeq(*tf.targets, ",") {
			id, addr, ok := strings.Cut(kv, "=")
			if !ok {
				return nil, chaos.MembershipBody{}, fmt.Errorf("-targets %q: want id=host:port", kv)
			}
			ts = append(ts, chaos.Target{ID: id, Address: addr})
		}
		return ts, chaos.MembershipBody{}, nil
	}
	m, err := chaos.FetchMembership(ctx, strings.Split(*tf.admin, ","))
	if err != nil {
		return nil, m, err
	}
	return m.Targets(), m, nil
}

func record(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("record", flag.ExitOnError)
	var cfg chaos.RecordConfig
	fs.StringVar(&cfg.Test, "test", "", "the test's name (required): every record carries it, and writes go to chaos-<test>-<n>")
	tf := addTargetFlags(fs)
	fs.StringVar(&cfg.Service, "service", "", "send through this Service address instead of to each pod (the realism run)")
	fs.IntVar(&cfg.ServiceConns, "service-conns", 12, "with -service: connections to it")
	fs.StringVar(&cfg.WritePrefix, "write-prefix", "", "write keys' prefix (default chaos-<test>-<run>, <run> the start time: a rerun of a test never writes the keys an earlier run did)")
	fs.IntVar(&cfg.Writes, "writes", 0, "write this many keys and stop, never dropping one (a preload); 0: until the run ends")
	fs.Float64Var(&cfg.WriteRate, "write-rate", 100, "writes per second (0: none)")
	fs.StringVar(&cfg.ReadPrefix, "read-prefix", "chaos-pre", "read keys' prefix")
	fs.IntVar(&cfg.ReadKeys, "read-keys", 0, "read among this many keys of -read-prefix")
	fs.Float64Var(&cfg.ReadRate, "read-rate", 0, "reads per second (0: none)")
	fs.IntVar(&cfg.ValueSize, "value-size", 256, "bytes per value")
	fs.IntVar(&cfg.Concurrency, "concurrency", 64, "requests in flight at most; a request due when all are busy is dropped (and recorded)")
	fs.DurationVar(&cfg.Timeout, "timeout", 5*time.Second, "per request")
	fs.DurationVar(&cfg.ConnectWait, "connect-wait", 30*time.Second, "for every connection to be ready before the first request")
	fs.DurationVar(&cfg.Duration, "duration", 0, "end the run after this long (0: no limit)")
	fs.StringVar(&cfg.StopFile, "stop-file", "", "end the run once this file exists (kvchaos stop)")
	fs.Uint64Var(&cfg.Seed, "seed", 1, "seed of the read keys' choice")
	out := fs.String("out", "-", "where to write the records (JSON lines)")
	holdAfter := fs.Bool("hold", false, "after the run, wait for SIGTERM")
	fs.Parse(args)

	if cfg.Test == "" {
		return errors.New("-test is required")
	}
	if cfg.WritePrefix == "" {
		cfg.WritePrefix = "chaos-" + cfg.Test + "-" + strconv.FormatInt(time.Now().Unix(), 36)
	}
	if cfg.Service == "" {
		ts, _, err := tf.resolve(ctx)
		if err != nil {
			return err
		}
		cfg.Targets = ts
	}
	cfg.Ready = func(conns []string) {
		fmt.Fprintf(os.Stderr, "kvchaos record %s: connections ready: %s\n", cfg.Test, strings.Join(conns, " "))
	}
	err := writeOut(*out, func(w io.Writer) error {
		start := time.Now()
		counts, err := chaos.Record(ctx, cfg, w)
		fmt.Fprintf(os.Stderr, "kvchaos record %s: %v, %s\n", cfg.Test, time.Since(start).Round(time.Second), countsText(counts))
		return err
	})
	if err != nil {
		return err
	}
	if *holdAfter {
		return hold(ctx, nil)
	}
	return nil
}

func countsText(c chaos.Counts) string {
	var parts []string
	for _, op := range slices.Sorted(maps.Keys(c)) {
		for _, r := range slices.Sorted(maps.Keys(c[op])) {
			parts = append(parts, fmt.Sprintf("%s %s=%d", op, r, c[op][r]))
		}
	}
	return strings.Join(parts, ", ")
}

func readback(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("readback", flag.ExitOnError)
	var cfg chaos.ReadbackConfig
	tf := addTargetFlags(fs)
	records := fs.String("records", "-", "records files, comma-separated, or - for stdin: every key written is read back")
	keysFile := fs.String("keys-file", "", "read back these keys (one per line) instead of the records'")
	fs.BoolVar(&cfg.Copies, "copies", false, "also ask each owner for its own copy (needs the membership from -admin)")
	fs.IntVar(&cfg.N, "n", 3, "with -copies: the replication factor N (checked against the membership's fingerprint)")
	fs.IntVar(&cfg.Concurrency, "concurrency", 32, "keys in flight")
	fs.DurationVar(&cfg.Timeout, "timeout", 5*time.Second, "per request")
	fs.DurationVar(&cfg.ConnectWait, "connect-wait", 30*time.Second, "for every connection to be ready")
	out := fs.String("out", "-", "where to write the read-back (JSON lines)")
	holdAfter := fs.Bool("hold", false, "after the run, wait for SIGTERM")
	fs.Parse(args)

	var keys []string
	if *keysFile != "" {
		var err error
		if keys, err = readKeys("", *keysFile); err != nil {
			return err
		}
	} else {
		reqs, err := readRecordFiles(*records)
		if err != nil {
			return err
		}
		keys = chaos.PutKeys(reqs)
	}
	ts, m, err := tf.resolve(ctx)
	if err != nil {
		return err
	}
	cfg.Targets, cfg.Membership = ts, m
	if cfg.Copies && m.Members == nil {
		return errors.New("-copies needs the membership: use -admin, not -targets")
	}
	err = writeOut(*out, func(w io.Writer) error {
		start := time.Now()
		counts, err := chaos.Readback(ctx, cfg, keys, w)
		fmt.Fprintf(os.Stderr, "kvchaos readback: %d keys in %v: %v\n", len(keys), time.Since(start).Round(time.Second), counts)
		return err
	})
	if err != nil {
		return err
	}
	if *holdAfter {
		return hold(ctx, nil)
	}
	return nil
}

func readRecordFiles(list string) ([]chaos.Request, error) {
	var all []chaos.Request
	for f := range strings.SplitSeq(list, ",") {
		var r io.Reader = os.Stdin
		if f != "-" {
			fh, err := os.Open(f)
			if err != nil {
				return nil, err
			}
			defer fh.Close()
			r = fh
		}
		reqs, err := chaos.ReadRecords(r)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", f, err)
		}
		all = append(all, reqs...)
	}
	return all, nil
}

func check(_ context.Context, args []string) error {
	fs := flag.NewFlagSet("check", flag.ExitOnError)
	var cfg chaos.CheckConfig
	topo := fs.String("topology", "", "the topology the prediction was made from")
	events := fs.String("events", "", "the fault timeline (JSON lines; see internal/chaos.Event)")
	records := fs.String("records", "", "records files, comma-separated")
	rb := fs.String("readback", "", "read-back files, comma-separated (each key's latest entry counts)")
	fs.BoolVar(&cfg.Converged, "converged", false, "every acknowledged write must be on all its owners")
	fs.DurationVar(&cfg.Tolerance, "tolerance", 2*time.Second, "widen every window by this much (clock differences)")
	fs.DurationVar(&cfg.Detection, "detection", 0, "time to notice a fault (default: from the topology's timing)")
	fs.DurationVar(&cfg.Recovery, "recovery", 0, "time to notice a heal (default: from the topology's timing)")
	fs.IntVar(&cfg.Examples, "examples", 5, "requests to show per test and bucket")
	fs.Parse(args)

	var err error
	if cfg.Topology, err = loadTopology(*topo); err != nil {
		return err
	}
	if *records == "" {
		return errors.New("-records is required")
	}
	if cfg.Records, err = readRecordFiles(*records); err != nil {
		return err
	}
	if *events != "" {
		f, err := os.Open(*events)
		if err != nil {
			return err
		}
		defer f.Close()
		if cfg.Events, err = chaos.ReadEvents(f); err != nil {
			return err
		}
	}
	if *rb != "" {
		for path := range strings.SplitSeq(*rb, ",") {
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			entries, err := chaos.ReadReadback(f)
			f.Close()
			if err != nil {
				return fmt.Errorf("%s: %w", path, err)
			}
			cfg.Readback = append(cfg.Readback, entries...)
		}
	}
	rep, err := chaos.Check(cfg)
	if err != nil {
		return err
	}
	rep.WriteText(os.Stdout)
	if !rep.OK() {
		return errFailed
	}
	return nil
}

func cat(_ context.Context, args []string) error {
	if len(args) != 1 {
		return errors.New("usage: kvchaos cat FILE")
	}
	f, err := os.Open(args[0])
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(os.Stdout, f)
	return err
}

func stopFile(_ context.Context, args []string) error {
	if len(args) != 1 {
		return errors.New("usage: kvchaos stop FILE")
	}
	return os.WriteFile(args[0], nil, 0o644)
}

func hold(ctx context.Context, _ []string) error {
	<-ctx.Done()
	return nil
}

// writeOut runs write on path ("-": stdout), and syncs and closes the file.
func writeOut(path string, write func(io.Writer) error) error {
	if path == "-" {
		return write(os.Stdout)
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := write(f); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
