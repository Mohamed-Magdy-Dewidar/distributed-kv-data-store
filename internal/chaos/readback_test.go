package chaos

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"distributed-kv-datastore/internal/node"
)

func TestReadbackCountsCopies(t *testing.T) {
	full, two := "chaos-t1-0000000", "chaos-t1-0000001"
	data := map[string]string{full: Value(full, 16), two: Value(two, 16)}
	pods := []*fakePod{startPod(t, data), startPod(t, data), startPod(t, data)}
	members := map[string]string{}
	var targets []Target
	for i, p := range pods {
		id := fmt.Sprintf("kv-%d", i)
		members[id] = p.addr
		targets = append(targets, Target{id, p.addr})
		p.repl.held[full] = data[full]
		if i < 2 {
			p.repl.held[two] = data[two]
		}
	}
	pods[2].repl.held["chaos-t1-0000002"] = "someone else's value"

	cfg := ReadbackConfig{
		Targets: targets, Copies: true, N: 3,
		Membership:  MembershipBody{Members: members, Fingerprint: node.Fingerprint(members, 3)},
		Concurrency: 4, Timeout: time.Second, ConnectWait: 5 * time.Second,
	}
	var out bytes.Buffer
	counts, err := Readback(context.Background(), cfg, []string{full, two, "chaos-t1-0000002"}, &out)
	if err != nil {
		t.Fatal(err)
	}
	if counts[ResultOK] != 2 || counts[ResultNotFound] != 1 {
		t.Errorf("counts %v", counts)
	}
	entries, err := ReadReadback(&out)
	if err != nil {
		t.Fatal(err)
	}
	byKey := map[string]ReadbackEntry{}
	for _, e := range entries {
		byKey[e.Key] = e
	}
	for key, want := range map[string]int{full: 3, two: 2, "chaos-t1-0000002": 0} {
		e := byKey[key]
		if len(e.Owners) != 3 || len(e.Holders) != want || len(e.Unreadable) != 0 {
			t.Errorf("%s: %+v, want 3 owners and %d holders", key, e, want)
		}
	}
	if !slices.Contains(byKey[two].Holders, "kv-0") || slices.Contains(byKey[two].Holders, "kv-2") {
		t.Errorf("holders of %s: %v", two, byKey[two].Holders)
	}
}

func TestReadbackRefusesAForeignRing(t *testing.T) {
	p := startPod(t, nil)
	members := map[string]string{"kv-0": p.addr}
	cfg := ReadbackConfig{
		Targets: []Target{{"kv-0", p.addr}}, Copies: true, N: 1,
		Membership:  MembershipBody{Members: members, Fingerprint: "not-the-nodes"},
		Concurrency: 1, Timeout: time.Second, ConnectWait: time.Second,
	}
	if _, err := Readback(context.Background(), cfg, []string{"k"}, &bytes.Buffer{}); err == nil {
		t.Fatal("counted copies on a ring whose fingerprint is not the membership's")
	}
}
