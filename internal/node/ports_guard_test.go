package node

import (
	"strings"
	"testing"
)

// knownAddr must never hand out a port past this package's range: another
// package's tests may be using it at the same time. At the end of the range it
// panics instead.
func TestKnownAddrPanicsAtTheEndOfItsRange(t *testing.T) {
	saved := nextKnownPort.Load()
	defer nextKnownPort.Store(saved)

	nextKnownPort.Store(knownPortCount - 1)
	if last := knownAddr(); last != "127.0.0.1:24999" {
		t.Fatalf("the last port in range is %s, want 127.0.0.1:24999", last)
	}
	defer func() {
		r := recover()
		if r == nil || !strings.Contains(r.(string), "used up") {
			t.Fatalf("knownAddr past the range: recovered %v, want a panic saying the range is used up", r)
		}
	}()
	addr := knownAddr()
	t.Fatalf("knownAddr handed out %s past the end of its range", addr)
}
