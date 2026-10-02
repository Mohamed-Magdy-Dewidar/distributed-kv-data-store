package node

import "testing"

// TestStatsReportsTheEpochOfTheHeldView: Stats follows the view the node
// holds, not the one it started with.
func TestStatsReportsTheEpochOfTheHeldView(t *testing.T) {
	nd, members := twoNode(t)
	if got := nd.Stats().Epoch; got != 0 {
		t.Fatalf("Stats().Epoch = %d before any SetMembership, want 0", got)
	}
	if _, err := nd.SetMembership(3, members); err != nil {
		t.Fatalf("SetMembership(3): %v", err)
	}
	if got := nd.Stats().Epoch; got != 3 {
		t.Fatalf("Stats().Epoch = %d after SetMembership(3), want 3", got)
	}
}
