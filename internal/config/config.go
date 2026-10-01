// Package config loads and validates cmd/node's YAML configuration: one
// node's listen addresses, data directory, cluster membership, and the
// durations that govern its background loops and shutdown.
package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// Member is one node in cluster.members, including entries for the node
// reading its own config — a node finds itself by matching NodeID against
// Member.ID, then builds its neighbor set from every other entry.
type Member struct {
	ID      string `yaml:"id"`
	Address string `yaml:"address"`
}

type Listen struct {
	GRPC string `yaml:"grpc"`
	HTTP string `yaml:"http"`
}

type Cluster struct {
	N int `yaml:"n"`
	W int `yaml:"w"`
	R int `yaml:"r"`

	// Epoch versions the member list: raise it whenever Members changes. A
	// node that has already adopted a newer epoch keeps that membership
	// (its persisted one) and ignores this list; one that holds the same
	// epoch with a different list refuses to start. Defaults to 0.

	// so epoch is like a term number that all nodes needs to agree on
	Epoch   uint64   `yaml:"epoch"`
	Members []Member `yaml:"members"`
}

type Storage struct {
	MemtableBytes int `yaml:"memtableBytes"`
}

type Intervals struct {
	Compaction   time.Duration `yaml:"compaction"`
	AntiEntropy  time.Duration `yaml:"antiEntropy"`
	HintDelivery time.Duration `yaml:"hintDelivery"`
	Heartbeat    time.Duration `yaml:"heartbeat"` // default 1s
}

type Timeouts struct {
	Replication         time.Duration `yaml:"replication"`
	MaxReconnectBackoff time.Duration `yaml:"maxReconnectBackoff"`
	Shutdown            time.Duration `yaml:"shutdown"`
	Heartbeat           time.Duration `yaml:"heartbeat"` // default 500ms; must be < intervals.heartbeat
}

type Health struct {
	MaxMissedHeartbeats int `yaml:"maxMissedHeartbeats"` // 3 missed heatbeats and node is considered dead
}

type Config struct {
	NodeID  string  `yaml:"nodeId"`
	Listen  Listen  `yaml:"listen"`
	DataDir string  `yaml:"dataDir"` // required; used as-is, not joined with NodeID
	Cluster Cluster `yaml:"cluster"`
	Storage Storage `yaml:"storage"`

	Intervals Intervals `yaml:"intervals"`
	Timeouts  Timeouts  `yaml:"timeouts"`
	Health    Health    `yaml:"health"`
}

// Defaults for the heartbeat settings, applied by Load to any that are left
// unset (zero).
const (
	defaultHeartbeatInterval   = time.Second
	defaultHeartbeatTimeout    = 500 * time.Millisecond
	defaultMaxMissedHeartbeats = 3
)

// applyDefaults fills in the settings that have defaults, where unset.
func (c *Config) applyDefaults() {
	if c.Intervals.Heartbeat == 0 {
		c.Intervals.Heartbeat = defaultHeartbeatInterval
	}
	if c.Timeouts.Heartbeat == 0 {
		c.Timeouts.Heartbeat = defaultHeartbeatTimeout
	}
	if c.Health.MaxMissedHeartbeats == 0 {
		c.Health.MaxMissedHeartbeats = defaultMaxMissedHeartbeats
	}
}

// Load reads and strictly decodes the YAML file at path (an unknown field
// is an error), overlays KV_NODE_ID onto NodeID if that env var is set,
// then validates the result. See validate for the exact rules.
func Load(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("config: open %s: %w", path, err)
	}
	defer f.Close()

	var cfg Config
	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}

	if envID := os.Getenv("KV_NODE_ID"); envID != "" {
		cfg.NodeID = envID
	}

	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// clockIDSeparator joins a node ID and its incarnation in a vector-clock ID,
// so no node ID may contain it.
const clockIDSeparator = "#"

// validate checks every rule Load requires before a Config is usable:
//   - nodeId is present and matches a member's ID
//   - dataDir is present
//   - no node or member ID contains '#', the separator in vector-clock IDs
//     ("<id>#<incarnation>", see internal/identity)
//   - every member ID is unique, and every member address is unique
//   - 1 <= W, R <= N <= len(members)
//   - every interval and timeout is > 0, and timeouts.heartbeat is shorter
//     than intervals.heartbeat
//   - health.maxMissedHeartbeats is > 0
//   - storage.memtableBytes is > 0
//
// Each error names the offending field, so a misconfigured deployment
// fails with a message that says what to fix rather than just that
// something's wrong.
func (c *Config) validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("config: nodeId is required (or set KV_NODE_ID)")
	}
	if strings.Contains(c.NodeID, clockIDSeparator) {
		return fmt.Errorf("config: nodeId %q must not contain %q (the clock-ID separator)", c.NodeID, clockIDSeparator)
	}

	seenID := make(map[string]bool, len(c.Cluster.Members))
	seenAddr := make(map[string]bool, len(c.Cluster.Members))
	selfFound := false
	for _, m := range c.Cluster.Members {
		if strings.Contains(m.ID, clockIDSeparator) {
			return fmt.Errorf("config: cluster.members: id %q must not contain %q (the clock-ID separator)", m.ID, clockIDSeparator)
		}
		if seenID[m.ID] {
			return fmt.Errorf("config: cluster.members: duplicate id %q", m.ID)
		}
		seenID[m.ID] = true
		if seenAddr[m.Address] {
			return fmt.Errorf("config: cluster.members: duplicate address %q", m.Address)
		}
		seenAddr[m.Address] = true
		if m.ID == c.NodeID {
			selfFound = true
		}
	}
	if !selfFound {
		return fmt.Errorf("config: nodeId %q is not in cluster.members", c.NodeID)
	}

	if c.DataDir == "" {
		return fmt.Errorf("config: dataDir is required")
	}

	n, w, r := c.Cluster.N, c.Cluster.W, c.Cluster.R
	if w < 1 || w > n {
		return fmt.Errorf("config: cluster.w must satisfy 1 <= w <= n, got w=%d n=%d", w, n)
	}
	if r < 1 || r > n {
		return fmt.Errorf("config: cluster.r must satisfy 1 <= r <= n, got r=%d n=%d", r, n)
	}
	if n > len(c.Cluster.Members) {
		return fmt.Errorf("config: cluster.n (%d) exceeds len(cluster.members) (%d)", n, len(c.Cluster.Members))
	}

	for name, d := range map[string]time.Duration{
		"intervals.compaction":         c.Intervals.Compaction,
		"intervals.antiEntropy":        c.Intervals.AntiEntropy,
		"intervals.hintDelivery":       c.Intervals.HintDelivery,
		"timeouts.replication":         c.Timeouts.Replication,
		"timeouts.maxReconnectBackoff": c.Timeouts.MaxReconnectBackoff,
		"timeouts.shutdown":            c.Timeouts.Shutdown,
		"intervals.heartbeat":          c.Intervals.Heartbeat,
		"timeouts.heartbeat":           c.Timeouts.Heartbeat,
	} {
		if d <= 0 {
			return fmt.Errorf("config: %s must be > 0, got %v", name, d)
		}
	}
	if c.Timeouts.Heartbeat >= c.Intervals.Heartbeat {
		return fmt.Errorf("config: timeouts.heartbeat (%v) must be shorter than intervals.heartbeat (%v)", c.Timeouts.Heartbeat, c.Intervals.Heartbeat)
	}
	if c.Health.MaxMissedHeartbeats <= 0 {
		return fmt.Errorf("config: health.maxMissedHeartbeats must be > 0, got %d", c.Health.MaxMissedHeartbeats)
	}

	if c.Storage.MemtableBytes <= 0 {
		return fmt.Errorf("config: storage.memtableBytes must be > 0, got %d", c.Storage.MemtableBytes)
	}

	return nil
}

func (c *Config) SelfAddress() string {
	for _, m := range c.Cluster.Members {
		if m.ID == c.NodeID {
			return m.Address
		}
	}
	return ""
}

// MemberAddrs returns every member's address, keyed by ID — this node's
// included — the shape node.Node.SetMembership takes.
func (c *Config) MemberAddrs() map[string]string {
	members := make(map[string]string, len(c.Cluster.Members))
	for _, m := range c.Cluster.Members {
		members[m.ID] = m.Address
	}
	return members
}

// NeighborAddrs returns every other member's address, keyed by ID — the
// shape node.New takes for neighborAddrs.
func (c *Config) NeighborAddrs() map[string]string {
	neighbors := make(map[string]string, len(c.Cluster.Members)-1)
	for _, m := range c.Cluster.Members {
		if m.ID != c.NodeID {
			neighbors[m.ID] = m.Address
		}
	}
	return neighbors
}
