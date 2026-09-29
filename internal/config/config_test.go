package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// validConfig returns a Config that satisfies every validation rule, as a
// starting point for tests that mutate exactly one field.
func validConfig() Config {
	return Config{
		NodeID: "kv-0",
		Listen: Listen{GRPC: ":7000", HTTP: ":8080"},
		Cluster: Cluster{
			N: 2, W: 1, R: 1,
			Members: []Member{
				{ID: "kv-0", Address: "kv-0.kv.default.svc.cluster.local:7000"},
				{ID: "kv-1", Address: "kv-1.kv.default.svc.cluster.local:7000"},
			},
		},
		Storage: Storage{MemtableBytes: 4 << 20},
		Intervals: Intervals{
			Compaction:   30 * time.Second,
			AntiEntropy:  30 * time.Second,
			HintDelivery: 10 * time.Second,
		},
		Timeouts: Timeouts{
			Replication:         5 * time.Second,
			MaxReconnectBackoff: 5 * time.Second,
			Shutdown:            20 * time.Second,
		},
	}
}

// TestValidateRules is table-driven: one case per validation rule in
// validate, each mutating exactly one field away from validConfig() and
// checking the error names the field it's about.
func TestValidateRules(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate    func(*Config)
		wantInErr string
	}{
		"nodeId empty": {
			mutate:    func(c *Config) { c.NodeID = "" },
			wantInErr: "nodeId",
		},
		"nodeId not in members": {
			mutate:    func(c *Config) { c.NodeID = "kv-99" },
			wantInErr: "nodeId",
		},
		"duplicate member id": {
			mutate: func(c *Config) {
				c.Cluster.Members[1].ID = c.Cluster.Members[0].ID
			},
			wantInErr: "duplicate id",
		},
		"duplicate member address": {
			mutate: func(c *Config) {
				c.Cluster.Members[1].Address = c.Cluster.Members[0].Address
			},
			wantInErr: "duplicate address",
		},
		"w below 1": {
			mutate:    func(c *Config) { c.Cluster.W = 0 },
			wantInErr: "cluster.w",
		},
		"w above n": {
			mutate:    func(c *Config) { c.Cluster.W = c.Cluster.N + 1 },
			wantInErr: "cluster.w",
		},
		"r below 1": {
			mutate:    func(c *Config) { c.Cluster.R = 0 },
			wantInErr: "cluster.r",
		},
		"r above n": {
			mutate:    func(c *Config) { c.Cluster.R = c.Cluster.N + 1 },
			wantInErr: "cluster.r",
		},
		"n exceeds member count": {
			mutate:    func(c *Config) { c.Cluster.N = len(c.Cluster.Members) + 1 },
			wantInErr: "cluster.n",
		},
		"compaction interval not positive": {
			mutate:    func(c *Config) { c.Intervals.Compaction = 0 },
			wantInErr: "intervals.compaction",
		},
		"antiEntropy interval not positive": {
			mutate:    func(c *Config) { c.Intervals.AntiEntropy = -time.Second },
			wantInErr: "intervals.antiEntropy",
		},
		"hintDelivery interval not positive": {
			mutate:    func(c *Config) { c.Intervals.HintDelivery = 0 },
			wantInErr: "intervals.hintDelivery",
		},
		"replication timeout not positive": {
			mutate:    func(c *Config) { c.Timeouts.Replication = 0 },
			wantInErr: "timeouts.replication",
		},
		"maxReconnectBackoff timeout not positive": {
			mutate:    func(c *Config) { c.Timeouts.MaxReconnectBackoff = 0 },
			wantInErr: "timeouts.maxReconnectBackoff",
		},
		"shutdown timeout not positive": {
			mutate:    func(c *Config) { c.Timeouts.Shutdown = 0 },
			wantInErr: "timeouts.shutdown",
		},
		"memtableBytes not positive": {
			mutate:    func(c *Config) { c.Storage.MemtableBytes = 0 },
			wantInErr: "storage.memtableBytes",
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := validConfig()
			tc.mutate(&cfg)
			err := cfg.validate()
			if err == nil {
				t.Fatalf("expected an error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantInErr) {
				t.Fatalf("expected error to mention %q, got %q", tc.wantInErr, err.Error())
			}
		})
	}
}

// TestValidateAcceptsAValidConfig: validConfig() itself must pass, so the
// table above is testing real deviations, not a baseline that's already
// broken.
func TestValidateAcceptsAValidConfig(t *testing.T) {
	cfg := validConfig()
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected validConfig() to be valid, got %v", err)
	}
}

const validYAML = `
nodeId: kv-0
listen:
  grpc: ":7000"
  http: ":8080"
dataDir: "/var/lib/kv"
cluster:
  n: 2
  w: 1
  r: 1
  members:
    - id: kv-0
      address: kv-0.kv.default.svc.cluster.local:7000
    - id: kv-1
      address: kv-1.kv.default.svc.cluster.local:7000
storage:
  memtableBytes: 4194304
intervals:
  compaction: 30s
  antiEntropy: 30s
  hintDelivery: 10s
timeouts:
  replication: 5s
  maxReconnectBackoff: 5s
  shutdown: 20s
`

func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(contents), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

// TestLoadRejectsUnknownFields: strict decoding must reject a field that
// doesn't exist in the schema, not silently ignore it.
func TestLoadRejectsUnknownFields(t *testing.T) {
	path := writeConfig(t, validYAML+"\nbogusField: 123\n")
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected Load to reject an unknown field")
	}
	if !strings.Contains(err.Error(), "bogusField") {
		t.Fatalf("expected the error to mention bogusField, got %v", err)
	}
}

// TestLoadParsesDurationsFromPlainStrings: "30s"-style YAML scalars must
// decode into time.Duration correctly, not error or silently zero out.
func TestLoadParsesDurationsFromPlainStrings(t *testing.T) {
	path := writeConfig(t, validYAML)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Intervals.Compaction != 30*time.Second {
		t.Errorf("expected intervals.compaction=30s, got %v", cfg.Intervals.Compaction)
	}
	if cfg.Intervals.HintDelivery != 10*time.Second {
		t.Errorf("expected intervals.hintDelivery=10s, got %v", cfg.Intervals.HintDelivery)
	}
	if cfg.Timeouts.Shutdown != 20*time.Second {
		t.Errorf("expected timeouts.shutdown=20s, got %v", cfg.Timeouts.Shutdown)
	}
}

// TestKV_NODE_IDOverlay covers all three cases: unset (YAML value stands),
// set (overrides the YAML value), and set to make an otherwise-invalid
// YAML nodeId (empty) valid.
func TestKV_NODE_IDOverlay(t *testing.T) {
	t.Run("unset leaves the YAML value in place", func(t *testing.T) {
		path := writeConfig(t, validYAML)
		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		if cfg.NodeID != "kv-0" {
			t.Fatalf("expected nodeId kv-0 from YAML, got %q", cfg.NodeID)
		}
	})

	t.Run("set overrides the YAML value", func(t *testing.T) {
		t.Setenv("KV_NODE_ID", "kv-1")
		path := writeConfig(t, validYAML) // YAML says kv-0
		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		if cfg.NodeID != "kv-1" {
			t.Fatalf("expected KV_NODE_ID to override to kv-1, got %q", cfg.NodeID)
		}
	})

	t.Run("set fills in an empty YAML value", func(t *testing.T) {
		t.Setenv("KV_NODE_ID", "kv-1")
		path := writeConfig(t, strings.Replace(validYAML, "nodeId: kv-0", `nodeId: ""`, 1))
		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		if cfg.NodeID != "kv-1" {
			t.Fatalf("expected KV_NODE_ID kv-1 to fill in the empty YAML value, got %q", cfg.NodeID)
		}
	})
}
