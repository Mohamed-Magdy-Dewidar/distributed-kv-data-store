package render

import (
	"fmt"
	"strings"

	yaml "go.yaml.in/yaml/v3"
	corev1 "k8s.io/api/core/v1"
)

// ParseConfigMap reads back the membership a node ConfigMap carries: the
// inverse of ConfigMap. It decodes config.yaml with the YAML library the
// nodes use (a YAML 1.1 parser would read the key "n" as false), reading only
// the cluster section.
func ParseConfigMap(cm *corev1.ConfigMap) (epoch uint64, members map[string]string, err error) {
	raw, ok := cm.Data[ConfigKey]
	if !ok {
		return 0, nil, fmt.Errorf("ConfigMap %s has no %s", cm.Name, ConfigKey)
	}
	var cfg struct {
		Cluster struct {
			Epoch   uint64 `yaml:"epoch"`
			Members []struct {
				ID      string `yaml:"id"`
				Address string `yaml:"address"`
			} `yaml:"members"`
		} `yaml:"cluster"`
	}
	if err := yaml.NewDecoder(strings.NewReader(raw)).Decode(&cfg); err != nil {
		return 0, nil, fmt.Errorf("ConfigMap %s: %s does not parse: %w", cm.Name, ConfigKey, err)
	}
	members = make(map[string]string, len(cfg.Cluster.Members))
	for _, m := range cfg.Cluster.Members {
		if _, dup := members[m.ID]; dup {
			return 0, nil, fmt.Errorf("ConfigMap %s: member %s is listed twice", cm.Name, m.ID)
		}
		members[m.ID] = m.Address
	}
	return cfg.Cluster.Epoch, members, nil
}
