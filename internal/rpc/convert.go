package rpc

import (
	"encoding/json"
	"fmt"

	"distributed-kv-datastore/internal/rpc/pb"
	"distributed-kv-datastore/internal/store"
	"distributed-kv-datastore/internal/vectorclock"
)

func toProtoDataItem(item *store.DataItem) (*pb.DataItem, error) {
	valueBytes, err := json.Marshal(item.Value)
	if err != nil {
		return nil, fmt.Errorf("marshal value: %w", err)
	}

	return &pb.DataItem{
		Value:         valueBytes,
		VectorClock:   item.VectorClock.Snapshot(),
		LastUpdatedBy: item.LastUpdatedBy,
		IsDeleted:     item.IsDeleted,
	}, nil
}

func fromProtoDataItem(p *pb.DataItem) (*store.DataItem, error) {
	var value any
	if err := json.Unmarshal(p.Value, &value); err != nil {
		return nil, fmt.Errorf("unmarshal value: %w", err)
	}

	return &store.DataItem{
		Value:         value,
		VectorClock:   vectorclock.FromSnapshot(p.VectorClock),
		LastUpdatedBy: p.LastUpdatedBy,
		IsDeleted:     p.IsDeleted,
	}, nil
}
