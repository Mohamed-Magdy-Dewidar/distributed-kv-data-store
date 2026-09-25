package rpc

import (
	"encoding/json"
	"fmt"

	"distributed-kv-datastore/internal/merkle"
	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/rpc/pb"
	"distributed-kv-datastore/internal/vectorclock"
)

func toProtoDataItem(item *model.DataItem) (*pb.DataItem, error) {
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

func fromProtoDataItem(p *pb.DataItem) (*model.DataItem, error) {
	var value any
	if err := json.Unmarshal(p.Value, &value); err != nil {
		return nil, fmt.Errorf("unmarshal value: %w", err)
	}

	return &model.DataItem{
		Value:         value,
		VectorClock:   vectorclock.FromSnapshot(p.VectorClock),
		LastUpdatedBy: p.LastUpdatedBy,
		IsDeleted:     p.IsDeleted,
	}, nil
}

func toProtoTree(t *merkle.Tree) *pb.GetMerkleTreeResponse {
	nodes := make([][]byte, len(t.Nodes))
	for i, h := range t.Nodes {
		nodes[i] = h[:]
	}
	return &pb.GetMerkleTreeResponse{
		NumBuckets: int32(t.NumBuckets),
		Nodes:      nodes,
	}
}

func fromProtoTree(resp *pb.GetMerkleTreeResponse) *merkle.Tree {
	nodes := make([]merkle.Hash, len(resp.Nodes))
	for i, b := range resp.Nodes {
		copy(nodes[i][:], b)
	}
	return &merkle.Tree{
		NumBuckets: int(resp.NumBuckets),
		Nodes:      nodes,
	}
}
