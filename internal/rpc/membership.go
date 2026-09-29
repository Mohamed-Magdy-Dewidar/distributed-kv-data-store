package rpc

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"distributed-kv-datastore/internal/rpc/pb"
)

// PingInfo is what a Ping announces about its sender: who it is, where it can
// be reached, and the membership it holds.
type PingInfo struct {
	SenderID      string
	SenderAddress string
	Epoch         uint64
	Fingerprint   string
}

// PingReply is what a Ping answers: the answering node's own ID and the
// membership it holds.
type PingReply struct {
	NodeID      string
	Epoch       uint64
	Fingerprint string
}

// MembershipInfo is a membership as it travels between nodes. Fingerprint is
// the sender's claim; receivers must not trust it (see node.Node).
type MembershipInfo struct {
	Epoch       uint64
	Members     map[string]string // node ID → address
	Fingerprint string
}

// MembershipService serves Ping and GetMembership. node.Node implements it;
// it is an interface here because node imports rpc, not the other way round.
// A server whose coordinator doesn't implement it answers both RPCs with
// codes.Unimplemented.
type MembershipService interface {
	// HandlePing answers a heartbeat. It must return promptly: anything slow
	// it triggers (fetching a newer membership) belongs in the background.
	HandlePing(ctx context.Context, from PingInfo) PingReply

	// CurrentMembership returns the current membership.
	CurrentMembership() MembershipInfo
}

// Ping implements pb.KVReplicationServer.
func (s *Server) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	if s.membership == nil {
		return nil, status.Error(codes.Unimplemented, "this server does not serve membership")
	}
	reply := s.membership.HandlePing(ctx, PingInfo{
		SenderID:      req.SenderId,
		SenderAddress: req.SenderAddress,
		Epoch:         req.Epoch,
		Fingerprint:   req.Fingerprint,
	})
	return &pb.PingResponse{NodeId: reply.NodeID, Epoch: reply.Epoch, Fingerprint: reply.Fingerprint}, nil
}

// GetMembership implements pb.KVReplicationServer.
func (s *Server) GetMembership(ctx context.Context, _ *pb.GetMembershipRequest) (*pb.GetMembershipResponse, error) {
	if s.membership == nil {
		return nil, status.Error(codes.Unimplemented, "this server does not serve membership")
	}
	m := s.membership.CurrentMembership()
	return &pb.GetMembershipResponse{Epoch: m.Epoch, Members: m.Members, Fingerprint: m.Fingerprint}, nil
}

// Ping sends a heartbeat to this peer.
func (c *Client) Ping(ctx context.Context, from PingInfo) (PingReply, error) {
	resp, err := c.stub.Ping(ctx, &pb.PingRequest{
		SenderId:      from.SenderID,
		SenderAddress: from.SenderAddress,
		Epoch:         from.Epoch,
		Fingerprint:   from.Fingerprint,
	})
	if err != nil {
		return PingReply{}, err
	}
	return PingReply{NodeID: resp.NodeId, Epoch: resp.Epoch, Fingerprint: resp.Fingerprint}, nil
}

// GetMembership fetches this peer's current membership.
func (c *Client) GetMembership(ctx context.Context) (MembershipInfo, error) {
	resp, err := c.stub.GetMembership(ctx, &pb.GetMembershipRequest{})
	if err != nil {
		return MembershipInfo{}, err
	}
	return MembershipInfo{Epoch: resp.Epoch, Members: resp.Members, Fingerprint: resp.Fingerprint}, nil
}

// FetchMembership dials address, fetches its membership and closes the
// connection again, for a peer that may not be in the caller's membership yet
// (so has no cached client). timeout bounds the whole exchange.
func FetchMembership(ctx context.Context, address string, timeout, maxReconnectBackoff time.Duration) (MembershipInfo, error) {
	client, err := Dial(address, maxReconnectBackoff)
	if err != nil {
		return MembershipInfo{}, err
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	info, err := client.GetMembership(ctx)
	if err != nil {
		return MembershipInfo{}, fmt.Errorf("get membership from %s: %w", address, err)
	}
	return info, nil
}
