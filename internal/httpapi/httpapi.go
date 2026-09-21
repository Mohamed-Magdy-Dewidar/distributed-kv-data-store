package httpapi

import (
	"context"
	"embed"
	"encoding/json"
	"io/fs"
	"net/http"
	"sync"
	"time"

	"distributed-kv-datastore/internal/node"
	"distributed-kv-datastore/internal/rpc"
)

//go:embed static/*
var staticFiles embed.FS

type managedNode struct {
	node      *node.Node
	listener  *rpc.Listener
	address   string
	w, r      int
	neighbors map[string]string
}

type Server struct {
	mu    sync.Mutex
	nodes map[string]*managedNode
}

func NewServer() *Server {
	return &Server{nodes: make(map[string]*managedNode)}
}

func (s *Server) Register(id string, n *node.Node, listener *rpc.Listener, address string, w, r int, neighbors map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodes[id] = &managedNode{node: n, listener: listener, address: address, w: w, r: r, neighbors: neighbors}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/nodes", s.handleListNodes)
	mux.HandleFunc("POST /api/nodes/{id}/put", s.handlePut)
	mux.HandleFunc("GET /api/nodes/{id}/get/{key}", s.handleGet)
	mux.HandleFunc("POST /api/nodes/{id}/stop", s.handleStop)
	mux.HandleFunc("POST /api/nodes/{id}/start", s.handleStart)

	mux.Handle("/", http.FileServer(http.FS(mustSub(staticFiles, "static"))))

	return mux
}

func mustSub(f embed.FS, dir string) fs.FS {
	sub, err := fs.Sub(f, dir)
	if err != nil {
		panic(err)
	}
	return sub
}

type nodeStatus struct {
	ID    string `json:"id"`
	Addr  string `json:"address"`
	W     int    `json:"w"`
	R     int    `json:"r"`
	Alive bool   `json:"alive"`
}

func (s *Server) handleListNodes(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	statuses := make([]nodeStatus, 0, len(s.nodes))
	for id, mn := range s.nodes {
		statuses = append(statuses, nodeStatus{
			ID:    id,
			Addr:  mn.address,
			W:     mn.w,
			R:     mn.r,
			Alive: mn.listener != nil,
		})
	}
	writeJSON(w, statuses)
}

type putRequest struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}

type putResponse struct {
	Success   bool   `json:"success"`
	Error     string `json:"error,omitempty"`
	ElapsedMS int64  `json:"elapsedMs"`
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	mn, ok := s.lookup(id)
	if !ok {
		http.Error(w, "unknown node", http.StatusNotFound)
		return
	}

	var req putRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request body", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	start := time.Now()
	err := mn.node.Put(ctx, req.Key, req.Value, nil)
	elapsed := time.Since(start)

	resp := putResponse{Success: err == nil, ElapsedMS: elapsed.Milliseconds()}
	if err != nil {
		resp.Error = err.Error()
	}
	writeJSON(w, resp)
}

type getResponse struct {
	Success   bool       `json:"success"`
	Error     string     `json:"error,omitempty"`
	Items     []itemView `json:"items,omitempty"`
	ElapsedMS int64      `json:"elapsedMs"`
}

type itemView struct {
	Value         any               `json:"value"`
	VectorClock   map[string]uint32 `json:"vectorClock"`
	LastUpdatedBy string            `json:"lastUpdatedBy"`
	IsDeleted     bool              `json:"isDeleted"`
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	key := r.PathValue("key")

	mn, ok := s.lookup(id)
	if !ok {
		http.Error(w, "unknown node", http.StatusNotFound)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	start := time.Now()
	items, err := mn.node.Get(ctx, key)
	elapsed := time.Since(start)

	resp := getResponse{Success: err == nil, ElapsedMS: elapsed.Milliseconds()}
	if err != nil {
		resp.Error = err.Error()
	}
	for _, it := range items {
		resp.Items = append(resp.Items, itemView{
			Value:         it.Value,
			VectorClock:   it.VectorClock.Snapshot(),
			LastUpdatedBy: it.LastUpdatedBy,
			IsDeleted:     it.IsDeleted,
		})
	}
	writeJSON(w, resp)
}

func (s *Server) handleStop(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	s.mu.Lock()
	defer s.mu.Unlock()

	mn, ok := s.nodes[id]
	if !ok {
		http.Error(w, "unknown node", http.StatusNotFound)
		return
	}
	if mn.listener != nil {
		mn.listener.Stop()
		mn.listener = nil
	}
	writeJSON(w, map[string]bool{"stopped": true})
}

func (s *Server) handleStart(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	s.mu.Lock()
	defer s.mu.Unlock()

	mn, ok := s.nodes[id]
	if !ok {
		http.Error(w, "unknown node", http.StatusNotFound)
		return
	}
	if mn.listener != nil {
		writeJSON(w, map[string]bool{"started": true})
		return
	}

	listener, err := rpc.Serve(mn.address, mn.node.Store)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	mn.listener = listener
	writeJSON(w, map[string]bool{"started": true})
}

func (s *Server) lookup(id string) (*managedNode, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	mn, ok := s.nodes[id]
	return mn, ok
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
