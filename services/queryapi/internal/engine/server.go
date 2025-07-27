package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/mux"
)

type Server struct {
	engine *Engine
	router *mux.Router
}

func NewServer(engine *Engine) *Server {
	s := &Server{
		engine: engine,
		router: mux.NewRouter(),
	}
	s.setupRoutes()
	return s
}

func (s *Server) setupRoutes() {
	// Query endpoint
	s.router.HandleFunc("/query", s.handleQuery).Methods(http.MethodPost)

	// Add other endpoints as needed
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()

	// Parse request body
	var req struct {
		// Add your request fields here
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service through engine
	response, err := s.engine.Query(ctx, req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Send response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}
