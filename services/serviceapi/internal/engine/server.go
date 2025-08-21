package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/mux"
)

type Server struct {
	engine        *Engine
	router        *mux.Router
	tenantHandler *TenantHandlers
	meshHandler   *MeshHandlers
}

func NewServer(engine *Engine) *Server {
	s := &Server{
		engine:        engine,
		router:        mux.NewRouter(),
		tenantHandler: NewTenantHandlers(engine),
		meshHandler:   NewMeshHandlers(engine),
	}
	s.setupRoutes()
	s.setupMiddleware()
	return s
}

func (s *Server) setupMiddleware() {
	// CORS middleware
	s.router.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusOK)
				return
			}

			next.ServeHTTP(w, r)
		})
	})

	// Logging middleware
	s.router.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			next.ServeHTTP(w, r)
			duration := time.Since(start)

			// Log the request (simplified logging)
			// In production, you'd want to use proper structured logging
			_ = duration // Placeholder for logging implementation
		})
	})
}

func (s *Server) setupRoutes() {
	// Health check endpoint
	s.router.HandleFunc("/health", s.handleHealth).Methods(http.MethodGet)

	// Global OPTIONS handler for CORS preflight requests
	s.router.PathPrefix("/").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			// CORS headers are already set by middleware
			w.WriteHeader(http.StatusOK)
			return
		}
		// If not OPTIONS, return 404 for unmatched routes
		http.NotFound(w, r)
	}).Methods(http.MethodOptions)

	// Initial setup endpoint (no authentication required)
	s.router.HandleFunc("/api/v1/setup", s.handleInitialSetup).Methods(http.MethodPost)

	// API v1 routes
	apiV1 := s.router.PathPrefix("/api/v1").Subrouter()

	// Tenant endpoints
	tenants := apiV1.PathPrefix("/tenants").Subrouter()
	tenants.HandleFunc("", s.tenantHandler.ListTenants).Methods(http.MethodGet)
	tenants.HandleFunc("", s.tenantHandler.AddTenant).Methods(http.MethodPost)
	tenants.HandleFunc("/{tenant_id}", s.tenantHandler.ShowTenant).Methods(http.MethodGet)
	tenants.HandleFunc("/{tenant_id}", s.tenantHandler.ModifyTenant).Methods(http.MethodPut)
	tenants.HandleFunc("/{tenant_id}", s.tenantHandler.DeleteTenant).Methods(http.MethodDelete)

	// Mesh endpoints
	mesh := apiV1.PathPrefix("/mesh").Subrouter()
	mesh.HandleFunc("/seed", s.meshHandler.SeedMesh).Methods(http.MethodPost)
	mesh.HandleFunc("/join", s.meshHandler.JoinMesh).Methods(http.MethodPost)
	mesh.HandleFunc("/{mesh_id}", s.meshHandler.ShowMesh).Methods(http.MethodGet)
	mesh.HandleFunc("/{mesh_id}/nodes", s.meshHandler.ListNodes).Methods(http.MethodGet)

	// Legacy query endpoint (keep for backwards compatibility)
	s.router.HandleFunc("/query", s.handleQuery).Methods(http.MethodPost)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	response := map[string]interface{}{
		"status":    "healthy",
		"service":   "serviceapi",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleInitialSetup(w http.ResponseWriter, r *http.Request) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()

	// Parse request body
	var req struct {
		TenantName        string `json:"tenant_name"`
		TenantURL         string `json:"tenant_url"`
		TenantDescription string `json:"tenant_description"`
		UserEmail         string `json:"user_email"`
		UserPassword      string `json:"user_password"`
		WorkspaceName     string `json:"workspace_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Validate required fields
	if req.TenantName == "" || req.TenantURL == "" || req.UserEmail == "" || req.UserPassword == "" || req.WorkspaceName == "" {
		http.Error(w, "Missing required fields: tenant_name, tenant_url, user_email, user_password, workspace_name", http.StatusBadRequest)
		return
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call the engine to perform initial setup
	response, err := s.engine.PerformInitialSetup(ctx, req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Send response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
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
