package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/mux"
)

type Server struct {
	engine                *Engine
	router                *mux.Router
	authHandler           *AuthHandlers
	workspaceHandler      *WorkspaceHandlers
	environmentHandler    *EnvironmentHandlers
	regionHandler         *RegionHandlers
	meshHandler           *MeshHandlers
	satelliteHandler      *SatelliteHandlers
	anchorHandler         *AnchorHandlers
	instanceHandler       *InstanceHandlers
	databaseHandler       *DatabaseHandlers
	repoHandler           *RepoHandlers
	branchHandler         *BranchHandlers
	commitHandler         *CommitHandlers
	mappingHandler        *MappingHandlers
	relationshipHandler   *RelationshipHandlers
	transformationHandler *TransformationHandlers
	policyHandler         *PolicyHandlers
	userHandler           *UserHandlers
	middleware            *Middleware
}

func NewServer(engine *Engine) *Server {
	s := &Server{
		engine:                engine,
		router:                mux.NewRouter(),
		authHandler:           NewAuthHandlers(engine),
		workspaceHandler:      NewWorkspaceHandlers(engine),
		environmentHandler:    NewEnvironmentHandlers(engine),
		regionHandler:         NewRegionHandlers(engine),
		meshHandler:           NewMeshHandlers(engine),
		satelliteHandler:      NewSatelliteHandlers(engine),
		anchorHandler:         NewAnchorHandlers(engine),
		instanceHandler:       NewInstanceHandlers(engine),
		databaseHandler:       NewDatabaseHandlers(engine),
		repoHandler:           NewRepoHandlers(engine),
		branchHandler:         NewBranchHandlers(engine),
		commitHandler:         NewCommitHandlers(engine),
		mappingHandler:        NewMappingHandlers(engine),
		relationshipHandler:   NewRelationshipHandlers(engine),
		transformationHandler: NewTransformationHandlers(engine),
		policyHandler:         NewPolicyHandlers(engine),
		userHandler:           NewUserHandlers(engine),
		middleware:            NewMiddleware(engine),
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

	// Authentication and Authorization middleware
	s.router.Use(s.middleware.AuthenticationMiddleware)
	s.router.Use(s.middleware.AuthorizationMiddleware)
}

func (s *Server) setupRoutes() {
	// Health check endpoint (global, no tenant)
	s.router.HandleFunc("/health", s.handleHealth).Methods(http.MethodGet)

	// Tenant-specific routes with tenant_url in path
	// Pattern: /{tenant_url}/api/v1/...
	tenantRouter := s.router.PathPrefix("/{tenant_url}/api/v1").Subrouter()

	// Authentication endpoints
	auth := tenantRouter.PathPrefix("/auth").Subrouter()
	auth.HandleFunc("/login", s.authHandler.Login).Methods(http.MethodPost)
	auth.HandleFunc("/logout", s.authHandler.Logout).Methods(http.MethodPost)
	auth.HandleFunc("/profile", s.authHandler.GetProfile).Methods(http.MethodGet)
	auth.HandleFunc("/change-password", s.authHandler.ChangePassword).Methods(http.MethodPost)

	// Session management endpoints
	auth.HandleFunc("/sessions", s.authHandler.ListSessions).Methods(http.MethodGet)
	auth.HandleFunc("/sessions/{session_id}/logout", s.authHandler.LogoutSession).Methods(http.MethodPost)
	auth.HandleFunc("/sessions/logout-all", s.authHandler.LogoutAllSessions).Methods(http.MethodPost)
	auth.HandleFunc("/sessions/{session_id}/name", s.authHandler.UpdateSessionName).Methods(http.MethodPut)

	// Legacy query endpoint (keep for backwards compatibility)
	tenantRouter.HandleFunc("/query", s.handleQuery).Methods(http.MethodPost)

	// Workspace endpoints
	workspaces := tenantRouter.PathPrefix("/workspaces").Subrouter()
	workspaces.HandleFunc("", s.workspaceHandler.ListWorkspaces).Methods(http.MethodGet)
	workspaces.HandleFunc("", s.workspaceHandler.AddWorkspace).Methods(http.MethodPost)
	workspaces.HandleFunc("/{workspace_name}", s.workspaceHandler.ShowWorkspace).Methods(http.MethodGet)
	workspaces.HandleFunc("/{workspace_name}", s.workspaceHandler.ModifyWorkspace).Methods(http.MethodPut)
	workspaces.HandleFunc("/{workspace_name}", s.workspaceHandler.DeleteWorkspace).Methods(http.MethodDelete)

	// Environment endpoints (nested under workspaces)
	environments := workspaces.PathPrefix("/{workspace_name}/environments").Subrouter()
	environments.HandleFunc("", s.environmentHandler.ListEnvironments).Methods(http.MethodGet)
	environments.HandleFunc("", s.environmentHandler.AddEnvironment).Methods(http.MethodPost)
	environments.HandleFunc("/{environment_name}", s.environmentHandler.ShowEnvironment).Methods(http.MethodGet)
	environments.HandleFunc("/{environment_name}", s.environmentHandler.ModifyEnvironment).Methods(http.MethodPut)
	environments.HandleFunc("/{environment_name}", s.environmentHandler.DeleteEnvironment).Methods(http.MethodDelete)

	// Region endpoints (tenant-level)
	regions := tenantRouter.PathPrefix("/regions").Subrouter()
	regions.HandleFunc("", s.regionHandler.ListRegions).Methods(http.MethodGet)
	regions.HandleFunc("", s.regionHandler.AddRegion).Methods(http.MethodPost)
	regions.HandleFunc("/{region_name}", s.regionHandler.ShowRegion).Methods(http.MethodGet)
	regions.HandleFunc("/{region_name}", s.regionHandler.ModifyRegion).Methods(http.MethodPut)
	regions.HandleFunc("/{region_name}", s.regionHandler.DeleteRegion).Methods(http.MethodDelete)

	// Mesh endpoints (tenant-level)
	meshes := tenantRouter.PathPrefix("/meshes").Subrouter()
	meshes.HandleFunc("/seed", s.meshHandler.SeedMesh).Methods(http.MethodPost)
	meshes.HandleFunc("/{mesh_id}/join", s.meshHandler.JoinMesh).Methods(http.MethodPost)
	meshes.HandleFunc("/{mesh_id}/leave", s.meshHandler.LeaveMesh).Methods(http.MethodPost)
	meshes.HandleFunc("/{mesh_id}", s.meshHandler.ShowMesh).Methods(http.MethodGet)
	meshes.HandleFunc("/{mesh_id}", s.meshHandler.ModifyMesh).Methods(http.MethodPut)
	meshes.HandleFunc("/{mesh_id}/nodes", s.meshHandler.ListNodes).Methods(http.MethodGet)
	meshes.HandleFunc("/{mesh_id}/nodes/{node_id}", s.meshHandler.ShowNode).Methods(http.MethodGet)
	meshes.HandleFunc("/{mesh_id}/nodes/{node_id}", s.meshHandler.ModifyNode).Methods(http.MethodPut)
	meshes.HandleFunc("/{mesh_id}/nodes/{node_id}/evict", s.meshHandler.EvictNode).Methods(http.MethodPost)
	meshes.HandleFunc("/{mesh_id}/topology", s.meshHandler.ShowTopology).Methods(http.MethodGet)
	meshes.HandleFunc("/{mesh_id}/routes", s.meshHandler.AddMeshRoute).Methods(http.MethodPost)
	meshes.HandleFunc("/{mesh_id}/routes/{source_node_id}/{target_node_id}", s.meshHandler.ModifyMeshRoute).Methods(http.MethodPut)
	meshes.HandleFunc("/{mesh_id}/routes/{source_node_id}/{target_node_id}", s.meshHandler.DeleteMeshRoute).Methods(http.MethodDelete)

	// Satellite endpoints (tenant-level)
	satellites := tenantRouter.PathPrefix("/satellites").Subrouter()
	satellites.HandleFunc("", s.satelliteHandler.ListSatellites).Methods(http.MethodGet)
	satellites.HandleFunc("", s.satelliteHandler.AddSatellite).Methods(http.MethodPost)
	satellites.HandleFunc("/{satellite_id}", s.satelliteHandler.ShowSatellite).Methods(http.MethodGet)
	satellites.HandleFunc("/{satellite_id}", s.satelliteHandler.ModifySatellite).Methods(http.MethodPut)
	satellites.HandleFunc("/{satellite_id}", s.satelliteHandler.DeleteSatellite).Methods(http.MethodDelete)

	// Anchor endpoints (tenant-level)
	anchors := tenantRouter.PathPrefix("/anchors").Subrouter()
	anchors.HandleFunc("", s.anchorHandler.ListAnchors).Methods(http.MethodGet)
	anchors.HandleFunc("", s.anchorHandler.AddAnchor).Methods(http.MethodPost)
	anchors.HandleFunc("/{anchor_id}", s.anchorHandler.ShowAnchor).Methods(http.MethodGet)
	anchors.HandleFunc("/{anchor_id}", s.anchorHandler.ModifyAnchor).Methods(http.MethodPut)
	anchors.HandleFunc("/{anchor_id}", s.anchorHandler.DeleteAnchor).Methods(http.MethodDelete)

	// Transformation endpoints (tenant-level)
	transformations := tenantRouter.PathPrefix("/transformations").Subrouter()
	transformations.HandleFunc("", s.transformationHandler.ListTransformations).Methods(http.MethodGet)
	transformations.HandleFunc("", s.transformationHandler.AddTransformation).Methods(http.MethodPost)
	transformations.HandleFunc("/{transformation_id}", s.transformationHandler.ShowTransformation).Methods(http.MethodGet)
	transformations.HandleFunc("/{transformation_id}", s.transformationHandler.ModifyTransformation).Methods(http.MethodPut)
	transformations.HandleFunc("/{transformation_id}", s.transformationHandler.DeleteTransformation).Methods(http.MethodDelete)

	// Policy endpoints (tenant-level)
	policies := tenantRouter.PathPrefix("/policies").Subrouter()
	policies.HandleFunc("", s.policyHandler.ListPolicies).Methods(http.MethodGet)
	policies.HandleFunc("", s.policyHandler.AddPolicy).Methods(http.MethodPost)
	policies.HandleFunc("/{policy_id}", s.policyHandler.ShowPolicy).Methods(http.MethodGet)
	policies.HandleFunc("/{policy_id}", s.policyHandler.ModifyPolicy).Methods(http.MethodPut)
	policies.HandleFunc("/{policy_id}", s.policyHandler.DeletePolicy).Methods(http.MethodDelete)

	// User endpoints (tenant-level)
	users := tenantRouter.PathPrefix("/users").Subrouter()
	users.HandleFunc("", s.userHandler.ListUsers).Methods(http.MethodGet)
	users.HandleFunc("", s.userHandler.AddUser).Methods(http.MethodPost)
	users.HandleFunc("/{user_id}", s.userHandler.ShowUser).Methods(http.MethodGet)
	users.HandleFunc("/{user_id}", s.userHandler.ModifyUser).Methods(http.MethodPut)
	users.HandleFunc("/{user_id}", s.userHandler.DeleteUser).Methods(http.MethodDelete)

	// Instance endpoints (workspace-level)
	instances := workspaces.PathPrefix("/{workspace_name}/instances").Subrouter()
	instances.HandleFunc("", s.instanceHandler.ListInstances).Methods(http.MethodGet)
	instances.HandleFunc("/connect", s.instanceHandler.ConnectInstance).Methods(http.MethodPost)
	instances.HandleFunc("/{instance_name}", s.instanceHandler.ShowInstance).Methods(http.MethodGet)
	instances.HandleFunc("/{instance_name}", s.instanceHandler.ModifyInstance).Methods(http.MethodPut)
	instances.HandleFunc("/{instance_name}/reconnect", s.instanceHandler.ReconnectInstance).Methods(http.MethodPost)
	instances.HandleFunc("/{instance_name}/disconnect", s.instanceHandler.DisconnectInstance).Methods(http.MethodPost)
	instances.HandleFunc("/{instance_name}/create", s.instanceHandler.CreateDatabase).Methods(http.MethodPost)

	// Database endpoints (workspace-level)
	databases := workspaces.PathPrefix("/{workspace_name}/databases").Subrouter()
	databases.HandleFunc("", s.databaseHandler.ListDatabases).Methods(http.MethodGet)
	databases.HandleFunc("/connect", s.databaseHandler.ConnectDatabase).Methods(http.MethodPost)
	databases.HandleFunc("/connect-with-instance", s.databaseHandler.ConnectDatabaseWithInstance).Methods(http.MethodPost)
	databases.HandleFunc("/{database_name}", s.databaseHandler.ShowDatabase).Methods(http.MethodGet)
	databases.HandleFunc("/{database_name}/reconnect", s.databaseHandler.ReconnectDatabase).Methods(http.MethodPost)
	databases.HandleFunc("/{database_name}", s.databaseHandler.ModifyDatabase).Methods(http.MethodPut)
	databases.HandleFunc("/{database_name}/disconnect", s.databaseHandler.DisconnectDatabase).Methods(http.MethodPost)
	databases.HandleFunc("/{database_name}/schema", s.databaseHandler.GetLatestStoredDatabaseSchema).Methods(http.MethodGet)
	databases.HandleFunc("/{database_name}/wipe", s.databaseHandler.WipeDatabase).Methods(http.MethodPost)
	databases.HandleFunc("/{database_name}/drop", s.databaseHandler.DropDatabase).Methods(http.MethodPost)
	databases.HandleFunc("/transform", s.databaseHandler.TransformData).Methods(http.MethodPost)

	// Repo endpoints (workspace-level)
	repos := workspaces.PathPrefix("/{workspace_name}/repos").Subrouter()
	repos.HandleFunc("", s.repoHandler.ListRepos).Methods(http.MethodGet)
	repos.HandleFunc("", s.repoHandler.AddRepo).Methods(http.MethodPost)
	repos.HandleFunc("/{repo_name}", s.repoHandler.ShowRepo).Methods(http.MethodGet)
	repos.HandleFunc("/{repo_name}", s.repoHandler.ModifyRepo).Methods(http.MethodPut)
	repos.HandleFunc("/{repo_name}", s.repoHandler.DeleteRepo).Methods(http.MethodDelete)
	repos.HandleFunc("/{repo_name}/clone", s.repoHandler.CloneRepo).Methods(http.MethodPost)

	// Branch endpoints (nested under repos)
	branches := repos.PathPrefix("/{repo_name}/branches").Subrouter()
	branches.HandleFunc("/{branch_name}", s.branchHandler.ShowBranch).Methods(http.MethodGet)
	branches.HandleFunc("/{branch_name}", s.branchHandler.ModifyBranch).Methods(http.MethodPut)
	branches.HandleFunc("/{branch_name}", s.branchHandler.DeleteBranch).Methods(http.MethodDelete)
	branches.HandleFunc("/{branch_name}/attach", s.branchHandler.AttachBranch).Methods(http.MethodPost)
	branches.HandleFunc("/{branch_name}/detach", s.branchHandler.DetachBranch).Methods(http.MethodPost)

	// Commit endpoints (nested under branches)
	commits := branches.PathPrefix("/{branch_name}/commits").Subrouter()
	commits.HandleFunc("/{commit_code}", s.commitHandler.ShowCommit).Methods(http.MethodGet)
	commits.HandleFunc("/{commit_code}/branch", s.commitHandler.BranchCommit).Methods(http.MethodPost)
	commits.HandleFunc("/{commit_code}/merge", s.commitHandler.MergeCommit).Methods(http.MethodPost)
	commits.HandleFunc("/{commit_code}/deploy", s.commitHandler.DeployCommit).Methods(http.MethodPost)

	// Mapping endpoints (workspace-level)
	mappings := workspaces.PathPrefix("/{workspace_name}/mappings").Subrouter()
	mappings.HandleFunc("", s.mappingHandler.ListMappings).Methods(http.MethodGet)
	mappings.HandleFunc("", s.mappingHandler.AddMapping).Methods(http.MethodPost)
	mappings.HandleFunc("/database", s.mappingHandler.AddDatabaseMapping).Methods(http.MethodPost)
	mappings.HandleFunc("/table", s.mappingHandler.AddTableMapping).Methods(http.MethodPost)
	mappings.HandleFunc("/{mapping_name}", s.mappingHandler.ShowMapping).Methods(http.MethodGet)
	mappings.HandleFunc("/{mapping_name}", s.mappingHandler.ModifyMapping).Methods(http.MethodPut)
	mappings.HandleFunc("/{mapping_name}", s.mappingHandler.DeleteMapping).Methods(http.MethodDelete)
	mappings.HandleFunc("/{mapping_name}/attach-rule", s.mappingHandler.AttachMappingRule).Methods(http.MethodPost)
	mappings.HandleFunc("/{mapping_name}/detach-rule", s.mappingHandler.DetachMappingRule).Methods(http.MethodPost)

	// Mapping rule endpoints (workspace-level)
	mappingRules := workspaces.PathPrefix("/{workspace_name}/mapping-rules").Subrouter()
	mappingRules.HandleFunc("", s.mappingHandler.ListMappingRules).Methods(http.MethodGet)
	mappingRules.HandleFunc("", s.mappingHandler.AddMappingRule).Methods(http.MethodPost)
	mappingRules.HandleFunc("/{mapping_rule_name}", s.mappingHandler.ShowMappingRule).Methods(http.MethodGet)
	mappingRules.HandleFunc("/{mapping_rule_name}", s.mappingHandler.ModifyMappingRule).Methods(http.MethodPut)
	mappingRules.HandleFunc("/{mapping_rule_name}", s.mappingHandler.DeleteMappingRule).Methods(http.MethodDelete)

	// Relationship endpoints (workspace-level)
	relationships := workspaces.PathPrefix("/{workspace_name}/relationships").Subrouter()
	relationships.HandleFunc("", s.relationshipHandler.ListRelationships).Methods(http.MethodGet)
	relationships.HandleFunc("", s.relationshipHandler.AddRelationship).Methods(http.MethodPost)
	relationships.HandleFunc("/{relationship_id}", s.relationshipHandler.ShowRelationship).Methods(http.MethodGet)
	relationships.HandleFunc("/{relationship_id}", s.relationshipHandler.ModifyRelationship).Methods(http.MethodPut)
	relationships.HandleFunc("/{relationship_id}", s.relationshipHandler.DeleteRelationship).Methods(http.MethodDelete)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	response := map[string]interface{}{
		"status":    "healthy",
		"service":   "clientapi",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()

	// Extract tenant_url from path
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	if tenantURL == "" {
		http.Error(w, "tenant_url is required", http.StatusBadRequest)
		return
	}

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
