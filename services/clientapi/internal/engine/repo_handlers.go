package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	securityv1 "github.com/redbco/redb-open/api/proto/security/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RepoHandlers contains the repository endpoint handlers
type RepoHandlers struct {
	engine *Engine
}

// NewRepoHandlers creates a new instance of RepoHandlers
func NewRepoHandlers(engine *Engine) *RepoHandlers {
	return &RepoHandlers{
		engine: engine,
	}
}

// ListRepos handles GET /{tenant_url}/api/v1/workspaces/{workspace_name}/repos
func (rh *RepoHandlers) ListRepos(w http.ResponseWriter, r *http.Request) {
	rh.engine.TrackOperation()
	defer rh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]

	if tenantURL == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if workspaceName == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "workspace_name is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		rh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if rh.engine.logger != nil {
		rh.engine.logger.Infof("List repos request for workspace: %s, tenant: %s, user: %s", workspaceName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ListReposRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
	}

	grpcResp, err := rh.engine.repoClient.ListRepos(ctx, grpcReq)
	if err != nil {
		rh.handleGRPCError(w, err, "Failed to list repos")
		return
	}

	// Convert gRPC response to REST response
	repos := make([]Repo, len(grpcResp.Repos))
	for i, repo := range grpcResp.Repos {
		repos[i] = Repo{
			TenantID:        repo.TenantId,
			WorkspaceID:     repo.WorkspaceId,
			RepoID:          repo.RepoId,
			RepoName:        repo.RepoName,
			RepoDescription: repo.RepoDescription,
			OwnerID:         repo.OwnerId,
		}
	}

	response := ListReposResponse{
		Repos: repos,
	}

	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Successfully listed %d repos for workspace: %s", len(repos), workspaceName)
	}

	rh.writeJSONResponse(w, http.StatusOK, response)
}

// ShowRepo handles GET /{tenant_url}/api/v1/workspaces/{workspace_name}/repos/{repo_name}
func (rh *RepoHandlers) ShowRepo(w http.ResponseWriter, r *http.Request) {
	rh.engine.TrackOperation()
	defer rh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	repoName := vars["repo_name"]

	if tenantURL == "" || workspaceName == "" || repoName == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and repo_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		rh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Show repo request for repo: %s, workspace: %s, tenant: %s", repoName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ShowRepoRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		RepoName:      repoName,
	}

	grpcResp, err := rh.engine.repoClient.ShowRepo(ctx, grpcReq)
	if err != nil {
		rh.handleGRPCError(w, err, "Failed to show repo")
		return
	}

	// Convert gRPC response to REST response
	repo := FullRepo{
		TenantID:        grpcResp.Repo.TenantId,
		WorkspaceID:     grpcResp.Repo.WorkspaceId,
		RepoID:          grpcResp.Repo.RepoId,
		RepoName:        grpcResp.Repo.RepoName,
		RepoDescription: grpcResp.Repo.RepoDescription,
		OwnerID:         grpcResp.Repo.OwnerId,
		Branches:        convertBranchesFromGRPC(grpcResp.Repo.Branches),
	}

	response := ShowRepoResponse{
		Repo: repo,
	}

	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Successfully showed repo: %s for workspace: %s", repoName, workspaceName)
	}

	rh.writeJSONResponse(w, http.StatusOK, response)
}

// AddRepo handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/repos
func (rh *RepoHandlers) AddRepo(w http.ResponseWriter, r *http.Request) {
	rh.engine.TrackOperation()
	defer rh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]

	if tenantURL == "" || workspaceName == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url and workspace_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		rh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req AddRepoRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if rh.engine.logger != nil {
			rh.engine.logger.Errorf("Failed to parse add repo request body: %v", err)
		}
		rh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.RepoName == "" || req.RepoDescription == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "Required fields missing", "repo_name and repo_description are required")
		return
	}

	// Log request
	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Add repo request for repo: %s, workspace: %s, tenant: %s", req.RepoName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.AddRepoRequest{
		TenantId:        profile.TenantId,
		WorkspaceName:   workspaceName,
		OwnerId:         profile.UserId,
		RepoName:        req.RepoName,
		RepoDescription: req.RepoDescription,
	}

	grpcResp, err := rh.engine.repoClient.AddRepo(ctx, grpcReq)
	if err != nil {
		rh.handleGRPCError(w, err, "Failed to add repo")
		return
	}

	// Convert gRPC response to REST response
	repo := Repo{
		TenantID:        grpcResp.Repo.TenantId,
		WorkspaceID:     grpcResp.Repo.WorkspaceId,
		RepoID:          grpcResp.Repo.RepoId,
		RepoName:        grpcResp.Repo.RepoName,
		RepoDescription: grpcResp.Repo.RepoDescription,
		OwnerID:         grpcResp.Repo.OwnerId,
	}

	response := AddRepoResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Repo:    repo,
		Status:  convertStatus(grpcResp.Status),
	}

	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Successfully added repo: %s for workspace: %s", req.RepoName, workspaceName)
	}

	rh.writeJSONResponse(w, http.StatusCreated, response)
}

// ModifyRepo handles PUT /{tenant_url}/api/v1/workspaces/{workspace_name}/repos/{repo_name}
func (rh *RepoHandlers) ModifyRepo(w http.ResponseWriter, r *http.Request) {
	rh.engine.TrackOperation()
	defer rh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	repoName := vars["repo_name"]

	if tenantURL == "" || workspaceName == "" || repoName == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and repo_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		rh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req ModifyRepoRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if rh.engine.logger != nil {
			rh.engine.logger.Errorf("Failed to parse modify repo request body: %v", err)
		}
		rh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Log request
	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Modify repo request for repo: %s, workspace: %s, tenant: %s", repoName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ModifyRepoRequest{
		TenantId:        profile.TenantId,
		WorkspaceName:   workspaceName,
		RepoName:        repoName,
		RepoNameNew:     &req.RepoNameNew,
		RepoDescription: &req.RepoDescription,
	}

	grpcResp, err := rh.engine.repoClient.ModifyRepo(ctx, grpcReq)
	if err != nil {
		rh.handleGRPCError(w, err, "Failed to modify repo")
		return
	}

	// Convert gRPC response to REST response
	repo := Repo{
		TenantID:        grpcResp.Repo.TenantId,
		WorkspaceID:     grpcResp.Repo.WorkspaceId,
		RepoID:          grpcResp.Repo.RepoId,
		RepoName:        grpcResp.Repo.RepoName,
		RepoDescription: grpcResp.Repo.RepoDescription,
		OwnerID:         grpcResp.Repo.OwnerId,
	}

	response := ModifyRepoResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Repo:    repo,
		Status:  convertStatus(grpcResp.Status),
	}

	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Successfully modified repo: %s for workspace: %s", repoName, workspaceName)
	}

	rh.writeJSONResponse(w, http.StatusOK, response)
}

// CloneRepo handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/repos/{repo_name}/clone
func (rh *RepoHandlers) CloneRepo(w http.ResponseWriter, r *http.Request) {
	rh.engine.TrackOperation()
	defer rh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	repoName := vars["repo_name"]

	if tenantURL == "" || workspaceName == "" || repoName == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and repo_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		rh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req CloneRepoRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if rh.engine.logger != nil {
			rh.engine.logger.Errorf("Failed to parse clone repo request body: %v", err)
		}
		rh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.CloneRepoName == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "Required fields missing", "clone_repo_name is required")
		return
	}

	// Log request
	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Clone repo request for repo: %s to %s, workspace: %s, tenant: %s", repoName, req.CloneRepoName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.CloneRepoRequest{
		TenantId:             profile.TenantId,
		WorkspaceName:        workspaceName,
		RepoName:             repoName,
		CloneRepoName:        req.CloneRepoName,
		CloneRepoDescription: req.CloneRepoDescription,
	}

	grpcResp, err := rh.engine.repoClient.CloneRepo(ctx, grpcReq)
	if err != nil {
		rh.handleGRPCError(w, err, "Failed to clone repo")
		return
	}

	// Convert gRPC response to REST response
	repo := Repo{
		TenantID:        grpcResp.Repo.TenantId,
		WorkspaceID:     grpcResp.Repo.WorkspaceId,
		RepoID:          grpcResp.Repo.RepoId,
		RepoName:        grpcResp.Repo.RepoName,
		RepoDescription: grpcResp.Repo.RepoDescription,
		OwnerID:         grpcResp.Repo.OwnerId,
	}

	response := CloneRepoResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Repo:    repo,
		Status:  convertStatus(grpcResp.Status),
	}

	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Successfully cloned repo: %s to %s for workspace: %s", repoName, req.CloneRepoName, workspaceName)
	}

	rh.writeJSONResponse(w, http.StatusCreated, response)
}

// DeleteRepo handles DELETE /{tenant_url}/api/v1/workspaces/{workspace_name}/repos/{repo_name}
func (rh *RepoHandlers) DeleteRepo(w http.ResponseWriter, r *http.Request) {
	rh.engine.TrackOperation()
	defer rh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	repoName := vars["repo_name"]

	if tenantURL == "" || workspaceName == "" || repoName == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and repo_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		rh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body (optional)
	var req DeleteRepoRequest
	if r.Body != nil {
		json.NewDecoder(r.Body).Decode(&req)
	}

	// Log request
	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Delete repo request for repo: %s, workspace: %s, tenant: %s", repoName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.DeleteRepoRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		RepoName:      repoName,
		Force:         req.Force,
	}

	grpcResp, err := rh.engine.repoClient.DeleteRepo(ctx, grpcReq)
	if err != nil {
		rh.handleGRPCError(w, err, "Failed to delete repo")
		return
	}

	response := DeleteRepoResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Status:  convertStatus(grpcResp.Status),
	}

	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Successfully deleted repo: %s for workspace: %s", repoName, workspaceName)
	}

	rh.writeJSONResponse(w, http.StatusOK, response)
}

// Helper methods

func (rh *RepoHandlers) handleGRPCError(w http.ResponseWriter, err error, defaultMessage string) {
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.NotFound:
			rh.writeErrorResponse(w, http.StatusNotFound, st.Message(), defaultMessage)
		case codes.AlreadyExists:
			rh.writeErrorResponse(w, http.StatusConflict, st.Message(), defaultMessage)
		case codes.InvalidArgument:
			rh.writeErrorResponse(w, http.StatusBadRequest, st.Message(), defaultMessage)
		case codes.PermissionDenied:
			rh.writeErrorResponse(w, http.StatusForbidden, st.Message(), defaultMessage)
		case codes.Unauthenticated:
			rh.writeErrorResponse(w, http.StatusUnauthorized, st.Message(), defaultMessage)
		default:
			rh.writeErrorResponse(w, http.StatusInternalServerError, st.Message(), defaultMessage)
		}
	} else {
		rh.writeErrorResponse(w, http.StatusInternalServerError, err.Error(), defaultMessage)
	}

	if rh.engine.logger != nil {
		rh.engine.logger.Errorf("Repo handler gRPC error: %v", err)
	}
}

func (rh *RepoHandlers) writeJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		if rh.engine.logger != nil {
			rh.engine.logger.Errorf("Failed to encode JSON response: %v", err)
		}
	}
}

func (rh *RepoHandlers) writeErrorResponse(w http.ResponseWriter, statusCode int, message, details string) {
	response := ErrorResponse{
		Error:   message,
		Message: details,
		Status:  StatusError,
	}
	rh.writeJSONResponse(w, statusCode, response)
}

// Helper function to convert a slice of gRPC Branches to REST Branches
func convertBranchesFromGRPC(grpcBranches []*corev1.Branch) []Branch {
	branches := make([]Branch, len(grpcBranches))
	for i, grpcBranch := range grpcBranches {
		branches[i] = convertBranchFromGRPC(grpcBranch)
	}
	return branches
}
