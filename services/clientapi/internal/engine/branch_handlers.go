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

// BranchHandlers contains the branch endpoint handlers
type BranchHandlers struct {
	engine *Engine
}

// NewBranchHandlers creates a new instance of BranchHandlers
func NewBranchHandlers(engine *Engine) *BranchHandlers {
	return &BranchHandlers{
		engine: engine,
	}
}

// ShowBranch handles GET /{tenant_url}/api/v1/workspaces/{workspace_name}/repos/{repo_name}/branches/{branch_name}
func (bh *BranchHandlers) ShowBranch(w http.ResponseWriter, r *http.Request) {
	bh.engine.TrackOperation()
	defer bh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	repoName := vars["repo_name"]
	branchName := vars["branch_name"]

	if tenantURL == "" || workspaceName == "" || repoName == "" || branchName == "" {
		bh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, repo_name, and branch_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		bh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if bh.engine.logger != nil {
		bh.engine.logger.Infof("Show branch request for branch: %s, repo: %s, workspace: %s, tenant: %s", branchName, repoName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ShowBranchRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		RepoName:      repoName,
		BranchName:    branchName,
	}

	grpcResp, err := bh.engine.branchClient.ShowBranch(ctx, grpcReq)
	if err != nil {
		bh.handleGRPCError(w, err, "Failed to show branch")
		return
	}

	// Convert gRPC response to REST response
	branch := convertBranchFromGRPC(grpcResp.Branch)

	response := ShowBranchResponse{
		Branch: branch,
	}

	if bh.engine.logger != nil {
		bh.engine.logger.Infof("Successfully showed branch: %s for repo: %s", branchName, repoName)
	}

	bh.writeJSONResponse(w, http.StatusOK, response)
}

// AttachBranch handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/repos/{repo_name}/branches/{branch_name}/attach
func (bh *BranchHandlers) AttachBranch(w http.ResponseWriter, r *http.Request) {
	bh.engine.TrackOperation()
	defer bh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	repoName := vars["repo_name"]
	branchName := vars["branch_name"]

	if tenantURL == "" || workspaceName == "" || repoName == "" || branchName == "" {
		bh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, repo_name, and branch_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		bh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req AttachBranchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if bh.engine.logger != nil {
			bh.engine.logger.Errorf("Failed to parse attach branch request body: %v", err)
		}
		bh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.DatabaseName == "" {
		bh.writeErrorResponse(w, http.StatusBadRequest, "Required fields missing", "database_name is required")
		return
	}

	// Log request
	if bh.engine.logger != nil {
		bh.engine.logger.Infof("Attach branch request for branch: %s, repo: %s, database: %s, workspace: %s, tenant: %s", branchName, repoName, req.DatabaseName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.AttachBranchRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		RepoName:      repoName,
		BranchName:    branchName,
		DatabaseName:  req.DatabaseName,
	}

	grpcResp, err := bh.engine.branchClient.AttachBranch(ctx, grpcReq)
	if err != nil {
		bh.handleGRPCError(w, err, "Failed to attach branch")
		return
	}

	// Convert gRPC response to REST response
	branch := convertBranchFromGRPC(grpcResp.Branch)

	response := AttachBranchResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Branch:  branch,
		Status:  convertStatus(grpcResp.Status),
	}

	if bh.engine.logger != nil {
		bh.engine.logger.Infof("Successfully attached branch: %s to database: %s", branchName, req.DatabaseName)
	}

	bh.writeJSONResponse(w, http.StatusOK, response)
}

// DetachBranch handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/repos/{repo_name}/branches/{branch_name}/detach
func (bh *BranchHandlers) DetachBranch(w http.ResponseWriter, r *http.Request) {
	bh.engine.TrackOperation()
	defer bh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	repoName := vars["repo_name"]
	branchName := vars["branch_name"]

	if tenantURL == "" || workspaceName == "" || repoName == "" || branchName == "" {
		bh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, repo_name, and branch_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		bh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if bh.engine.logger != nil {
		bh.engine.logger.Infof("Detach branch request for branch: %s, repo: %s, workspace: %s, tenant: %s", branchName, repoName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.DetachBranchRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		RepoName:      repoName,
		BranchName:    branchName,
	}

	grpcResp, err := bh.engine.branchClient.DetachBranch(ctx, grpcReq)
	if err != nil {
		bh.handleGRPCError(w, err, "Failed to detach branch")
		return
	}

	// Convert gRPC response to REST response
	branch := convertBranchFromGRPC(grpcResp.Branch)

	response := DetachBranchResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Branch:  branch,
		Status:  convertStatus(grpcResp.Status),
	}

	if bh.engine.logger != nil {
		bh.engine.logger.Infof("Successfully detached branch: %s", branchName)
	}

	bh.writeJSONResponse(w, http.StatusOK, response)
}

// ModifyBranch handles PUT /{tenant_url}/api/v1/workspaces/{workspace_name}/repos/{repo_name}/branches/{branch_name}
func (bh *BranchHandlers) ModifyBranch(w http.ResponseWriter, r *http.Request) {
	bh.engine.TrackOperation()
	defer bh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	repoName := vars["repo_name"]
	branchName := vars["branch_name"]

	if tenantURL == "" || workspaceName == "" || repoName == "" || branchName == "" {
		bh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, repo_name, and branch_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		bh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req ModifyBranchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if bh.engine.logger != nil {
			bh.engine.logger.Errorf("Failed to parse modify branch request body: %v", err)
		}
		bh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Log request
	if bh.engine.logger != nil {
		bh.engine.logger.Infof("Modify branch request for branch: %s, repo: %s, workspace: %s, tenant: %s", branchName, repoName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ModifyBranchRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		RepoName:      repoName,
		BranchName:    branchName,
		BranchNameNew: &req.BranchNameNew,
	}

	grpcResp, err := bh.engine.branchClient.ModifyBranch(ctx, grpcReq)
	if err != nil {
		bh.handleGRPCError(w, err, "Failed to modify branch")
		return
	}

	// Convert gRPC response to REST response
	branch := convertBranchFromGRPC(grpcResp.Branch)

	response := ModifyBranchResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Branch:  branch,
		Status:  convertStatus(grpcResp.Status),
	}

	if bh.engine.logger != nil {
		bh.engine.logger.Infof("Successfully modified branch: %s", branchName)
	}

	bh.writeJSONResponse(w, http.StatusOK, response)
}

// DeleteBranch handles DELETE /{tenant_url}/api/v1/workspaces/{workspace_name}/repos/{repo_name}/branches/{branch_name}
func (bh *BranchHandlers) DeleteBranch(w http.ResponseWriter, r *http.Request) {
	bh.engine.TrackOperation()
	defer bh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	repoName := vars["repo_name"]
	branchName := vars["branch_name"]

	if tenantURL == "" || workspaceName == "" || repoName == "" || branchName == "" {
		bh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, repo_name, and branch_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		bh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body (optional)
	var req DeleteBranchRequest
	if r.Body != nil {
		json.NewDecoder(r.Body).Decode(&req)
	}

	// Log request
	if bh.engine.logger != nil {
		bh.engine.logger.Infof("Delete branch request for branch: %s, repo: %s, workspace: %s, tenant: %s", branchName, repoName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.DeleteBranchRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		RepoName:      repoName,
		BranchName:    branchName,
		Force:         req.Force,
	}

	grpcResp, err := bh.engine.branchClient.DeleteBranch(ctx, grpcReq)
	if err != nil {
		bh.handleGRPCError(w, err, "Failed to delete branch")
		return
	}

	response := DeleteBranchResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Status:  convertStatus(grpcResp.Status),
	}

	if bh.engine.logger != nil {
		bh.engine.logger.Infof("Successfully deleted branch: %s", branchName)
	}

	bh.writeJSONResponse(w, http.StatusOK, response)
}

// Helper function to convert gRPC Branch to REST Branch
func convertBranchFromGRPC(grpcBranch *corev1.Branch) Branch {
	// Convert child branches
	childBranches := make([]Branch, len(grpcBranch.Branches))
	for i, child := range grpcBranch.Branches {
		childBranches[i] = convertBranchFromGRPC(child)
	}

	// Convert commits
	commits := make([]Commit, len(grpcBranch.Commits))
	for i, commit := range grpcBranch.Commits {
		commits[i] = Commit{
			TenantID:        commit.TenantId,
			WorkspaceID:     commit.WorkspaceId,
			RepoID:          commit.RepoId,
			BranchID:        commit.BranchId,
			CommitID:        commit.CommitId,
			CommitCode:      commit.CommitCode,
			IsHead:          commit.IsHead,
			CommitMessage:   commit.CommitMessage,
			SchemaType:      commit.SchemaType,
			SchemaStructure: commit.SchemaStructure,
			CommitDate:      commit.CommitDate,
		}
	}

	return Branch{
		TenantID:            grpcBranch.TenantId,
		WorkspaceID:         grpcBranch.WorkspaceId,
		RepoID:              grpcBranch.RepoId,
		BranchID:            grpcBranch.BranchId,
		BranchName:          grpcBranch.BranchName,
		ParentBranchID:      grpcBranch.ParentBranchId,
		ParentBranchName:    grpcBranch.ParentBranchName,
		ConnectedToDatabase: grpcBranch.ConnectedToDatabase,
		DatabaseID:          grpcBranch.DatabaseId,
		Branches:            childBranches,
		Commits:             commits,
		Status:              convertStatus(grpcBranch.Status),
	}
}

// Helper methods

func (bh *BranchHandlers) handleGRPCError(w http.ResponseWriter, err error, defaultMessage string) {
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.NotFound:
			bh.writeErrorResponse(w, http.StatusNotFound, st.Message(), defaultMessage)
		case codes.AlreadyExists:
			bh.writeErrorResponse(w, http.StatusConflict, st.Message(), defaultMessage)
		case codes.InvalidArgument:
			bh.writeErrorResponse(w, http.StatusBadRequest, st.Message(), defaultMessage)
		case codes.PermissionDenied:
			bh.writeErrorResponse(w, http.StatusForbidden, st.Message(), defaultMessage)
		case codes.Unauthenticated:
			bh.writeErrorResponse(w, http.StatusUnauthorized, st.Message(), defaultMessage)
		default:
			bh.writeErrorResponse(w, http.StatusInternalServerError, st.Message(), defaultMessage)
		}
	} else {
		bh.writeErrorResponse(w, http.StatusInternalServerError, err.Error(), defaultMessage)
	}

	if bh.engine.logger != nil {
		bh.engine.logger.Errorf("Branch handler gRPC error: %v", err)
	}
}

func (bh *BranchHandlers) writeJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		if bh.engine.logger != nil {
			bh.engine.logger.Errorf("Failed to encode JSON response: %v", err)
		}
	}
}

func (bh *BranchHandlers) writeErrorResponse(w http.ResponseWriter, statusCode int, message, details string) {
	response := ErrorResponse{
		Error:   message,
		Message: details,
		Status:  StatusError,
	}
	bh.writeJSONResponse(w, statusCode, response)
}
