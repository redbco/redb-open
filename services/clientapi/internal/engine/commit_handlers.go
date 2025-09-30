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

// CommitHandlers contains the commit endpoint handlers
type CommitHandlers struct {
	engine *Engine
}

// NewCommitHandlers creates a new instance of CommitHandlers
func NewCommitHandlers(engine *Engine) *CommitHandlers {
	return &CommitHandlers{
		engine: engine,
	}
}

// ShowCommit handles GET /{tenant_url}/api/v1/workspaces/{workspace_name}/repos/{repo_name}/branches/{branch_name}/commits/{commit_code}
func (ch *CommitHandlers) ShowCommit(w http.ResponseWriter, r *http.Request) {
	ch.engine.TrackOperation()
	defer ch.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	repoName := vars["repo_name"]
	branchName := vars["branch_name"]
	commitCode := vars["commit_code"]

	if tenantURL == "" || workspaceName == "" || repoName == "" || branchName == "" || commitCode == "" {
		ch.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, repo_name, branch_name, and commit_code are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		ch.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if ch.engine.logger != nil {
		ch.engine.logger.Infof("Show commit request for commit: %s, branch: %s, repo: %s, workspace: %s, tenant: %s", commitCode, branchName, repoName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ShowCommitRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		RepoName:      repoName,
		BranchName:    branchName,
		CommitCode:    commitCode,
	}

	grpcResp, err := ch.engine.commitClient.ShowCommit(ctx, grpcReq)
	if err != nil {
		ch.handleGRPCError(w, err, "Failed to show commit")
		return
	}

	// Convert gRPC response to REST response
	commit := Commit{
		TenantID:        grpcResp.Commit.TenantId,
		WorkspaceID:     grpcResp.Commit.WorkspaceId,
		RepoID:          grpcResp.Commit.RepoId,
		BranchID:        grpcResp.Commit.BranchId,
		CommitID:        grpcResp.Commit.CommitId,
		CommitCode:      grpcResp.Commit.CommitCode,
		IsHead:          grpcResp.Commit.IsHead,
		CommitMessage:   grpcResp.Commit.CommitMessage,
		SchemaType:      grpcResp.Commit.SchemaType,
		SchemaStructure: grpcResp.Commit.SchemaStructure,
		CommitDate:      grpcResp.Commit.CommitDate,
	}

	response := ShowCommitResponse{
		Commit: commit,
	}

	if ch.engine.logger != nil {
		ch.engine.logger.Infof("Successfully showed commit: %s for branch: %s", commitCode, branchName)
	}

	ch.writeJSONResponse(w, http.StatusOK, response)
}

// BranchCommit handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/repos/{repo_name}/branches/{branch_name}/commits/{commit_code}/branch
func (ch *CommitHandlers) BranchCommit(w http.ResponseWriter, r *http.Request) {
	ch.engine.TrackOperation()
	defer ch.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	repoName := vars["repo_name"]
	branchName := vars["branch_name"]
	commitCode := vars["commit_code"]

	if tenantURL == "" || workspaceName == "" || repoName == "" || branchName == "" || commitCode == "" {
		ch.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, repo_name, branch_name, and commit_code are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		ch.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req BranchCommitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if ch.engine.logger != nil {
			ch.engine.logger.Errorf("Failed to parse branch commit request body: %v", err)
		}
		ch.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.NewBranchName == "" {
		ch.writeErrorResponse(w, http.StatusBadRequest, "Required fields missing", "new_branch_name is required")
		return
	}

	// Log request
	if ch.engine.logger != nil {
		ch.engine.logger.Infof("Branch commit request for commit: %s to new branch: %s, branch: %s, repo: %s, workspace: %s, tenant: %s", commitCode, req.NewBranchName, branchName, repoName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.BranchCommitRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		RepoName:      repoName,
		BranchName:    branchName,
		CommitCode:    commitCode,
		NewBranchName: req.NewBranchName,
	}

	grpcResp, err := ch.engine.commitClient.BranchCommit(ctx, grpcReq)
	if err != nil {
		ch.handleGRPCError(w, err, "Failed to branch commit")
		return
	}

	// Convert gRPC response to REST response
	commit := Commit{
		TenantID:        grpcResp.Commit.TenantId,
		WorkspaceID:     grpcResp.Commit.WorkspaceId,
		RepoID:          grpcResp.Commit.RepoId,
		BranchID:        grpcResp.Commit.BranchId,
		CommitID:        grpcResp.Commit.CommitId,
		CommitCode:      grpcResp.Commit.CommitCode,
		IsHead:          grpcResp.Commit.IsHead,
		CommitMessage:   grpcResp.Commit.CommitMessage,
		SchemaType:      grpcResp.Commit.SchemaType,
		SchemaStructure: grpcResp.Commit.SchemaStructure,
		CommitDate:      grpcResp.Commit.CommitDate,
	}

	response := BranchCommitResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Commit:  commit,
		Status:  convertStatus(grpcResp.Status),
	}

	if ch.engine.logger != nil {
		ch.engine.logger.Infof("Successfully created branch from commit: %s to branch: %s", commitCode, req.NewBranchName)
	}

	ch.writeJSONResponse(w, http.StatusCreated, response)
}

// MergeCommit handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/repos/{repo_name}/branches/{branch_name}/commits/{commit_code}/merge
func (ch *CommitHandlers) MergeCommit(w http.ResponseWriter, r *http.Request) {
	ch.engine.TrackOperation()
	defer ch.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	repoName := vars["repo_name"]
	branchName := vars["branch_name"]
	commitCode := vars["commit_code"]

	if tenantURL == "" || workspaceName == "" || repoName == "" || branchName == "" || commitCode == "" {
		ch.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, repo_name, branch_name, and commit_code are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		ch.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if ch.engine.logger != nil {
		ch.engine.logger.Infof("Merge commit request for commit: %s, branch: %s, repo: %s, workspace: %s, tenant: %s", commitCode, branchName, repoName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.MergeCommitRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		RepoName:      repoName,
		BranchName:    branchName,
		CommitCode:    commitCode,
	}

	grpcResp, err := ch.engine.commitClient.MergeCommit(ctx, grpcReq)
	if err != nil {
		ch.handleGRPCError(w, err, "Failed to merge commit")
		return
	}

	// Convert gRPC response to REST response
	commit := Commit{
		TenantID:        grpcResp.Commit.TenantId,
		WorkspaceID:     grpcResp.Commit.WorkspaceId,
		RepoID:          grpcResp.Commit.RepoId,
		BranchID:        grpcResp.Commit.BranchId,
		CommitID:        grpcResp.Commit.CommitId,
		CommitCode:      grpcResp.Commit.CommitCode,
		IsHead:          grpcResp.Commit.IsHead,
		CommitMessage:   grpcResp.Commit.CommitMessage,
		SchemaType:      grpcResp.Commit.SchemaType,
		SchemaStructure: grpcResp.Commit.SchemaStructure,
		CommitDate:      grpcResp.Commit.CommitDate,
	}

	response := MergeCommitResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Commit:  commit,
		Status:  convertStatus(grpcResp.Status),
	}

	if ch.engine.logger != nil {
		ch.engine.logger.Infof("Successfully merged commit: %s", commitCode)
	}

	ch.writeJSONResponse(w, http.StatusOK, response)
}

// DeployCommit handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/repos/{repo_name}/branches/{branch_name}/commits/{commit_code}/deploy
func (ch *CommitHandlers) DeployCommit(w http.ResponseWriter, r *http.Request) {
	ch.engine.TrackOperation()
	defer ch.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	repoName := vars["repo_name"]
	branchName := vars["branch_name"]
	commitCode := vars["commit_code"]

	if tenantURL == "" || workspaceName == "" || repoName == "" || branchName == "" || commitCode == "" {
		ch.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, repo_name, branch_name, and commit_code are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		ch.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if ch.engine.logger != nil {
		ch.engine.logger.Infof("Deploy commit request for commit: %s, branch: %s, repo: %s, workspace: %s, tenant: %s", commitCode, branchName, repoName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.DeployCommitRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		RepoName:      repoName,
		BranchName:    branchName,
		CommitCode:    commitCode,
	}

	grpcResp, err := ch.engine.commitClient.DeployCommit(ctx, grpcReq)
	if err != nil {
		ch.handleGRPCError(w, err, "Failed to deploy commit")
		return
	}

	// Convert gRPC response to REST response
	commit := Commit{
		TenantID:        grpcResp.Commit.TenantId,
		WorkspaceID:     grpcResp.Commit.WorkspaceId,
		RepoID:          grpcResp.Commit.RepoId,
		BranchID:        grpcResp.Commit.BranchId,
		CommitID:        grpcResp.Commit.CommitId,
		CommitCode:      grpcResp.Commit.CommitCode,
		IsHead:          grpcResp.Commit.IsHead,
		CommitMessage:   grpcResp.Commit.CommitMessage,
		SchemaType:      grpcResp.Commit.SchemaType,
		SchemaStructure: grpcResp.Commit.SchemaStructure,
		CommitDate:      grpcResp.Commit.CommitDate,
	}

	response := DeployCommitResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Commit:  commit,
		Status:  convertStatus(grpcResp.Status),
	}

	if ch.engine.logger != nil {
		ch.engine.logger.Infof("Successfully deployed commit: %s", commitCode)
	}

	ch.writeJSONResponse(w, http.StatusOK, response)
}

// DeployCommitSchemaRequest represents the request payload for deploying commit schema
type DeployCommitSchemaRequest struct {
	RepoName     string                    `json:"repo_name"`
	BranchName   string                    `json:"branch_name"`
	CommitCode   string                    `json:"commit_code"`
	Target       DeployCommitSchemaTarget  `json:"target"`
	Options      DeployCommitSchemaOptions `json:"options"`
	SourceNodeID *uint64                   `json:"source_node_id,omitempty"`
	TargetNodeID *uint64                   `json:"target_node_id,omitempty"`
}

type DeployCommitSchemaTarget struct {
	NewDatabase      *NewDatabaseTarget      `json:"new_database,omitempty"`
	ExistingDatabase *ExistingDatabaseTarget `json:"existing_database,omitempty"`
}

type DeployCommitSchemaOptions struct {
	Wipe                  bool              `json:"wipe"`
	Merge                 bool              `json:"merge"`
	TransformationOptions map[string]string `json:"transformation_options,omitempty"`
}

// DeployCommitSchemaResponse represents the response from deploying commit schema
type DeployCommitSchemaResponse struct {
	Message          string   `json:"message"`
	Success          bool     `json:"success"`
	Status           string   `json:"status"`
	TargetDatabaseId string   `json:"target_database_id"`
	TargetRepoId     string   `json:"target_repo_id"`
	TargetBranchId   string   `json:"target_branch_id"`
	TargetCommitId   string   `json:"target_commit_id"`
	Warnings         []string `json:"warnings"`
}

// DeployCommitSchema handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/commits/deploy-schema
func (ch *CommitHandlers) DeployCommitSchema(w http.ResponseWriter, r *http.Request) {
	ch.engine.TrackOperation()
	defer ch.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]

	if tenantURL == "" || workspaceName == "" {
		ch.writeErrorResponse(w, http.StatusBadRequest, "tenant_url and workspace_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		ch.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req DeployCommitSchemaRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		ch.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	// Validate request
	if req.RepoName == "" || req.BranchName == "" || req.CommitCode == "" {
		ch.writeErrorResponse(w, http.StatusBadRequest, "repo_name, branch_name, and commit_code are required", "")
		return
	}

	// Validate target (must have exactly one)
	if req.Target.NewDatabase == nil && req.Target.ExistingDatabase == nil {
		ch.writeErrorResponse(w, http.StatusBadRequest, "target must be specified (new_database or existing_database)", "")
		return
	}

	if req.Target.NewDatabase != nil && req.Target.ExistingDatabase != nil {
		ch.writeErrorResponse(w, http.StatusBadRequest, "only one target type can be specified", "")
		return
	}

	// Log request
	if ch.engine.logger != nil {
		ch.engine.logger.Infof("Deploy commit schema request: repo=%s, branch=%s, commit=%s, workspace=%s, tenant=%s, user=%s",
			req.RepoName, req.BranchName, req.CommitCode, workspaceName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 300*time.Second) // 5 minutes for potentially long operation
	defer cancel()

	// Build gRPC request
	grpcReq := &corev1.DeployCommitSchemaRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		RepoName:      req.RepoName,
		BranchName:    req.BranchName,
		CommitCode:    req.CommitCode,
		Options: &corev1.DeploymentOptions{
			Wipe:                  req.Options.Wipe,
			Merge:                 req.Options.Merge,
			TransformationOptions: req.Options.TransformationOptions,
		},
	}

	// Set target
	if req.Target.NewDatabase != nil {
		grpcReq.Target = &corev1.DeployCommitSchemaRequest_NewDatabase{
			NewDatabase: &corev1.NewDatabaseTarget{
				InstanceName: req.Target.NewDatabase.InstanceName,
				DatabaseName: req.Target.NewDatabase.DatabaseName,
			},
		}
	} else if req.Target.ExistingDatabase != nil {
		grpcReq.Target = &corev1.DeployCommitSchemaRequest_ExistingDatabase{
			ExistingDatabase: &corev1.ExistingDatabaseTarget{
				DatabaseName: req.Target.ExistingDatabase.DatabaseName,
				Wipe:         req.Target.ExistingDatabase.Wipe,
				Merge:        req.Target.ExistingDatabase.Merge,
			},
		}
	}

	// Call appropriate gRPC method based on cross-node requirements
	var grpcResp *corev1.DeployCommitSchemaResponse
	var err error

	if req.SourceNodeID != nil && req.TargetNodeID != nil {
		// Cross-node operation
		remoteReq := &corev1.DeployCommitSchemaRemoteRequest{
			Request:      grpcReq,
			SourceNodeId: *req.SourceNodeID,
			TargetNodeId: *req.TargetNodeID,
		}
		remoteResp, err := ch.engine.commitClient.DeployCommitSchemaRemote(ctx, remoteReq)
		if err != nil {
			ch.handleGRPCError(w, err, "Failed to deploy commit schema across nodes")
			return
		}
		// Convert remote response to regular response
		grpcResp = &corev1.DeployCommitSchemaResponse{
			Message:          remoteResp.Message,
			Success:          remoteResp.Success,
			Status:           remoteResp.Status,
			TargetDatabaseId: remoteResp.TargetDatabaseId,
			TargetRepoId:     remoteResp.TargetRepoId,
			TargetBranchId:   remoteResp.TargetBranchId,
			TargetCommitId:   remoteResp.TargetCommitId,
			Warnings:         remoteResp.Warnings,
		}
	} else {
		// Same-node operation
		grpcResp, err = ch.engine.commitClient.DeployCommitSchema(ctx, grpcReq)
		if err != nil {
			ch.handleGRPCError(w, err, "Failed to deploy commit schema")
			return
		}
	}

	// Build response
	response := DeployCommitSchemaResponse{
		Message:          grpcResp.Message,
		Success:          grpcResp.Success,
		Status:           string(convertStatus(grpcResp.Status)),
		TargetDatabaseId: grpcResp.TargetDatabaseId,
		TargetRepoId:     grpcResp.TargetRepoId,
		TargetBranchId:   grpcResp.TargetBranchId,
		TargetCommitId:   grpcResp.TargetCommitId,
		Warnings:         grpcResp.Warnings,
	}

	if ch.engine.logger != nil {
		ch.engine.logger.Infof("Successfully deployed commit schema: repo=%s, branch=%s, commit=%s, target=%s",
			req.RepoName, req.BranchName, req.CommitCode, grpcResp.TargetDatabaseId)
	}

	ch.writeJSONResponse(w, http.StatusOK, response)
}

// Helper methods

func (ch *CommitHandlers) handleGRPCError(w http.ResponseWriter, err error, defaultMessage string) {
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.NotFound:
			ch.writeErrorResponse(w, http.StatusNotFound, st.Message(), defaultMessage)
		case codes.AlreadyExists:
			ch.writeErrorResponse(w, http.StatusConflict, st.Message(), defaultMessage)
		case codes.InvalidArgument:
			ch.writeErrorResponse(w, http.StatusBadRequest, st.Message(), defaultMessage)
		case codes.PermissionDenied:
			ch.writeErrorResponse(w, http.StatusForbidden, st.Message(), defaultMessage)
		case codes.Unauthenticated:
			ch.writeErrorResponse(w, http.StatusUnauthorized, st.Message(), defaultMessage)
		default:
			ch.writeErrorResponse(w, http.StatusInternalServerError, st.Message(), defaultMessage)
		}
	} else {
		ch.writeErrorResponse(w, http.StatusInternalServerError, err.Error(), defaultMessage)
	}

	if ch.engine.logger != nil {
		ch.engine.logger.Errorf("Commit handler gRPC error: %v", err)
	}
}

func (ch *CommitHandlers) writeJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		if ch.engine.logger != nil {
			ch.engine.logger.Errorf("Failed to encode JSON response: %v", err)
		}
	}
}

func (ch *CommitHandlers) writeErrorResponse(w http.ResponseWriter, statusCode int, message, details string) {
	response := ErrorResponse{
		Error:   message,
		Message: details,
		Status:  StatusError,
	}
	ch.writeJSONResponse(w, statusCode, response)
}
