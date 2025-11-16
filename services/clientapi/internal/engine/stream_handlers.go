package engine

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	securityv1 "github.com/redbco/redb-open/api/proto/security/v1"
	streamv1 "github.com/redbco/redb-open/api/proto/stream/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

type StreamHandlers struct {
	engine *Engine
}

func NewStreamHandlers(engine *Engine) *StreamHandlers {
	return &StreamHandlers{
		engine: engine,
	}
}

// ListStreams handles GET /{tenant_url}/api/v1/workspaces/{workspace_name}/streams
func (h *StreamHandlers) ListStreams(w http.ResponseWriter, r *http.Request) {
	h.engine.TrackOperation()
	defer h.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]

	if tenantURL == "" {
		http.Error(w, "tenant_url is required", http.StatusBadRequest)
		return
	}

	if workspaceName == "" {
		http.Error(w, "workspace_name is required", http.StatusBadRequest)
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		http.Error(w, "Profile not found in context", http.StatusInternalServerError)
		return
	}

	// Log request
	if h.engine.logger != nil {
		h.engine.logger.Infof("List streams request for workspace: %s, tenant: %s, user: %s", workspaceName, profile.TenantId, profile.UserId)
	}

	resp, err := h.engine.streamClient.ListStreams(r.Context(), &corev1.ListStreamsRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
	})

	if err != nil || !resp.Success {
		http.Error(w, fmt.Sprintf("Failed to list streams: %v", err), http.StatusInternalServerError)
		return
	}

	// Ensure streams is always an array, not null
	streams := resp.Streams
	if streams == nil {
		streams = []*corev1.Stream{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"streams": streams,
	})
}

// ShowStream handles GET /{tenant_url}/api/v1/workspaces/{workspace_name}/streams/{stream_name}
func (h *StreamHandlers) ShowStream(w http.ResponseWriter, r *http.Request) {
	h.engine.TrackOperation()
	defer h.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	streamName := vars["stream_name"]

	if tenantURL == "" {
		http.Error(w, "tenant_url is required", http.StatusBadRequest)
		return
	}

	if workspaceName == "" {
		http.Error(w, "workspace_name is required", http.StatusBadRequest)
		return
	}

	if streamName == "" {
		http.Error(w, "stream_name is required", http.StatusBadRequest)
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		http.Error(w, "Profile not found in context", http.StatusInternalServerError)
		return
	}

	resp, err := h.engine.streamClient.ShowStream(r.Context(), &corev1.ShowStreamRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		StreamName:    streamName,
	})

	if err != nil || !resp.Success {
		http.Error(w, fmt.Sprintf("Failed to get stream: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"stream": resp.Stream,
	})
}

// ConnectStream handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/streams/connect
func (h *StreamHandlers) ConnectStream(w http.ResponseWriter, r *http.Request) {
	h.engine.TrackOperation()
	defer h.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]

	if tenantURL == "" {
		http.Error(w, "tenant_url is required", http.StatusBadRequest)
		return
	}

	if workspaceName == "" {
		http.Error(w, "workspace_name is required", http.StatusBadRequest)
		return
	}

	// Get tenant_id and user_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		http.Error(w, "Profile not found in context", http.StatusInternalServerError)
		return
	}

	var req struct {
		StreamName        string                 `json:"stream_name"`
		StreamDescription string                 `json:"stream_description"`
		StreamPlatform    string                 `json:"stream_platform"`
		RegionName        string                 `json:"region_name"`
		ConnectionConfig  map[string]interface{} `json:"connection_config"`
		MonitoredTopics   []string               `json:"monitored_topics"`
		NodeID            int64                  `json:"node_id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	connectionConfig, _ := structpb.NewStruct(req.ConnectionConfig)

	resp, err := h.engine.streamClient.ConnectStream(r.Context(), &corev1.ConnectStreamRequest{
		TenantId:          profile.TenantId,
		WorkspaceName:     workspaceName,
		StreamName:        req.StreamName,
		StreamDescription: req.StreamDescription,
		StreamPlatform:    req.StreamPlatform,
		RegionName:        req.RegionName,
		ConnectionConfig:  connectionConfig,
		MonitoredTopics:   req.MonitoredTopics,
		NodeId:            req.NodeID,
		OwnerId:           profile.UserId,
	})

	if err != nil || !resp.Success {
		errorMsg := ""
		if err != nil {
			errorMsg = err.Error()
		} else if resp.Message != "" {
			errorMsg = resp.Message
		} else {
			errorMsg = "Unknown error"
		}
		http.Error(w, fmt.Sprintf("Failed to connect stream: %s", errorMsg), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"stream": resp.Stream,
	})
}

// ReconnectStream handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/streams/{stream_name}/reconnect
func (h *StreamHandlers) ReconnectStream(w http.ResponseWriter, r *http.Request) {
	h.engine.TrackOperation()
	defer h.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	streamName := vars["stream_name"]

	if tenantURL == "" {
		http.Error(w, "tenant_url is required", http.StatusBadRequest)
		return
	}

	if workspaceName == "" {
		http.Error(w, "workspace_name is required", http.StatusBadRequest)
		return
	}

	if streamName == "" {
		http.Error(w, "stream_name is required", http.StatusBadRequest)
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		http.Error(w, "Profile not found in context", http.StatusInternalServerError)
		return
	}

	resp, err := h.engine.streamClient.ReconnectStream(r.Context(), &corev1.ReconnectStreamRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		StreamName:    streamName,
	})

	if err != nil || !resp.Success {
		http.Error(w, fmt.Sprintf("Failed to reconnect stream: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"stream": resp.Stream,
	})
}

// DisconnectStream handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/streams/{stream_name}/disconnect
func (h *StreamHandlers) DisconnectStream(w http.ResponseWriter, r *http.Request) {
	h.engine.TrackOperation()
	defer h.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	streamName := vars["stream_name"]

	if tenantURL == "" {
		http.Error(w, "tenant_url is required", http.StatusBadRequest)
		return
	}

	if workspaceName == "" {
		http.Error(w, "workspace_name is required", http.StatusBadRequest)
		return
	}

	if streamName == "" {
		http.Error(w, "stream_name is required", http.StatusBadRequest)
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		http.Error(w, "Profile not found in context", http.StatusInternalServerError)
		return
	}

	var req struct {
		DeleteStream bool `json:"delete_stream"`
	}
	json.NewDecoder(r.Body).Decode(&req)

	resp, err := h.engine.streamClient.DisconnectStream(r.Context(), &corev1.DisconnectStreamRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		StreamName:    streamName,
		DeleteStream:  &req.DeleteStream,
	})

	if err != nil || !resp.Success {
		http.Error(w, fmt.Sprintf("Failed to disconnect stream: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNoContent)
}

// ModifyStream handles PUT /{tenant_url}/api/v1/workspaces/{workspace_name}/streams/{stream_name}
func (h *StreamHandlers) ModifyStream(w http.ResponseWriter, r *http.Request) {
	h.engine.TrackOperation()
	defer h.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	streamName := vars["stream_name"]

	if tenantURL == "" {
		http.Error(w, "tenant_url is required", http.StatusBadRequest)
		return
	}

	if workspaceName == "" {
		http.Error(w, "workspace_name is required", http.StatusBadRequest)
		return
	}

	if streamName == "" {
		http.Error(w, "stream_name is required", http.StatusBadRequest)
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		http.Error(w, "Profile not found in context", http.StatusInternalServerError)
		return
	}

	var req struct {
		StreamDescription string                 `json:"stream_description"`
		ConnectionConfig  map[string]interface{} `json:"connection_config"`
		MonitoredTopics   []string               `json:"monitored_topics"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	connectionConfig, _ := structpb.NewStruct(req.ConnectionConfig)

	resp, err := h.engine.streamClient.ModifyStream(r.Context(), &corev1.ModifyStreamRequest{
		TenantId:          profile.TenantId,
		WorkspaceName:     workspaceName,
		StreamName:        streamName,
		StreamDescription: &req.StreamDescription,
		ConnectionConfig:  connectionConfig,
		MonitoredTopics:   req.MonitoredTopics,
	})

	if err != nil || !resp.Success {
		http.Error(w, fmt.Sprintf("Failed to modify stream: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"stream": resp.Stream,
	})
}

// ListTopics handles GET /{tenant_url}/api/v1/workspaces/{workspace_name}/streams/{stream_name}/topics
func (h *StreamHandlers) ListTopics(w http.ResponseWriter, r *http.Request) {
	h.engine.TrackOperation()
	defer h.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	streamName := vars["stream_name"]

	if tenantURL == "" {
		http.Error(w, "tenant_url is required", http.StatusBadRequest)
		return
	}

	if workspaceName == "" {
		http.Error(w, "workspace_name is required", http.StatusBadRequest)
		return
	}

	if streamName == "" {
		http.Error(w, "stream_name is required", http.StatusBadRequest)
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		http.Error(w, "Profile not found in context", http.StatusInternalServerError)
		return
	}

	// Check if stream service client is available
	if h.engine.streamServiceClient == nil {
		http.Error(w, "Stream service is not available", http.StatusServiceUnavailable)
		return
	}

	// First, get the stream ID from core service
	showResp, err := h.engine.streamClient.ShowStream(r.Context(), &corev1.ShowStreamRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		StreamName:    streamName,
	})

	if err != nil || !showResp.Success || showResp.Stream == nil {
		http.Error(w, fmt.Sprintf("Failed to get stream: %v", err), http.StatusInternalServerError)
		return
	}

	// Call the stream service directly to list topics
	resp, err := h.engine.streamServiceClient.ListTopics(r.Context(), &streamv1.ListTopicsRequest{
		TenantId: profile.TenantId,
		StreamId: showResp.Stream.StreamId,
	})

	if err != nil || !resp.Success {
		http.Error(w, fmt.Sprintf("Failed to list topics: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"topics": resp.Topics,
	})
}

// GetTopicSchema handles GET /{tenant_url}/api/v1/workspaces/{workspace_name}/streams/{stream_name}/topics/{topic_name}/schema
func (h *StreamHandlers) GetTopicSchema(w http.ResponseWriter, r *http.Request) {
	h.engine.TrackOperation()
	defer h.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	streamName := vars["stream_name"]
	topicName := vars["topic_name"]

	if tenantURL == "" {
		http.Error(w, "tenant_url is required", http.StatusBadRequest)
		return
	}

	if workspaceName == "" {
		http.Error(w, "workspace_name is required", http.StatusBadRequest)
		return
	}

	if streamName == "" {
		http.Error(w, "stream_name is required", http.StatusBadRequest)
		return
	}

	if topicName == "" {
		http.Error(w, "topic_name is required", http.StatusBadRequest)
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		http.Error(w, "Profile not found in context", http.StatusInternalServerError)
		return
	}

	// Check if stream service client is available
	if h.engine.streamServiceClient == nil {
		http.Error(w, "Stream service is not available", http.StatusServiceUnavailable)
		return
	}

	// First, get the stream ID from core service
	showResp, err := h.engine.streamClient.ShowStream(r.Context(), &corev1.ShowStreamRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		StreamName:    streamName,
	})

	if err != nil || !showResp.Success || showResp.Stream == nil {
		http.Error(w, fmt.Sprintf("Failed to get stream: %v", err), http.StatusInternalServerError)
		return
	}

	// Call the stream service directly to get topic schema
	resp, err := h.engine.streamServiceClient.GetTopicSchema(r.Context(), &streamv1.GetTopicSchemaRequest{
		TenantId:  profile.TenantId,
		StreamId:  showResp.Stream.StreamId,
		TopicName: topicName,
	})

	if err != nil || !resp.Success {
		http.Error(w, fmt.Sprintf("Failed to get topic schema: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"topic_name":       resp.TopicName,
		"schema":           resp.Schema,
		"messages_sampled": resp.MessagesSampled,
		"confidence_score": resp.ConfidenceScore,
	})
}
