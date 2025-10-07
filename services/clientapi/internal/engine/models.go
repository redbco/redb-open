package engine

// REST API models for authentication endpoints
// These correspond to the gRPC messages in core v2 proto

// Status represents the status of an operation
type Status string

const (
	StatusHealthy       Status = "healthy"
	StatusDegraded      Status = "degraded"
	StatusUnhealthy     Status = "unhealthy"
	StatusPending       Status = "pending"
	StatusUnknown       Status = "unknown"
	StatusSuccess       Status = "success"
	StatusFailure       Status = "failure"
	StatusStarting      Status = "starting"
	StatusStopping      Status = "stopping"
	StatusStopped       Status = "stopped"
	StatusStarted       Status = "started"
	StatusCreated       Status = "created"
	StatusDeleted       Status = "deleted"
	StatusUpdated       Status = "updated"
	StatusConnected     Status = "connected"
	StatusDisconnected  Status = "disconnected"
	StatusConnecting    Status = "connecting"
	StatusDisconnecting Status = "disconnecting"
	StatusReconnecting  Status = "reconnecting"
	StatusError         Status = "error"
	StatusWarning       Status = "warning"
	StatusInfo          Status = "info"
	StatusDebug         Status = "debug"
	StatusTrace         Status = "trace"
	StatusEmpty         Status = "empty"
	StatusJoining       Status = "joining"
	StatusLeaving       Status = "leaving"
	StatusSeeding       Status = "seeding"
	StatusOrphaned      Status = "orphaned"
	StatusSent          Status = "sent"
	StatusCancelled     Status = "cancelled"
	StatusProcessing    Status = "processing"
	StatusDone          Status = "done"
	StatusReceived      Status = "received"
	StatusActive        Status = "active"
	StatusClean         Status = "clean"
)

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
	Status  Status `json:"status"`
}
