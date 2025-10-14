package protocol

// JSON-RPC 2.0 types and MCP protocol messages

// JSONRPCRequest represents a JSON-RPC 2.0 request
type JSONRPCRequest struct {
	JSONRPC string      `json:"jsonrpc"` // Must be "2.0"
	Method  string      `json:"method"`
	Params  interface{} `json:"params,omitempty"`
	ID      interface{} `json:"id"` // string, number, or null
}

// JSONRPCResponse represents a JSON-RPC 2.0 response
type JSONRPCResponse struct {
	JSONRPC string      `json:"jsonrpc"` // Must be "2.0"
	Result  interface{} `json:"result,omitempty"`
	Error   *RPCError   `json:"error,omitempty"`
	ID      interface{} `json:"id"`
}

// JSONRPCNotification represents a JSON-RPC 2.0 notification (no response expected)
type JSONRPCNotification struct {
	JSONRPC string      `json:"jsonrpc"` // Must be "2.0"
	Method  string      `json:"method"`
	Params  interface{} `json:"params,omitempty"`
}

// RPCError represents a JSON-RPC 2.0 error object
type RPCError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// Error implements the error interface
func (e *RPCError) Error() string {
	return e.Message
}

// Standard JSON-RPC 2.0 error codes
const (
	ParseError     = -32700
	InvalidRequest = -32600
	MethodNotFound = -32601
	InvalidParams  = -32602
	InternalError  = -32603
)

// MCP-specific error codes (application errors start at -32000)
const (
	UnauthorizedError     = -32000
	ForbiddenError        = -32001
	ResourceNotFoundError = -32002
	ToolExecutionError    = -32003
	PolicyViolationError  = -32004
)

// === MCP Protocol Messages ===

// InitializeRequest is sent by the client to initialize the MCP session
type InitializeRequest struct {
	ProtocolVersion string             `json:"protocolVersion"`
	Capabilities    ClientCapabilities `json:"capabilities"`
	ClientInfo      ImplementationInfo `json:"clientInfo"`
}

// InitializeResult is returned after successful initialization
type InitializeResult struct {
	ProtocolVersion string             `json:"protocolVersion"`
	Capabilities    ServerCapabilities `json:"capabilities"`
	ServerInfo      ImplementationInfo `json:"serverInfo"`
}

// ClientCapabilities describes what the client supports
type ClientCapabilities struct {
	Roots        *RootsCapability       `json:"roots,omitempty"`
	Sampling     *SamplingCapability    `json:"sampling,omitempty"`
	Experimental map[string]interface{} `json:"experimental,omitempty"`
}

// ServerCapabilities describes what the server supports
type ServerCapabilities struct {
	Resources    *ResourcesCapability   `json:"resources,omitempty"`
	Tools        *ToolsCapability       `json:"tools,omitempty"`
	Prompts      *PromptsCapability     `json:"prompts,omitempty"`
	Logging      *LoggingCapability     `json:"logging,omitempty"`
	Experimental map[string]interface{} `json:"experimental,omitempty"`
}

// RootsCapability indicates client can provide filesystem roots
type RootsCapability struct {
	ListChanged bool `json:"listChanged,omitempty"`
}

// SamplingCapability indicates client supports sampling
type SamplingCapability struct{}

// ResourcesCapability indicates server provides resources
type ResourcesCapability struct {
	Subscribe   bool `json:"subscribe,omitempty"`
	ListChanged bool `json:"listChanged,omitempty"`
}

// ToolsCapability indicates server provides tools
type ToolsCapability struct {
	ListChanged bool `json:"listChanged,omitempty"`
}

// PromptsCapability indicates server provides prompts
type PromptsCapability struct {
	ListChanged bool `json:"listChanged,omitempty"`
}

// LoggingCapability indicates server supports logging
type LoggingCapability struct{}

// ImplementationInfo describes the client or server implementation
type ImplementationInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// === Resources ===

// ListResourcesRequest requests the list of available resources
type ListResourcesRequest struct {
	Cursor string `json:"cursor,omitempty"`
}

// ListResourcesResult returns the list of resources
type ListResourcesResult struct {
	Resources  []Resource `json:"resources"`
	NextCursor string     `json:"nextCursor,omitempty"`
}

// Resource represents an MCP resource
type Resource struct {
	URI         string      `json:"uri"`
	Name        string      `json:"name"`
	Description string      `json:"description,omitempty"`
	MimeType    string      `json:"mimeType,omitempty"`
	Metadata    interface{} `json:"metadata,omitempty"`
}

// ReadResourceRequest requests the contents of a resource
type ReadResourceRequest struct {
	URI string `json:"uri"`
}

// ReadResourceResult returns the resource contents
type ReadResourceResult struct {
	Contents []ResourceContents `json:"contents"`
}

// ResourceContents represents the contents of a resource
type ResourceContents struct {
	URI      string      `json:"uri"`
	MimeType string      `json:"mimeType,omitempty"`
	Text     string      `json:"text,omitempty"`
	Blob     string      `json:"blob,omitempty"` // base64 encoded
	Metadata interface{} `json:"metadata,omitempty"`
}

// SubscribeRequest requests subscription to resource updates
type SubscribeRequest struct {
	URI string `json:"uri"`
}

// UnsubscribeRequest requests unsubscription from resource updates
type UnsubscribeRequest struct {
	URI string `json:"uri"`
}

// === Tools ===

// ListToolsRequest requests the list of available tools
type ListToolsRequest struct {
	Cursor string `json:"cursor,omitempty"`
}

// ListToolsResult returns the list of tools
type ListToolsResult struct {
	Tools      []Tool `json:"tools"`
	NextCursor string `json:"nextCursor,omitempty"`
}

// Tool represents an MCP tool
type Tool struct {
	Name        string      `json:"name"`
	Description string      `json:"description,omitempty"`
	InputSchema interface{} `json:"inputSchema"` // JSON Schema
}

// CallToolRequest requests execution of a tool
type CallToolRequest struct {
	Name      string                 `json:"name"`
	Arguments map[string]interface{} `json:"arguments,omitempty"`
}

// CallToolResult returns the result of tool execution
type CallToolResult struct {
	Content []ToolContent `json:"content,omitempty"`
	IsError bool          `json:"isError,omitempty"`
}

// ToolContent represents tool output
type ToolContent struct {
	Type     string      `json:"type"` // "text", "image", "resource"
	Text     string      `json:"text,omitempty"`
	Data     string      `json:"data,omitempty"` // base64 for images
	MimeType string      `json:"mimeType,omitempty"`
	Metadata interface{} `json:"metadata,omitempty"`
}

// === Prompts ===

// ListPromptsRequest requests the list of available prompts
type ListPromptsRequest struct {
	Cursor string `json:"cursor,omitempty"`
}

// ListPromptsResult returns the list of prompts
type ListPromptsResult struct {
	Prompts    []Prompt `json:"prompts"`
	NextCursor string   `json:"nextCursor,omitempty"`
}

// Prompt represents an MCP prompt
type Prompt struct {
	Name        string           `json:"name"`
	Description string           `json:"description,omitempty"`
	Arguments   []PromptArgument `json:"arguments,omitempty"`
}

// PromptArgument describes a prompt argument
type PromptArgument struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Required    bool   `json:"required,omitempty"`
}

// GetPromptRequest requests a prompt with arguments
type GetPromptRequest struct {
	Name      string                 `json:"name"`
	Arguments map[string]interface{} `json:"arguments,omitempty"`
}

// GetPromptResult returns the rendered prompt
type GetPromptResult struct {
	Description string          `json:"description,omitempty"`
	Messages    []PromptMessage `json:"messages"`
}

// PromptMessage represents a message in a prompt
type PromptMessage struct {
	Role    string        `json:"role"` // "user" or "assistant"
	Content PromptContent `json:"content"`
}

// PromptContent represents content in a prompt message
type PromptContent struct {
	Type     string `json:"type"` // "text", "image", "resource"
	Text     string `json:"text,omitempty"`
	Data     string `json:"data,omitempty"` // base64 for images
	MimeType string `json:"mimeType,omitempty"`
}

// === Logging ===

// SetLevelRequest sets the logging level
type SetLevelRequest struct {
	Level string `json:"level"` // "debug", "info", "notice", "warning", "error", "critical", "alert", "emergency"
}

// LoggingMessageNotification is sent by server to log a message
type LoggingMessageNotification struct {
	Level  string      `json:"level"`
	Logger string      `json:"logger,omitempty"`
	Data   interface{} `json:"data"`
}

// === Progress ===

// ProgressNotification indicates progress on a long-running operation
type ProgressNotification struct {
	ProgressToken string  `json:"progressToken"`
	Progress      float64 `json:"progress"` // 0 to 1
	Total         float64 `json:"total,omitempty"`
}
