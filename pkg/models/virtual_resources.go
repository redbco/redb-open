package models

// Binding mode constants for virtual resources
const (
	// BindingModeTemplate - Pure template, never auto-matches
	BindingModeTemplate = "template"

	// BindingModeUnbound - Created but not bound, no auto-matching until explicitly bound
	BindingModeUnbound = "unbound"

	// BindingModeBound - Explicitly bound to a specific database, participates in reconciliation
	BindingModeBound = "bound"

	// BindingModeAutoBind - Auto-binds to first matching database in workspace
	BindingModeAutoBind = "auto_bind"
)

// Reconciliation status constants for virtual resources
const (
	// ReconciliationStatusPending - Waiting for reconciliation
	ReconciliationStatusPending = "pending"

	// ReconciliationStatusMatched - Successfully matched with discovered resource
	ReconciliationStatusMatched = "matched"

	// ReconciliationStatusConflict - Found matching resource but has type conflicts
	ReconciliationStatusConflict = "conflict"

	// ReconciliationStatusOrphaned - No matching resource found in discovered schema
	ReconciliationStatusOrphaned = "orphaned"

	// ReconciliationStatusResolved - Conflict was resolved by user
	ReconciliationStatusResolved = "resolved"
)

// Virtual source constants
const (
	// VirtualSourceUser - Created manually by user
	VirtualSourceUser = "user"

	// VirtualSourceInferred - Automatically inferred from mapping
	VirtualSourceInferred = "inferred"

	// VirtualSourceTemplate - Created from template
	VirtualSourceTemplate = "template"

	// VirtualSourceMCP - Created for MCP resource/tool
	VirtualSourceMCP = "mcp"

	// VirtualSourceFromMapping - Created from existing mapping definition
	VirtualSourceFromMapping = "from_mapping"
)

// IsValidBindingMode checks if a binding mode string is valid
func IsValidBindingMode(mode string) bool {
	switch mode {
	case BindingModeTemplate, BindingModeUnbound, BindingModeBound, BindingModeAutoBind:
		return true
	default:
		return false
	}
}

// IsValidReconciliationStatus checks if a reconciliation status string is valid
func IsValidReconciliationStatus(status string) bool {
	switch status {
	case ReconciliationStatusPending, ReconciliationStatusMatched,
		ReconciliationStatusConflict, ReconciliationStatusOrphaned,
		ReconciliationStatusResolved:
		return true
	default:
		return false
	}
}

// IsValidVirtualSource checks if a virtual source string is valid
func IsValidVirtualSource(source string) bool {
	switch source {
	case VirtualSourceUser, VirtualSourceInferred, VirtualSourceTemplate,
		VirtualSourceMCP, VirtualSourceFromMapping:
		return true
	default:
		return false
	}
}
