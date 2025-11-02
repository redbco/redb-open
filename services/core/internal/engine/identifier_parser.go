package engine

import (
	"fmt"

	"github.com/redbco/redb-open/pkg/unifiedmodel/resource"
)

// DatabaseIdentifierInfo contains parsed database identifier components
type DatabaseIdentifierInfo struct {
	DatabaseID   string
	TableName    string
	ColumnName   string
	ResourceAddr *resource.ResourceAddress
}

// parseResourceIdentifier parses a resource URI and extracts database identifier info
// Only supports new redb:// format
func (s *Server) parseResourceIdentifier(uri string) (*DatabaseIdentifierInfo, error) {
	// Parse the resource URI
	addr, err := resource.ParseResourceURI(uri)
	if err != nil {
		return nil, fmt.Errorf("failed to parse resource URI: %w", err)
	}

	// Validate it's a database resource
	if !addr.IsDatabase() {
		return nil, fmt.Errorf("URI must be a database resource (redb://), got: %s", addr.Protocol)
	}

	// Extract database components
	info := &DatabaseIdentifierInfo{
		DatabaseID:   addr.DatabaseID,
		ResourceAddr: addr,
	}

	// Extract table name from ObjectName
	if addr.ObjectType == resource.ObjectTypeTable {
		info.TableName = addr.ObjectName
	} else {
		return nil, fmt.Errorf("expected table object type, got: %s", addr.ObjectType)
	}

	// Extract column name from PathSegments
	if len(addr.PathSegments) > 0 && addr.PathSegments[0].Type == resource.SegmentTypeColumn {
		info.ColumnName = addr.PathSegments[0].Name
	} else if len(addr.PathSegments) == 0 {
		// Column is optional for some operations (like table-level operations)
		info.ColumnName = ""
	} else {
		return nil, fmt.Errorf("expected column path segment, got: %s", addr.PathSegments[0].Type)
	}

	return info, nil
}

