package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	"github.com/redbco/redb-open/services/core/internal/services/anchor"
	"github.com/redbco/redb-open/services/core/internal/services/database"
	"github.com/redbco/redb-open/services/core/internal/services/environment"
	"github.com/redbco/redb-open/services/core/internal/services/instance"
	"github.com/redbco/redb-open/services/core/internal/services/mapping"
	"github.com/redbco/redb-open/services/core/internal/services/mesh"
	"github.com/redbco/redb-open/services/core/internal/services/policy"
	"github.com/redbco/redb-open/services/core/internal/services/region"
	"github.com/redbco/redb-open/services/core/internal/services/relationship"
	"github.com/redbco/redb-open/services/core/internal/services/repo"
	"github.com/redbco/redb-open/services/core/internal/services/satellite"
	"github.com/redbco/redb-open/services/core/internal/services/tenant"
	"github.com/redbco/redb-open/services/core/internal/services/transformation"
	"github.com/redbco/redb-open/services/core/internal/services/user"
	"github.com/redbco/redb-open/services/core/internal/services/workspace"
	"google.golang.org/protobuf/types/known/structpb"
)

// Helper function to convert policy to protobuf
func (s *Server) policyToProto(p *policy.Policy) (*corev1.Policy, error) {
	// Parse JSON object into protobuf Struct
	policyStruct, err := structpb.NewStruct(p.Conditions)
	if err != nil {
		return nil, fmt.Errorf("failed to convert policy conditions to struct: %w", err)
	}

	return &corev1.Policy{
		TenantId:          p.TenantID,
		PolicyId:          p.ID,
		PolicyName:        p.Name,
		PolicyDescription: p.Description,
		PolicyObject:      policyStruct,
		OwnerId:           p.OwnerID,
	}, nil
}

// meshToProto converts a mesh service model to protobuf
func (s *Server) meshToProto(m *mesh.Mesh) *corev1.Mesh {
	// Convert enum to boolean: 'OPEN' -> true, others -> false
	allowJoin := m.AllowJoin == "OPEN"

	return &corev1.Mesh{
		MeshId:          fmt.Sprintf("%d", m.ID),
		MeshName:        m.Name,
		MeshDescription: m.Description,
		AllowJoin:       allowJoin,
		NodeCount:       m.NodeCount,
		Status:          statusStringToProto(m.Status),
	}
}

// nodeToProto converts a node service model to protobuf (legacy format)
func (s *Server) nodeToProto(n *mesh.Node) *corev1.Node {

	// Map status to NodeStatus enum
	var nodeStatus corev1.NodeStatus
	switch n.Status {
	case "STATUS_CLEAN":
		nodeStatus = corev1.NodeStatus_NODE_STATUS_CLEAN
	case "STATUS_JOINING":
		nodeStatus = corev1.NodeStatus_NODE_STATUS_JOINING
	case "STATUS_ACTIVE":
		nodeStatus = corev1.NodeStatus_NODE_STATUS_ACTIVE
	case "STATUS_LEAVING":
		nodeStatus = corev1.NodeStatus_NODE_STATUS_LEAVING
	case "STATUS_OFFLINE":
		nodeStatus = corev1.NodeStatus_NODE_STATUS_OFFLINE
	default:
		nodeStatus = corev1.NodeStatus_NODE_STATUS_UNSPECIFIED
	}

	return &corev1.Node{
		NodeId:          fmt.Sprintf("%d", n.ID),
		NodeName:        n.Name,
		NodeDescription: n.Description,
		NodePlatform:    n.Platform,
		NodeVersion:     n.Version,
		RegionId:        n.RegionID,
		RegionName:      n.RegionName,
		IpAddress:       n.IPAddress,
		Port:            n.Port,
		NodeStatus:      nodeStatus,
		CreatedAt:       n.Created.Unix(),
		UpdatedAt:       n.Updated.Unix(),
	}
}

// satelliteToProto converts a satellite service model to protobuf
func (s *Server) satelliteToProto(sat *satellite.Satellite) *corev1.Satellite {
	return &corev1.Satellite{
		TenantId:             sat.TenantID,
		SatelliteId:          sat.ID,
		SatelliteName:        sat.Name,
		SatelliteDescription: sat.Description,
		SatellitePlatform:    sat.Platform,
		SatelliteVersion:     sat.Version,
		IpAddress:            sat.IPAddress,
		NodeId:               sat.ConnectedNodeID,
		Status:               statusStringToProto(sat.Status),
		OwnerId:              sat.OwnerID,
	}
}

// anchorToProto converts an anchor service model to protobuf
func (s *Server) anchorToProto(anc *anchor.Anchor) *corev1.Anchor {
	nodeId := ""
	if anc.ConnectedNodeID != nil {
		nodeId = *anc.ConnectedNodeID
	}

	return &corev1.Anchor{
		TenantId:          anc.TenantID,
		AnchorId:          anc.ID,
		AnchorName:        anc.Name,
		AnchorDescription: anc.Description,
		AnchorPlatform:    anc.Platform,
		AnchorVersion:     anc.Version,
		IpAddress:         anc.IPAddress,
		NodeId:            nodeId,
		Status:            statusStringToProto(anc.Status),
		OwnerId:           anc.OwnerID,
	}
}

// instanceToProto converts an instance service model to protobuf
func (s *Server) instanceToProto(inst *instance.Instance) *corev1.Instance {
	environmentId := ""
	if inst.EnvironmentID != nil {
		environmentId = *inst.EnvironmentID
	}

	sslCert := ""
	if inst.SSLCert != nil {
		sslCert = *inst.SSLCert
	}

	sslKey := ""
	if inst.SSLKey != nil {
		sslKey = *inst.SSLKey
	}

	sslRootCert := ""
	if inst.SSLRootCert != nil {
		sslRootCert = *inst.SSLRootCert
	}

	// Convert metadata map to protobuf Struct
	var metadataStruct *structpb.Struct
	if len(inst.Metadata) > 0 {
		var err error
		metadataStruct, err = structpb.NewStruct(inst.Metadata)
		if err != nil {
			// Log error but continue - metadata is optional
			s.engine.logger.Warnf("Failed to convert metadata to protobuf Struct: %v", err)
		}
	}

	return &corev1.Instance{
		TenantId:                 inst.TenantID,
		WorkspaceId:              inst.WorkspaceID,
		EnvironmentId:            environmentId,
		InstanceId:               inst.ID,
		InstanceName:             inst.Name,
		InstanceDescription:      inst.Description,
		InstanceType:             inst.Type,
		InstanceVendor:           inst.Vendor,
		InstanceVersion:          inst.Version,
		InstanceUniqueIdentifier: inst.UniqueIdentifier,
		ConnectedToNodeId:        inst.ConnectedToNodeID,
		InstanceHost:             inst.Host,
		InstancePort:             inst.Port,
		InstanceUsername:         inst.Username,
		InstancePassword:         inst.Password,
		InstanceSystemDbName:     inst.SystemDBName,
		InstanceEnabled:          inst.Enabled,
		InstanceSsl:              inst.SSL,
		InstanceSslMode:          inst.SSLMode,
		InstanceSslCert:          sslCert,
		InstanceSslKey:           sslKey,
		InstanceSslRootCert:      sslRootCert,
		InstanceMetadata:         metadataStruct,
		PolicyIds:                inst.PolicyIDs,
		OwnerId:                  inst.OwnerID,
		InstanceStatusMessage:    inst.StatusMessage,
		Status:                   statusStringToProto(inst.Status),
		Created:                  inst.Created.Format("2006-01-02T15:04:05Z"),
		Updated:                  inst.Updated.Format("2006-01-02T15:04:05Z"),
	}
}

// instanceToRecordData converts an instance to record data for broadcasting
func (s *Server) instanceToRecordData(inst *instance.Instance) map[string]interface{} {
	recordData := map[string]interface{}{
		"instance_id":                inst.ID,
		"tenant_id":                  inst.TenantID,
		"workspace_id":               inst.WorkspaceID,
		"connected_to_node_id":       inst.ConnectedToNodeID,
		"instance_name":              inst.Name,
		"instance_description":       inst.Description,
		"instance_type":              inst.Type,
		"instance_vendor":            inst.Vendor,
		"instance_version":           inst.Version,
		"instance_unique_identifier": inst.UniqueIdentifier,
		"instance_host":              inst.Host,
		"instance_port":              inst.Port,
		"instance_username":          inst.Username,
		"instance_password":          inst.Password,
		"instance_system_db_name":    inst.SystemDBName,
		"instance_enabled":           inst.Enabled,
		"instance_ssl":               inst.SSL,
		"instance_ssl_mode":          inst.SSLMode,
		"owner_id":                   inst.OwnerID,
		"instance_status_message":    inst.StatusMessage,
		"status":                     inst.Status,
	}

	// Add optional fields
	if inst.EnvironmentID != nil {
		recordData["environment_id"] = *inst.EnvironmentID
	}
	if inst.SSLCert != nil {
		recordData["instance_ssl_cert"] = *inst.SSLCert
	}
	if inst.SSLKey != nil {
		recordData["instance_ssl_key"] = *inst.SSLKey
	}
	if inst.SSLRootCert != nil {
		recordData["instance_ssl_root_cert"] = *inst.SSLRootCert
	}

	return recordData
}

// databaseToProto converts a database service model to protobuf
func (s *Server) databaseToProto(db *database.Database) *corev1.Database {
	environmentId := ""
	if db.EnvironmentID != nil {
		environmentId = *db.EnvironmentID
	}

	// Convert schema structure to JSON string
	schemaJSON := "{}"
	if db.Schema != "" {
		if jsonBytes, err := json.Marshal(db.Schema); err == nil {
			schemaJSON = string(jsonBytes)
		}
	}

	// Convert tables structure to JSON string
	tablesJSON := "{}"
	if db.Tables != "" {
		if jsonBytes, err := json.Marshal(db.Tables); err == nil {
			tablesJSON = string(jsonBytes)
		}
	}

	// Fetch resource containers and items from resource registry
	databaseService := database.NewService(s.engine.db, s.engine.logger)
	schemaResponse, err := databaseService.GetSchemaFromResourceRegistry(context.Background(), db.TenantID, db.ID)

	var protoContainers []*corev1.DatabaseResourceContainer
	if err != nil {
		s.engine.logger.Warnf("Failed to fetch resource containers for database %s: %v", db.ID, err)
		protoContainers = []*corev1.DatabaseResourceContainer{}
	} else {
		protoContainers = make([]*corev1.DatabaseResourceContainer, len(schemaResponse.Containers))
		for i, container := range schemaResponse.Containers {
			protoContainers[i] = s.containerToProto(&container)
		}
	}

	return &corev1.Database{
		TenantId:              db.TenantID,
		WorkspaceId:           db.WorkspaceID,
		EnvironmentId:         environmentId,
		ConnectedToNodeId:     db.ConnectedToNodeID,
		InstanceId:            db.InstanceID,
		InstanceName:          db.InstanceName,
		DatabaseId:            db.ID,
		DatabaseName:          db.Name,
		DatabaseDescription:   db.Description,
		DatabaseType:          db.Type,
		DatabaseVendor:        db.Vendor,
		DatabaseVersion:       db.Version,
		DatabaseUsername:      db.Username,
		DatabasePassword:      db.Password,
		DatabaseDbName:        db.DBName,
		DatabaseEnabled:       db.Enabled,
		PolicyIds:             db.PolicyIDs,
		OwnerId:               db.OwnerID,
		DatabaseStatusMessage: db.StatusMessage,
		Status:                statusStringToProto(db.Status),
		Created:               db.Created.Format("2006-01-02T15:04:05Z"),
		Updated:               db.Updated.Format("2006-01-02T15:04:05Z"),
		DatabaseSchema:        schemaJSON,
		DatabaseTables:        tablesJSON,
		InstanceHost:          db.InstanceHost,
		InstancePort:          db.InstancePort,
		InstanceSslMode:       db.InstanceSSLMode,
		InstanceSsl:           db.InstanceSSL,
		InstanceStatusMessage: db.InstanceStatusMessage,
		InstanceStatus:        db.InstanceStatus,
		ResourceContainers:    protoContainers,
	}
}

// databaseToRecordData converts a database to record data for broadcasting
func (s *Server) databaseToRecordData(db *database.Database) map[string]interface{} {
	recordData := map[string]interface{}{
		"database_id":             db.ID,
		"tenant_id":               db.TenantID,
		"workspace_id":            db.WorkspaceID,
		"connected_to_node_id":    db.ConnectedToNodeID,
		"instance_id":             db.InstanceID,
		"database_name":           db.Name,
		"database_description":    db.Description,
		"database_type":           db.Type,
		"database_vendor":         db.Vendor,
		"database_version":        db.Version,
		"database_username":       db.Username,
		"database_password":       db.Password,
		"database_db_name":        db.DBName,
		"database_enabled":        db.Enabled,
		"owner_id":                db.OwnerID,
		"database_status_message": db.StatusMessage,
		"status":                  db.Status,
	}

	// Add optional fields
	if db.EnvironmentID != nil {
		recordData["environment_id"] = *db.EnvironmentID
	}

	return recordData
}

// Helper function to convert repo to protobuf
func (s *Server) repoToProto(r *repo.Repo) *corev1.Repo {
	return &corev1.Repo{
		RepoId:          r.ID,
		RepoName:        r.Name,
		RepoDescription: r.Description,
		TenantId:        r.TenantID,
		WorkspaceId:     r.WorkspaceID,
		OwnerId:         r.OwnerID,
	}
}

// Helper function to convert environment to protobuf with counts
func (s *Server) environmentToProtoWithCounts(ctx context.Context, env *environment.Environment, tenantId, workspaceId string) (*corev1.Environment, error) {
	// Get environment service to calculate counts
	environmentService := environment.NewService(s.engine.db, s.engine.logger)

	instanceCount, err := environmentService.GetInstanceCount(ctx, tenantId, workspaceId, env.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get instance count: %w", err)
	}

	databaseCount, err := environmentService.GetDatabaseCount(ctx, tenantId, workspaceId, env.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get database count: %w", err)
	}

	return &corev1.Environment{
		EnvironmentId:           env.ID,
		EnvironmentName:         env.Name,
		EnvironmentDescription:  env.Description,
		EnvironmentIsProduction: env.Production,
		EnvironmentCriticality:  env.Criticality,
		EnvironmentPriority:     env.Priority,
		InstanceCount:           instanceCount,
		DatabaseCount:           databaseCount,
		Status:                  statusStringToProto(env.Status),
		OwnerId:                 env.OwnerID,
	}, nil
}

// Helper function to convert region to protobuf
func (s *Server) regionToProto(reg *region.Region) *corev1.Region {
	// Handle optional float64 pointers for coordinates
	var latitude, longitude float64
	if reg.Latitude != nil {
		latitude = *reg.Latitude
	}
	if reg.Longitude != nil {
		longitude = *reg.Longitude
	}

	return &corev1.Region{
		RegionId:          reg.ID,
		RegionName:        reg.Name,
		RegionDescription: reg.Description,
		RegionLocation:    reg.Location,
		RegionLatitude:    latitude,
		RegionLongitude:   longitude,
		RegionType:        reg.RegionType,
		NodeCount:         reg.NodeCount,
		InstanceCount:     reg.InstanceCount,
		DatabaseCount:     reg.DatabaseCount,
		Status:            statusStringToProto(reg.Status),
		GlobalRegion:      reg.GlobalRegion,
		Created:           reg.Created.Format("2006-01-02T15:04:05Z"),
		Updated:           reg.Updated.Format("2006-01-02T15:04:05Z"),
	}
}

// Helper function to convert status string to proto enum
func statusStringToProto(status string) commonv1.Status {
	switch status {
	case "STATUS_HEALTHY":
		return commonv1.Status_STATUS_HEALTHY
	case "STATUS_DEGRADED":
		return commonv1.Status_STATUS_DEGRADED
	case "STATUS_UNHEALTHY":
		return commonv1.Status_STATUS_UNHEALTHY
	case "STATUS_PENDING":
		return commonv1.Status_STATUS_PENDING
	case "STATUS_UNKNOWN":
		return commonv1.Status_STATUS_UNKNOWN
	case "STATUS_SUCCESS":
		return commonv1.Status_STATUS_SUCCESS
	case "STATUS_FAILURE":
		return commonv1.Status_STATUS_FAILURE
	case "STATUS_STARTING":
		return commonv1.Status_STATUS_STARTING
	case "STATUS_STOPPING":
		return commonv1.Status_STATUS_STOPPING
	case "STATUS_STOPPED":
		return commonv1.Status_STATUS_STOPPED
	case "STATUS_STARTED":
		return commonv1.Status_STATUS_STARTED
	case "STATUS_CREATED":
		return commonv1.Status_STATUS_CREATED
	case "STATUS_DELETED":
		return commonv1.Status_STATUS_DELETED
	case "STATUS_UPDATED":
		return commonv1.Status_STATUS_UPDATED
	case "STATUS_CONNECTED":
		return commonv1.Status_STATUS_CONNECTED
	case "STATUS_DISCONNECTED":
		return commonv1.Status_STATUS_DISCONNECTED
	case "STATUS_CONNECTING":
		return commonv1.Status_STATUS_CONNECTING
	case "STATUS_DISCONNECTING":
		return commonv1.Status_STATUS_DISCONNECTING
	case "STATUS_RECONNECTING":
		return commonv1.Status_STATUS_RECONNECTING
	case "STATUS_ERROR":
		return commonv1.Status_STATUS_ERROR
	case "STATUS_WARNING":
		return commonv1.Status_STATUS_WARNING
	case "STATUS_INFO":
		return commonv1.Status_STATUS_INFO
	case "STATUS_DEBUG":
		return commonv1.Status_STATUS_DEBUG
	case "STATUS_TRACE":
		return commonv1.Status_STATUS_TRACE
	case "STATUS_EMPTY":
		return commonv1.Status_STATUS_EMPTY
	case "STATUS_JOINING":
		return commonv1.Status_STATUS_JOINING
	case "STATUS_LEAVING":
		return commonv1.Status_STATUS_LEAVING
	case "STATUS_SEEDING":
		return commonv1.Status_STATUS_SEEDING
	case "STATUS_ORPHANED":
		return commonv1.Status_STATUS_ORPHANED
	case "STATUS_SENT":
		return commonv1.Status_STATUS_SENT
	case "STATUS_CANCELLED":
		return commonv1.Status_STATUS_CANCELLED
	case "STATUS_PROCESSING":
		return commonv1.Status_STATUS_PROCESSING
	case "STATUS_DONE":
		return commonv1.Status_STATUS_DONE
	case "STATUS_RECEIVED":
		return commonv1.Status_STATUS_RECEIVED
	case "STATUS_ACTIVE":
		return commonv1.Status_STATUS_ACTIVE
	case "STATUS_CLEAN":
		return commonv1.Status_STATUS_CLEAN
	case "STATUS_INCONSISTENT":
		return commonv1.Status_STATUS_INCONSISTENT
	default:
		return commonv1.Status_STATUS_UNKNOWN
	}
}

// Helper function to convert mapping to protobuf
func (s *Server) mappingToProto(m *mapping.Mapping) (*corev1.Mapping, error) {
	return s.mappingToProtoWithContext(context.Background(), m)
}

func (s *Server) mappingToProtoWithContext(ctx context.Context, m *mapping.Mapping) (*corev1.Mapping, error) {
	var policyId string
	if len(m.PolicyIDs) > 0 {
		policyId = m.PolicyIDs[0] // Use first policy ID for protobuf
	}

	var validatedAt string
	if m.ValidatedAt != nil {
		validatedAt = m.ValidatedAt.Format(time.RFC3339)
	}

	// Marshal mapping_object to JSON string
	var mappingObjectJSON string
	if len(m.MappingObject) > 0 {
		bytes, err := json.Marshal(m.MappingObject)
		if err != nil {
			s.engine.logger.Warnf("Failed to marshal mapping_object: %v", err)
			mappingObjectJSON = "{}"
		} else {
			mappingObjectJSON = string(bytes)
		}
	} else {
		mappingObjectJSON = "{}"
	}

	// Get mapping service to fetch relationships and MCP assignments
	mappingService := mapping.NewService(s.engine.db, s.engine.logger)

	// Fetch relationship names
	relationshipNames, err := mappingService.GetRelationshipNamesByMappingID(ctx, m.ID)
	if err != nil {
		s.engine.logger.Warnf("Failed to get relationship names for mapping %s: %v", m.ID, err)
		relationshipNames = []string{}
	}

	// Fetch relationship infos (names and statuses)
	relationshipInfos, err := mappingService.GetRelationshipInfosByMappingID(ctx, m.ID)
	if err != nil {
		s.engine.logger.Warnf("Failed to get relationship infos for mapping %s: %v", m.ID, err)
		relationshipInfos = []mapping.RelationshipInfo{}
	}

	// Convert relationship infos to protobuf
	protoRelationshipInfos := make([]*corev1.RelationshipInfo, len(relationshipInfos))
	for i, info := range relationshipInfos {
		protoRelationshipInfos[i] = &corev1.RelationshipInfo{
			RelationshipName: info.Name,
			Status:           statusStringToProto(info.Status),
		}
	}

	// Fetch MCP resource names
	mcpResourceNames, err := mappingService.GetMCPResourceNamesByMappingID(ctx, m.ID)
	if err != nil {
		s.engine.logger.Warnf("Failed to get MCP resource names for mapping %s: %v", m.ID, err)
		mcpResourceNames = []string{}
	}

	// Fetch MCP tool names
	mcpToolNames, err := mappingService.GetMCPToolNamesByMappingID(ctx, m.ID)
	if err != nil {
		s.engine.logger.Warnf("Failed to get MCP tool names for mapping %s: %v", m.ID, err)
		mcpToolNames = []string{}
	}

	// Convert filters to protobuf
	protoFilters := make([]*corev1.MappingFilter, len(m.Filters))
	for i, filter := range m.Filters {
		// Convert filter expression to JSON string
		expressionJSON := "{}"
		if len(filter.FilterExpression) > 0 {
			bytes, err := json.Marshal(filter.FilterExpression)
			if err != nil {
				s.engine.logger.Warnf("Failed to marshal filter expression: %v", err)
			} else {
				expressionJSON = string(bytes)
			}
		}

		protoFilters[i] = &corev1.MappingFilter{
			FilterId:         filter.FilterID,
			MappingId:        filter.MappingID,
			FilterType:       filter.FilterType,
			FilterExpression: expressionJSON,
			FilterOrder:      int32(filter.FilterOrder),
			FilterOperator:   filter.FilterOperator,
		}
	}

	// Set container IDs if present
	sourceContainerID := ""
	if m.SourceContainerID != nil {
		sourceContainerID = *m.SourceContainerID
	}
	targetContainerID := ""
	if m.TargetContainerID != nil {
		targetContainerID = *m.TargetContainerID
	}

	return &corev1.Mapping{
		TenantId:                 m.TenantID,
		WorkspaceId:              m.WorkspaceID,
		MappingId:                m.ID,
		MappingName:              m.Name,
		MappingDescription:       m.Description,
		MappingType:              m.MappingType,
		MappingSourceType:        m.SourceType,
		MappingTargetType:        m.TargetType,
		MappingSourceIdentifier:  m.SourceIdentifier,
		MappingTargetIdentifier:  m.TargetIdentifier,
		MappingSourceContainerId: sourceContainerID,
		MappingTargetContainerId: targetContainerID,
		MappingObject:            mappingObjectJSON,
		PolicyId:                 policyId,
		OwnerId:                  m.OwnerID,
		MappingRuleCount:         m.MappingRuleCount,
		Validated:                m.Validated,
		ValidatedAt:              validatedAt,
		ValidationErrors:         m.ValidationErrors,
		ValidationWarnings:       m.ValidationWarnings,
		RelationshipNames:        relationshipNames,
		RelationshipInfos:        protoRelationshipInfos,
		McpResourceNames:         mcpResourceNames,
		McpToolNames:             mcpToolNames,
		Filters:                  protoFilters,
	}, nil
}

// Helper function to convert mapping rule to protobuf
func (s *Server) mappingRuleToProto(m *mapping.Rule) (*corev1.MappingRule, error) {
	// Extract values from metadata (backward compatibility)
	var sourceURI, targetURI, transformationID, transformationName string
	var transformationOptions map[string]interface{}

	if m.Metadata != nil {
		if v, ok := m.Metadata["source_resource_uri"].(string); ok {
			sourceURI = v
		}
		if v, ok := m.Metadata["target_resource_uri"].(string); ok {
			targetURI = v
		}
		if v, ok := m.Metadata["transformation_name"].(string); ok {
			transformationName = v
		}
		if v, ok := m.Metadata["transformation_options"].(map[string]interface{}); ok {
			transformationOptions = v
		}
	}

	// Convert transformation options to JSON string
	transformationOptionsJSON := "{}"
	if len(transformationOptions) > 0 {
		if jsonBytes, err := json.Marshal(transformationOptions); err == nil {
			transformationOptionsJSON = string(jsonBytes)
		} else {
			// Log error but continue - transformation options are optional
			s.engine.logger.Warnf("Failed to convert transformation options to JSON: %v", err)
		}
	}

	// Convert metadata map to JSON string
	metadataJSON := "{}"
	if len(m.Metadata) > 0 {
		if jsonBytes, err := json.Marshal(m.Metadata); err == nil {
			metadataJSON = string(jsonBytes)
		} else {
			// Log error but continue - metadata is optional
			s.engine.logger.Warnf("Failed to convert metadata to JSON: %v", err)
		}
	}

	return &corev1.MappingRule{
		TenantId:                         m.TenantID,
		WorkspaceId:                      m.WorkspaceID,
		MappingRuleId:                    m.ID,
		MappingRuleName:                  m.Name,
		MappingRuleDescription:           m.Description,
		MappingRuleSource:                sourceURI,
		MappingRuleTarget:                targetURI,
		MappingRuleTransformationId:      transformationID,
		MappingRuleTransformationName:    transformationName,
		MappingRuleTransformationOptions: transformationOptionsJSON,
		MappingRuleMetadata:              metadataJSON,
		OwnerId:                          m.OwnerID,
		MappingCount:                     m.MappingCount,
	}, nil
}

// mappingRuleToProtoWithItems converts a mapping rule to protobuf format and includes full item details in metadata
func (s *Server) mappingRuleToProtoWithItems(m *mapping.Rule) (*corev1.MappingRule, error) {
	// Start with base conversion
	protoRule, err := s.mappingRuleToProto(m)
	if err != nil {
		return nil, err
	}

	// Parse the existing metadata
	metadata := make(map[string]interface{})
	if protoRule.MappingRuleMetadata != "" && protoRule.MappingRuleMetadata != "{}" {
		if err := json.Unmarshal([]byte(protoRule.MappingRuleMetadata), &metadata); err != nil {
			s.engine.logger.Warnf("Failed to parse existing metadata: %v", err)
		}
	}

	// Add full source item details if available
	if len(m.SourceItems) > 0 {
		sourceItemsData := make([]map[string]interface{}, len(m.SourceItems))
		for i, item := range m.SourceItems {
			sourceItemsData[i] = map[string]interface{}{
				"item_id":                   item.ItemID,
				"container_id":              item.ContainerID,
				"resource_uri":              item.ResourceURI,
				"item_type":                 item.ItemType,
				"item_name":                 item.ItemName,
				"item_display_name":         item.ItemDisplayName,
				"item_path":                 item.ItemPath,
				"data_type":                 item.DataType,
				"unified_data_type":         item.UnifiedDataType,
				"is_nullable":               item.IsNullable,
				"is_primary_key":            item.IsPrimaryKey,
				"is_unique":                 item.IsUnique,
				"is_indexed":                item.IsIndexed,
				"is_required":               item.IsRequired,
				"is_array":                  item.IsArray,
				"array_dimensions":          item.ArrayDimensions,
				"default_value":             item.DefaultValue,
				"max_length":                item.MaxLength,
				"precision":                 item.Precision,
				"scale":                     item.Scale,
				"description":               item.Description,
				"is_privileged":             item.IsPrivileged,
				"privileged_classification": item.PrivilegedClassification,
				"detection_confidence":      item.DetectionConfidence,
				"detection_method":          item.DetectionMethod,
			}
		}
		metadata["source_items"] = sourceItemsData
	}

	// Add full target item details if available
	if len(m.TargetItems) > 0 {
		targetItemsData := make([]map[string]interface{}, len(m.TargetItems))
		for i, item := range m.TargetItems {
			targetItemsData[i] = map[string]interface{}{
				"item_id":                   item.ItemID,
				"container_id":              item.ContainerID,
				"resource_uri":              item.ResourceURI,
				"item_type":                 item.ItemType,
				"item_name":                 item.ItemName,
				"item_display_name":         item.ItemDisplayName,
				"item_path":                 item.ItemPath,
				"data_type":                 item.DataType,
				"unified_data_type":         item.UnifiedDataType,
				"is_nullable":               item.IsNullable,
				"is_primary_key":            item.IsPrimaryKey,
				"is_unique":                 item.IsUnique,
				"is_indexed":                item.IsIndexed,
				"is_required":               item.IsRequired,
				"is_array":                  item.IsArray,
				"array_dimensions":          item.ArrayDimensions,
				"default_value":             item.DefaultValue,
				"max_length":                item.MaxLength,
				"precision":                 item.Precision,
				"scale":                     item.Scale,
				"description":               item.Description,
				"is_privileged":             item.IsPrivileged,
				"privileged_classification": item.PrivilegedClassification,
				"detection_confidence":      item.DetectionConfidence,
				"detection_method":          item.DetectionMethod,
			}
		}
		metadata["target_items"] = targetItemsData
	}

	// Re-serialize metadata with item details
	metadataJSON := "{}"
	if len(metadata) > 0 {
		if jsonBytes, err := json.Marshal(metadata); err == nil {
			metadataJSON = string(jsonBytes)
		} else {
			s.engine.logger.Warnf("Failed to convert metadata with items to JSON: %v", err)
			return protoRule, nil // Return without item details rather than failing
		}
	}

	protoRule.MappingRuleMetadata = metadataJSON
	return protoRule, nil
}

// Helper function to convert relationship to protobuf
func (s *Server) relationshipToProto(r *relationship.Relationship) *corev1.Relationship {
	var policyId string
	if len(r.PolicyIDs) > 0 {
		policyId = r.PolicyIDs[0] // Use first policy ID for protobuf
	}

	// Fetch mapping name
	mappingName := ""
	if r.MappingID != "" {
		mappingService := mapping.NewService(s.engine.db, s.engine.logger)
		if m, err := mappingService.GetByID(context.Background(), r.MappingID); err == nil {
			mappingName = m.Name
		} else {
			s.engine.logger.Warnf("Failed to fetch mapping name for ID %s: %v", r.MappingID, err)
		}
	}

	// Fetch source database details
	sourceDatabaseName := ""
	sourceDatabaseType := ""
	if r.SourceDatabaseID != "" {
		databaseService := database.NewService(s.engine.db, s.engine.logger)
		if db, err := databaseService.GetByID(context.Background(), r.SourceDatabaseID); err == nil {
			sourceDatabaseName = db.Name
			sourceDatabaseType = db.Type
		} else {
			s.engine.logger.Warnf("Failed to fetch source database details for ID %s: %v", r.SourceDatabaseID, err)
		}
	}

	// Fetch target database details
	targetDatabaseName := ""
	targetDatabaseType := ""
	if r.TargetDatabaseID != "" {
		databaseService := database.NewService(s.engine.db, s.engine.logger)
		if db, err := databaseService.GetByID(context.Background(), r.TargetDatabaseID); err == nil {
			targetDatabaseName = db.Name
			targetDatabaseType = db.Type
		} else {
			s.engine.logger.Warnf("Failed to fetch target database details for ID %s: %v", r.TargetDatabaseID, err)
		}
	}

	return &corev1.Relationship{
		TenantId:                       r.TenantID,
		WorkspaceId:                    r.WorkspaceID,
		RelationshipId:                 r.ID,
		RelationshipName:               r.Name,
		RelationshipDescription:        r.Description,
		RelationshipType:               r.Type,
		RelationshipSourceDatabaseId:   r.SourceDatabaseID,
		RelationshipSourceTableName:    r.SourceTableName,
		RelationshipTargetDatabaseId:   r.TargetDatabaseID,
		RelationshipTargetTableName:    r.TargetTableName,
		MappingId:                      r.MappingID,
		PolicyId:                       policyId,
		StatusMessage:                  r.StatusMessage,
		Status:                         statusStringToProto(r.Status),
		OwnerId:                        r.OwnerID,
		MappingName:                    mappingName,
		RelationshipSourceDatabaseName: sourceDatabaseName,
		RelationshipTargetDatabaseName: targetDatabaseName,
		RelationshipSourceDatabaseType: sourceDatabaseType,
		RelationshipTargetDatabaseType: targetDatabaseType,
	}
}

// Helper function to convert transformation to protobuf
func (s *Server) transformationToProto(t *transformation.Transformation) *corev1.Transformation {
	return &corev1.Transformation{
		TenantId:                  t.TenantID,
		TransformationId:          t.ID,
		TransformationName:        t.Name,
		TransformationDescription: t.Description,
		TransformationType:        t.Type,
		TransformationVersion:     t.Version,
		TransformationFunction:    t.Function,
		OwnerId:                   t.OwnerID,
	}
}

// userToProto converts a user service model to protobuf
func (s *Server) userToProto(u *user.User) *corev1.User {
	return &corev1.User{
		TenantId:     u.TenantID,
		UserId:       u.ID,
		UserName:     u.Name,
		UserEmail:    u.Email,
		UserPassword: "", // Don't expose password hash in response
		UserEnabled:  u.Enabled,
	}
}

// tenantToProto converts a tenant service model to protobuf
func (s *Server) tenantToProto(t *tenant.Tenant) *corev1.Tenant {
	return &corev1.Tenant{
		TenantId:          t.ID,
		TenantName:        t.Name,
		TenantDescription: t.Description,
		TenantUrl:         t.URL,
	}
}

// Helper function to convert workspace to protobuf with counts
func (s *Server) workspaceToProtoWithCounts(ctx context.Context, ws *workspace.Workspace, tenantId string) (*corev1.Workspace, error) {
	// Calculate counts for related entities
	instanceCount, err := s.getInstanceCount(ctx, tenantId, ws.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get instance count: %w", err)
	}

	databaseCount, err := s.getDatabaseCount(ctx, tenantId, ws.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get database count: %w", err)
	}

	repoCount, err := s.getRepoCount(ctx, tenantId, ws.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get repo count: %w", err)
	}

	mappingCount, err := s.getMappingCount(ctx, tenantId, ws.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get mapping count: %w", err)
	}

	relationshipCount, err := s.getRelationshipCount(ctx, tenantId, ws.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get relationship count: %w", err)
	}

	return &corev1.Workspace{
		WorkspaceId:          ws.ID,
		WorkspaceName:        ws.Name,
		WorkspaceDescription: ws.Description,
		InstanceCount:        instanceCount,
		DatabaseCount:        databaseCount,
		RepoCount:            repoCount,
		MappingCount:         mappingCount,
		RelationshipCount:    relationshipCount,
		OwnerId:              ws.OwnerID,
	}, nil
}

// containerToProto converts a SchemaContainer to protobuf
func (s *Server) containerToProto(container *database.SchemaContainer) *corev1.DatabaseResourceContainer {
	// Convert container metadata to JSON string
	containerMetadataJSON := "{}"
	if len(container.ContainerMetadata) > 0 {
		if jsonBytes, err := json.Marshal(container.ContainerMetadata); err == nil {
			containerMetadataJSON = string(jsonBytes)
		}
	}

	// Convert enriched metadata to JSON string
	enrichedMetadataJSON := "{}"
	if len(container.EnrichedMetadata) > 0 {
		if jsonBytes, err := json.Marshal(container.EnrichedMetadata); err == nil {
			enrichedMetadataJSON = string(jsonBytes)
		}
	}

	// Convert items
	protoItems := make([]*corev1.DatabaseResourceItem, len(container.Items))
	for i, item := range container.Items {
		protoItems[i] = s.itemToProto(&item)
	}

	classification := ""
	if container.ContainerClassification != nil {
		classification = *container.ContainerClassification
	}

	confidence := 0.0
	if container.ContainerClassificationConfidence != nil {
		confidence = *container.ContainerClassificationConfidence
	}

	dbType := ""
	if container.DatabaseType != nil {
		dbType = *container.DatabaseType
	}

	vendor := ""
	if container.Vendor != nil {
		vendor = *container.Vendor
	}

	return &corev1.DatabaseResourceContainer{
		ObjectType:                        container.ObjectType,
		ObjectName:                        container.ObjectName,
		ContainerClassification:           &classification,
		ContainerClassificationConfidence: &confidence,
		ContainerClassificationSource:     container.ContainerClassificationSource,
		ContainerMetadataJson:             containerMetadataJSON,
		EnrichedMetadataJson:              enrichedMetadataJSON,
		DatabaseType:                      &dbType,
		Vendor:                            &vendor,
		ItemCount:                         int32(container.ItemCount),
		Status:                            container.Status,
		Items:                             protoItems,
	}
}

// itemToProto converts a SchemaItem to protobuf
func (s *Server) itemToProto(item *database.SchemaItem) *corev1.DatabaseResourceItem {
	// Convert constraints to JSON string
	constraintsJSON := "[]"
	if len(item.Constraints) > 0 {
		if jsonBytes, err := json.Marshal(item.Constraints); err == nil {
			constraintsJSON = string(jsonBytes)
		}
	}

	unifiedType := ""
	if item.UnifiedDataType != nil {
		unifiedType = *item.UnifiedDataType
	}

	defaultVal := ""
	if item.DefaultValue != nil {
		defaultVal = *item.DefaultValue
	}

	privClass := ""
	if item.PrivilegedClassification != nil {
		privClass = *item.PrivilegedClassification
	}

	confidence := 0.0
	if item.DetectionConfidence != nil {
		confidence = *item.DetectionConfidence
	}

	method := ""
	if item.DetectionMethod != nil {
		method = *item.DetectionMethod
	}

	comment := ""
	if item.ItemComment != nil {
		comment = *item.ItemComment
	}

	maxLen := int32(0)
	if item.MaxLength != nil {
		maxLen = int32(*item.MaxLength)
	}

	prec := int32(0)
	if item.Precision != nil {
		prec = int32(*item.Precision)
	}

	scaleVal := int32(0)
	if item.Scale != nil {
		scaleVal = int32(*item.Scale)
	}

	return &corev1.DatabaseResourceItem{
		ItemName:                 item.ItemName,
		ItemDisplayName:          item.ItemDisplayName,
		DataType:                 item.DataType,
		UnifiedDataType:          &unifiedType,
		IsNullable:               item.IsNullable,
		IsPrimaryKey:             item.IsPrimaryKey,
		IsUnique:                 item.IsUnique,
		IsIndexed:                item.IsIndexed,
		IsRequired:               item.IsRequired,
		IsArray:                  item.IsArray,
		DefaultValue:             &defaultVal,
		ConstraintsJson:          constraintsJSON,
		IsPrivileged:             item.IsPrivileged,
		PrivilegedClassification: &privClass,
		DetectionConfidence:      &confidence,
		DetectionMethod:          &method,
		OrdinalPosition:          item.OrdinalPosition,
		MaxLength:                &maxLen,
		Precision:                &prec,
		Scale:                    &scaleVal,
		ItemComment:              &comment,
	}
}
