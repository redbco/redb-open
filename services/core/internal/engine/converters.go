package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

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
	return &corev1.Mesh{
		MeshId:          m.ID,
		MeshName:        m.Name,
		MeshDescription: m.Description,
		AllowJoin:       m.AllowJoin,
		NodeCount:       m.NodeCount,
		Status:          statusStringToProto(m.Status),
	}
}

// nodeToProto converts a node service model to protobuf (legacy format)
func (s *Server) nodeToProto(n *mesh.Node) *corev1.Node {
	// Convert string node ID to uint64
	nodeID, _ := strconv.ParseUint(n.ID, 10, 64)

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
		NodeId:          strconv.FormatUint(nodeID, 10),
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
	}
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
	default:
		return commonv1.Status_STATUS_UNKNOWN
	}
}

// Helper function to convert mapping to protobuf
func (s *Server) mappingToProto(m *mapping.Mapping) (*corev1.Mapping, error) {
	var policyId string
	if len(m.PolicyIDs) > 0 {
		policyId = m.PolicyIDs[0] // Use first policy ID for protobuf
	}

	return &corev1.Mapping{
		TenantId:           m.TenantID,
		WorkspaceId:        m.WorkspaceID,
		MappingId:          m.ID,
		MappingName:        m.Name,
		MappingDescription: m.Description,
		MappingType:        m.MappingType,
		PolicyId:           policyId,
		OwnerId:            m.OwnerID,
		MappingRuleCount:   m.MappingRuleCount,
	}, nil
}

// Helper function to convert mapping rule to protobuf
func (s *Server) mappingRuleToProto(m *mapping.Rule) (*corev1.MappingRule, error) {
	// Convert transformation options map to JSON string
	transformationOptionsJSON := "{}"
	if len(m.TransformationOptions) > 0 {
		if jsonBytes, err := json.Marshal(m.TransformationOptions); err == nil {
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
		MappingRuleSource:                m.SourceIdentifier,
		MappingRuleTarget:                m.TargetIdentifier,
		MappingRuleTransformationId:      m.TransformationID,
		MappingRuleTransformationName:    m.TransformationName,
		MappingRuleTransformationOptions: transformationOptionsJSON,
		MappingRuleMetadata:              metadataJSON,
		OwnerId:                          m.OwnerID,
		MappingCount:                     m.MappingCount,
	}, nil
}

// Helper function to convert relationship to protobuf
func (s *Server) relationshipToProto(r *relationship.Relationship) *corev1.Relationship {
	var policyId string
	if len(r.PolicyIDs) > 0 {
		policyId = r.PolicyIDs[0] // Use first policy ID for protobuf
	}

	return &corev1.Relationship{
		TenantId:                     r.TenantID,
		WorkspaceId:                  r.WorkspaceID,
		RelationshipId:               r.ID,
		RelationshipName:             r.Name,
		RelationshipDescription:      r.Description,
		RelationshipType:             r.Type,
		RelationshipSourceDatabaseId: r.SourceDatabaseID,
		RelationshipSourceTableName:  r.SourceTableName,
		RelationshipTargetDatabaseId: r.TargetDatabaseID,
		RelationshipTargetTableName:  r.TargetTableName,
		MappingId:                    r.MappingID,
		PolicyId:                     policyId,
		StatusMessage:                r.StatusMessage,
		Status:                       statusStringToProto(r.Status),
		OwnerId:                      r.OwnerID,
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
