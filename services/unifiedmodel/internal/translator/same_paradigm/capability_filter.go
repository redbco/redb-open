package same_paradigm

import (
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// CapabilityFilter filters objects based on database capabilities
type CapabilityFilter struct {
	capabilityMatrix map[dbcapabilities.DatabaseType]DatabaseCapabilities
}

// DatabaseCapabilities defines what object types a database supports
type DatabaseCapabilities struct {
	SupportedObjects map[unifiedmodel.ObjectType]ObjectSupport `json:"supported_objects"`
}

// ObjectSupport defines the level of support for an object type
type ObjectSupport struct {
	Supported    bool     `json:"supported"`
	Limitations  []string `json:"limitations,omitempty"`
	Alternatives []string `json:"alternatives,omitempty"`
	Notes        string   `json:"notes,omitempty"`
}

// NewCapabilityFilter creates a new capability filter
func NewCapabilityFilter() *CapabilityFilter {
	filter := &CapabilityFilter{
		capabilityMatrix: make(map[dbcapabilities.DatabaseType]DatabaseCapabilities),
	}
	filter.initializeCapabilityMatrix()
	return filter
}

// IsObjectTypeSupported checks if an object type is supported by the target database
func (cf *CapabilityFilter) IsObjectTypeSupported(targetDB dbcapabilities.DatabaseType, objectType unifiedmodel.ObjectType) bool {
	capabilities, exists := cf.capabilityMatrix[targetDB]
	if !exists {
		// If not defined, assume supported (conservative approach)
		return true
	}

	support, exists := capabilities.SupportedObjects[objectType]
	if !exists {
		// If not defined, assume supported
		return true
	}

	return support.Supported
}

// GetObjectSupport returns detailed support information for an object type
func (cf *CapabilityFilter) GetObjectSupport(targetDB dbcapabilities.DatabaseType, objectType unifiedmodel.ObjectType) ObjectSupport {
	capabilities, exists := cf.capabilityMatrix[targetDB]
	if !exists {
		return ObjectSupport{Supported: true}
	}

	support, exists := capabilities.SupportedObjects[objectType]
	if !exists {
		return ObjectSupport{Supported: true}
	}

	return support
}

// GetUnsupportedObjects returns a list of object types not supported by the target database
func (cf *CapabilityFilter) GetUnsupportedObjects(targetDB dbcapabilities.DatabaseType) []unifiedmodel.ObjectType {
	var unsupported []unifiedmodel.ObjectType

	capabilities, exists := cf.capabilityMatrix[targetDB]
	if !exists {
		return unsupported
	}

	for objectType, support := range capabilities.SupportedObjects {
		if !support.Supported {
			unsupported = append(unsupported, objectType)
		}
	}

	return unsupported
}

// GetAlternatives returns alternative object types for unsupported objects
func (cf *CapabilityFilter) GetAlternatives(targetDB dbcapabilities.DatabaseType, objectType unifiedmodel.ObjectType) []string {
	support := cf.GetObjectSupport(targetDB, objectType)
	return support.Alternatives
}

// GetLimitations returns limitations for supported objects
func (cf *CapabilityFilter) GetLimitations(targetDB dbcapabilities.DatabaseType, objectType unifiedmodel.ObjectType) []string {
	support := cf.GetObjectSupport(targetDB, objectType)
	return support.Limitations
}

// initializeCapabilityMatrix sets up the capability matrix for all supported databases
func (cf *CapabilityFilter) initializeCapabilityMatrix() {
	// PostgreSQL capabilities
	cf.capabilityMatrix[dbcapabilities.PostgreSQL] = DatabaseCapabilities{
		SupportedObjects: map[unifiedmodel.ObjectType]ObjectSupport{
			unifiedmodel.ObjectTypeTable:            {Supported: true},
			unifiedmodel.ObjectTypeCollection:       {Supported: false, Alternatives: []string{"table", "jsonb_table"}},
			unifiedmodel.ObjectTypeNode:             {Supported: false, Alternatives: []string{"table"}},
			unifiedmodel.ObjectTypeView:             {Supported: true},
			unifiedmodel.ObjectTypeMaterializedView: {Supported: true},
			unifiedmodel.ObjectTypeFunction:         {Supported: true},
			unifiedmodel.ObjectTypeProcedure:        {Supported: true},
			unifiedmodel.ObjectTypeTrigger:          {Supported: true},
			unifiedmodel.ObjectTypeIndex:            {Supported: true},
			unifiedmodel.ObjectTypeConstraint:       {Supported: true},
			unifiedmodel.ObjectTypeSequence:         {Supported: true},
			unifiedmodel.ObjectTypeType:             {Supported: true},
		},
	}

	// MySQL capabilities
	cf.capabilityMatrix[dbcapabilities.MySQL] = DatabaseCapabilities{
		SupportedObjects: map[unifiedmodel.ObjectType]ObjectSupport{
			unifiedmodel.ObjectTypeTable:            {Supported: true},
			unifiedmodel.ObjectTypeCollection:       {Supported: false, Alternatives: []string{"table", "json_table"}},
			unifiedmodel.ObjectTypeNode:             {Supported: false, Alternatives: []string{"table"}},
			unifiedmodel.ObjectTypeView:             {Supported: true},
			unifiedmodel.ObjectTypeMaterializedView: {Supported: false, Alternatives: []string{"table_with_triggers"}},
			unifiedmodel.ObjectTypeFunction:         {Supported: true, Limitations: []string{"limited_language_support"}},
			unifiedmodel.ObjectTypeProcedure:        {Supported: true},
			unifiedmodel.ObjectTypeTrigger:          {Supported: true},
			unifiedmodel.ObjectTypeIndex:            {Supported: true},
			unifiedmodel.ObjectTypeConstraint:       {Supported: true, Limitations: []string{"no_check_constraints_before_8.0.16"}},
			unifiedmodel.ObjectTypeSequence:         {Supported: false, Alternatives: []string{"auto_increment"}},
			unifiedmodel.ObjectTypeType:             {Supported: false, Alternatives: []string{"enum", "set"}},
		},
	}

	// MongoDB capabilities
	cf.capabilityMatrix[dbcapabilities.MongoDB] = DatabaseCapabilities{
		SupportedObjects: map[unifiedmodel.ObjectType]ObjectSupport{
			unifiedmodel.ObjectTypeTable:            {Supported: false, Alternatives: []string{"collection"}},
			unifiedmodel.ObjectTypeCollection:       {Supported: true},
			unifiedmodel.ObjectTypeNode:             {Supported: false, Alternatives: []string{"document"}},
			unifiedmodel.ObjectTypeView:             {Supported: true, Notes: "MongoDB views are read-only aggregation pipelines"},
			unifiedmodel.ObjectTypeMaterializedView: {Supported: false, Alternatives: []string{"collection_with_aggregation"}},
			unifiedmodel.ObjectTypeFunction:         {Supported: false, Alternatives: []string{"javascript_functions", "aggregation_pipeline"}},
			unifiedmodel.ObjectTypeProcedure:        {Supported: false, Alternatives: []string{"javascript_functions"}},
			unifiedmodel.ObjectTypeTrigger:          {Supported: false, Alternatives: []string{"change_streams", "database_triggers"}},
			unifiedmodel.ObjectTypeIndex:            {Supported: true},
			unifiedmodel.ObjectTypeConstraint:       {Supported: false, Alternatives: []string{"schema_validation"}},
			unifiedmodel.ObjectTypeSequence:         {Supported: false, Alternatives: []string{"counter_collection", "objectid"}},
			unifiedmodel.ObjectTypeType:             {Supported: false, Alternatives: []string{"schema_validation"}},
		},
	}

	// Redis capabilities
	cf.capabilityMatrix[dbcapabilities.Redis] = DatabaseCapabilities{
		SupportedObjects: map[unifiedmodel.ObjectType]ObjectSupport{
			unifiedmodel.ObjectTypeTable:            {Supported: false, Alternatives: []string{"hash", "sorted_set"}},
			unifiedmodel.ObjectTypeCollection:       {Supported: false, Alternatives: []string{"list", "set"}},
			unifiedmodel.ObjectTypeNode:             {Supported: false, Alternatives: []string{"hash"}},
			unifiedmodel.ObjectTypeView:             {Supported: false},
			unifiedmodel.ObjectTypeMaterializedView: {Supported: false},
			unifiedmodel.ObjectTypeFunction:         {Supported: false, Alternatives: []string{"lua_scripts"}},
			unifiedmodel.ObjectTypeProcedure:        {Supported: false, Alternatives: []string{"lua_scripts"}},
			unifiedmodel.ObjectTypeTrigger:          {Supported: false, Alternatives: []string{"keyspace_notifications"}},
			unifiedmodel.ObjectTypeIndex:            {Supported: false, Notes: "Redis uses key patterns for indexing"},
			unifiedmodel.ObjectTypeConstraint:       {Supported: false},
			unifiedmodel.ObjectTypeSequence:         {Supported: false, Alternatives: []string{"incr_command"}},
			unifiedmodel.ObjectTypeType:             {Supported: false},
		},
	}

	// Neo4j capabilities
	cf.capabilityMatrix[dbcapabilities.Neo4j] = DatabaseCapabilities{
		SupportedObjects: map[unifiedmodel.ObjectType]ObjectSupport{
			unifiedmodel.ObjectTypeTable:            {Supported: false, Alternatives: []string{"node_label"}},
			unifiedmodel.ObjectTypeCollection:       {Supported: false, Alternatives: []string{"node_label"}},
			unifiedmodel.ObjectTypeNode:             {Supported: true},
			unifiedmodel.ObjectTypeView:             {Supported: false, Alternatives: []string{"cypher_projection"}},
			unifiedmodel.ObjectTypeMaterializedView: {Supported: false},
			unifiedmodel.ObjectTypeFunction:         {Supported: true, Notes: "User-defined functions and procedures"},
			unifiedmodel.ObjectTypeProcedure:        {Supported: true},
			unifiedmodel.ObjectTypeTrigger:          {Supported: false, Alternatives: []string{"apoc_triggers"}},
			unifiedmodel.ObjectTypeIndex:            {Supported: true, Notes: "Node and relationship indexes"},
			unifiedmodel.ObjectTypeConstraint:       {Supported: true, Notes: "Uniqueness and existence constraints"},
			unifiedmodel.ObjectTypeSequence:         {Supported: false, Alternatives: []string{"id_function"}},
			unifiedmodel.ObjectTypeType:             {Supported: false},
		},
	}

	// Elasticsearch capabilities
	cf.capabilityMatrix[dbcapabilities.Elasticsearch] = DatabaseCapabilities{
		SupportedObjects: map[unifiedmodel.ObjectType]ObjectSupport{
			unifiedmodel.ObjectTypeTable:            {Supported: false, Alternatives: []string{"index"}},
			unifiedmodel.ObjectTypeCollection:       {Supported: false, Alternatives: []string{"index"}},
			unifiedmodel.ObjectTypeNode:             {Supported: false, Alternatives: []string{"document"}},
			unifiedmodel.ObjectTypeView:             {Supported: false, Alternatives: []string{"index_alias"}},
			unifiedmodel.ObjectTypeMaterializedView: {Supported: false},
			unifiedmodel.ObjectTypeFunction:         {Supported: false, Alternatives: []string{"painless_scripts"}},
			unifiedmodel.ObjectTypeProcedure:        {Supported: false, Alternatives: []string{"painless_scripts"}},
			unifiedmodel.ObjectTypeTrigger:          {Supported: false, Alternatives: []string{"ingest_pipelines"}},
			unifiedmodel.ObjectTypeIndex:            {Supported: true, Notes: "Elasticsearch indexes are the primary data structure"},
			unifiedmodel.ObjectTypeConstraint:       {Supported: false, Alternatives: []string{"mapping_validation"}},
			unifiedmodel.ObjectTypeSequence:         {Supported: false, Alternatives: []string{"auto_generated_ids"}},
			unifiedmodel.ObjectTypeType:             {Supported: false, Alternatives: []string{"mapping_types"}},
		},
	}

	// Cassandra capabilities
	cf.capabilityMatrix[dbcapabilities.Cassandra] = DatabaseCapabilities{
		SupportedObjects: map[unifiedmodel.ObjectType]ObjectSupport{
			unifiedmodel.ObjectTypeTable:            {Supported: true},
			unifiedmodel.ObjectTypeCollection:       {Supported: false, Alternatives: []string{"table_with_collections"}},
			unifiedmodel.ObjectTypeNode:             {Supported: false, Alternatives: []string{"table"}},
			unifiedmodel.ObjectTypeView:             {Supported: false, Alternatives: []string{"materialized_view"}},
			unifiedmodel.ObjectTypeMaterializedView: {Supported: true},
			unifiedmodel.ObjectTypeFunction:         {Supported: true, Limitations: []string{"user_defined_functions_only"}},
			unifiedmodel.ObjectTypeProcedure:        {Supported: false},
			unifiedmodel.ObjectTypeTrigger:          {Supported: false},
			unifiedmodel.ObjectTypeIndex:            {Supported: true, Limitations: []string{"secondary_indexes_only"}},
			unifiedmodel.ObjectTypeConstraint:       {Supported: false},
			unifiedmodel.ObjectTypeSequence:         {Supported: false, Alternatives: []string{"uuid", "timeuuid"}},
			unifiedmodel.ObjectTypeType:             {Supported: true, Notes: "User-defined types supported"},
		},
	}

	// DynamoDB capabilities
	cf.capabilityMatrix[dbcapabilities.DynamoDB] = DatabaseCapabilities{
		SupportedObjects: map[unifiedmodel.ObjectType]ObjectSupport{
			unifiedmodel.ObjectTypeTable:            {Supported: true, Notes: "NoSQL tables with key-value structure"},
			unifiedmodel.ObjectTypeCollection:       {Supported: false, Alternatives: []string{"table"}},
			unifiedmodel.ObjectTypeNode:             {Supported: false, Alternatives: []string{"item"}},
			unifiedmodel.ObjectTypeView:             {Supported: false, Alternatives: []string{"global_secondary_index"}},
			unifiedmodel.ObjectTypeMaterializedView: {Supported: false},
			unifiedmodel.ObjectTypeFunction:         {Supported: false, Alternatives: []string{"lambda_functions"}},
			unifiedmodel.ObjectTypeProcedure:        {Supported: false},
			unifiedmodel.ObjectTypeTrigger:          {Supported: false, Alternatives: []string{"dynamodb_streams"}},
			unifiedmodel.ObjectTypeIndex:            {Supported: true, Notes: "Global and local secondary indexes"},
			unifiedmodel.ObjectTypeConstraint:       {Supported: false},
			unifiedmodel.ObjectTypeSequence:         {Supported: false, Alternatives: []string{"auto_generated_keys"}},
			unifiedmodel.ObjectTypeType:             {Supported: false},
		},
	}

	// Add more databases as needed...
}
