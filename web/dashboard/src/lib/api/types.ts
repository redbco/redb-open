/* eslint-disable @typescript-eslint/no-explicit-any */
// API Response Types matching the Client API service

export interface ApiError {
  error: string;
  message: string;
  status: string;
}

export interface ApiResponse<T> {
  data?: T;
  error?: string;
  message?: string;
  status?: string;
  success?: boolean;
}

// Resource Types
export type ResourceType = 'database' | 'table' | 'mcp-resource' | 'mcp-tool' | 'webhook' | 'stream';

export interface ResourceContainer {
  container_id: string;
  tenant_id: string;
  workspace_id: string;
  resource_uri: string;
  protocol: string;
  scope: string;
  object_type: string;
  object_name: string;
  database_id?: string;
  instance_id?: string;
  integration_id?: string;
  mcpserver_id?: string;
  connected_to_node_id?: number;
  owner_id: string;
  status: string;
  status_message?: string;
  last_seen?: string;
  online: boolean;
  container_metadata?: Record<string, any>;
  enriched_metadata?: Record<string, any>;
  database_type?: string;
  vendor?: string;
  item_count?: number;
  size_bytes?: number;
  created?: string;
  updated?: string;
}

export interface ResourceItem {
  item_id: string;
  container_id: string;
  tenant_id: string;
  workspace_id: string;
  resource_uri: string;
  protocol: string;
  scope: string;
  item_type: string;
  item_name: string;
  item_display_name?: string;
  item_path?: string[];
  data_type: string;
  unified_data_type?: string;
  is_nullable: boolean;
  is_primary_key: boolean;
  is_unique: boolean;
  is_indexed: boolean;
  is_required: boolean;
  is_array: boolean;
  array_dimensions?: number;
  default_value?: string;
  constraints?: Record<string, any>[];
  is_custom_type: boolean;
  custom_type_name?: string;
  custom_type_definition?: Record<string, any>;
  has_schema: boolean;
  schema_format?: string;
  schema_definition?: Record<string, any>;
  schema_version?: string;
  schema_evolution_version?: number;
  schema_validation_mode?: string;
  schema_mismatch_action?: string;
  allow_new_fields: boolean;
  allow_field_type_widening: boolean;
  allow_field_removal: boolean;
  schema_evolution_log?: Record<string, any>[];
  nested_items?: Record<string, any>[];
  max_length?: number;
  precision?: number;
  scale?: number;
  connected_to_node_id?: number;
  status: string;
  online: boolean;
  item_metadata?: Record<string, any>;
  enriched_metadata?: Record<string, any>;
  item_comment?: string;
  is_privileged: boolean;
  privileged_classification?: string;
  detection_confidence?: number;
  detection_method?: string;
  ordinal_position?: number;
  created?: string;
  updated?: string;
}

export interface ListResourceContainersResponse {
  containers: ResourceContainer[];
}

export interface ListResourceItemsResponse {
  items: ResourceItem[];
}

export interface ResourceSelection {
  type: ResourceType;
  resourceId: string;
  resourceName: string;
  databaseId?: string;
  databaseName?: string;
  tableName?: string;
  uri: string;
}

// DataProduct Types
export interface DataProduct {
  product_id: string;
  tenant_id: string;
  workspace_id: string;
  product_name: string;
  product_description: string;
  resource_items: ResourceItem[];
  metadata?: Record<string, any>;
  owner_id: string;
  status: string;
  created: string;
  updated: string;
}

export interface ResourceContainerFilter {
  protocol?: string;
  scope?: string;
  object_type?: string;
  database_type?: string;
  status?: string;
  online?: boolean;
  limit?: number;
  offset?: number;
}

export interface ResourceItemFilter {
  container_id?: string;
  item_type?: string;
  data_type?: string;
  unified_data_type?: string;
  is_privileged?: boolean;
  is_primary_key?: boolean;
  is_unique?: boolean;
  is_indexed?: boolean;
  online?: boolean;
  limit?: number;
  offset?: number;
}

export interface ListDataProductsResponse {
  dataproducts: DataProduct[];
}

export interface ShowDataProductResponse {
  dataproduct: DataProduct;
}

export interface CreateDataProductRequest {
  product_name: string;
  product_description: string;
  resource_item_ids: string[];
  metadata?: Record<string, any>;
}

export interface CreateDataProductResponse {
  success: boolean;
  message: string;
  dataproduct: DataProduct;
}

export interface ModifyDataProductRequest {
  product_description?: string;
  resource_item_ids?: string[];
  metadata?: Record<string, any>;
  status?: string;
}

export interface ModifyDataProductResponse {
  success: boolean;
  message: string;
  dataproduct: DataProduct;
}

export interface DeleteDataProductResponse {
  success: boolean;
  message: string;
}

export interface ShowResourceContainerResponse {
  container: ResourceContainer;
}

// Auth Types
export interface LoginRequest {
  username: string;
  password: string;
  expiry_time_hours?: number;
  session_name?: string;
  user_agent?: string;
  ip_address?: string;
}

export interface LoginResponse {
  message: string;
  success: boolean;
  access_token: string;
  profile: Profile;
  status: string;
}

export interface Profile {
  tenant_id: string;
  user_id: string;
  username: string;
  email: string;
  first_name?: string;
  last_name?: string;
  role?: string;
  workspace_ids?: string[];
}

// Database Types
export interface Database {
  tenant_id: string;
  workspace_id: string;
  environment_id?: string;
  connected_to_node_id?: string;
  instance_id: string;
  instance_name: string;
  database_id: string;
  database_name: string;
  database_description?: string;
  database_type: string;
  database_vendor: string;
  database_version?: string;
  database_username?: string;
  database_password?: string;
  database_db_name: string;
  database_enabled: boolean;
  policy_ids?: string[];
  owner_id?: string;
  database_status_message?: string;
  status: string;
  created?: string;
  updated?: string;
  database_schema?: string; // JSON string of schema data
  database_tables?: string; // JSON string of tables data
  instance_host?: string;
  instance_port?: number;
  instance_ssl_mode?: string;
  instance_ssl_cert?: string;
  instance_ssl_key?: string;
  instance_ssl_root_cert?: string;
  instance_ssl?: boolean;
  instance_status_message?: string;
  instance_status?: string;
}

export interface ListDatabasesResponse {
  databases: Database[];
}

export interface ShowDatabaseResponse {
  database: Database;
}

export interface ConnectDatabaseRequest {
  database_name: string;
  database_description?: string;
  database_type: string;
  database_vendor: string;
  host: string;
  port: number;
  username: string;
  password: string;
  db_name: string;
  node_id: string;
  enabled?: boolean;
  ssl?: boolean;
  ssl_mode?: string;
  ssl_cert?: string;
  ssl_key?: string;
  ssl_root_cert?: string;
  environment_id?: string;
  instance_id?: string;
  instance_name?: string;
  instance_description?: string;
}

export interface ConnectDatabaseStringRequest {
  connection_string: string;
  database_name: string;
  database_description?: string;
  node_id?: string;
  environment_id?: string;
  enabled?: boolean;
}

export interface ConnectDatabaseResponse {
  message: string;
  success: boolean;
  database: Database;
  status: string;
}

export interface ModifyDatabaseRequest {
  database_name?: string;
  database_description?: string;
  host?: string;
  port?: number;
  username?: string;
  password?: string;
  enabled?: boolean;
}

export interface DisconnectDatabaseRequest {
  delete_database_object?: boolean;
  delete_branch?: boolean;
  delete_repo?: boolean;
  disconnect_instance?: boolean;
}

export interface DatabaseDisconnectMetadata {
  database_name: string;
  instance_name: string;
  is_last_database_in_instance: boolean;
  total_databases_in_instance: number;
  has_attached_branch: boolean;
  attached_repo_name?: string;
  attached_branch_name?: string;
  is_only_branch_in_repo: boolean;
  total_branches_in_repo: number;
  has_other_databases_on_branch: boolean;
  can_delete_branch_only: boolean;
  can_delete_entire_repo: boolean;
  should_delete_repo: boolean;
  should_delete_branch: boolean;
}

export interface GetDatabaseDisconnectMetadataResponse {
  message: string;
  success: boolean;
  status: string;
  metadata: DatabaseDisconnectMetadata;
}

// ==================== Stream Types ====================

export interface Stream {
  tenant_id: string;
  workspace_id: string;
  stream_id: string;
  stream_name: string;
  stream_description?: string;
  stream_platform: string;
  stream_version?: string;
  region_id?: string;
  connection_config: Record<string, any>;
  credential_key?: string;
  metadata?: Record<string, any>;
  monitored_topics: string[];
  connected_to_node_id: number;
  owner_id?: string;
  status: string;
  created?: string;
  updated?: string;
}

export interface TopicInfo {
  name: string;
  partitions: number;
  replicas: number;
  config: Record<string, string>;
}

export interface TopicSchema {
  topic_name: string;
  schema: any;
  messages_sampled: number;
  confidence_score: number;
}

export interface ConnectStreamRequest {
  stream_name: string;
  stream_description?: string;
  stream_platform: string;
  region_name?: string;
  connection_config: Record<string, any>;
  monitored_topics: string[];
  node_id?: number;
}

export interface ConnectStreamResponse {
  stream: Stream;
}

export interface ListStreamsResponse {
  streams: Stream[];
}

export interface ShowStreamResponse {
  stream: Stream;
}

export interface ModifyStreamRequest {
  stream_description?: string;
  connection_config?: Record<string, any>;
  monitored_topics?: string[];
}

export interface ModifyStreamResponse {
  stream: Stream;
}

export interface ReconnectStreamResponse {
  stream: Stream;
}

export interface DisconnectStreamRequest {
  delete_stream?: boolean;
}

export interface DisconnectStreamResponse {
  // 204 No Content - empty response
}

export interface ListTopicsResponse {
  topics: TopicInfo[];
}

export interface GetTopicSchemaResponse {
  topic_name: string;
  schema: any;
  messages_sampled: number;
  confidence_score: number;
}

// Schema Column Structure
export interface SchemaColumn {
  name: string;
  isArray?: boolean;
  dataType: string;
  type?: string; // Alternative field name used by some responses
  data_type?: string; // Alternative field name
  isUnique?: boolean;
  is_unique?: boolean; // Alternative field name
  isNullable?: boolean;
  is_nullable?: boolean; // Alternative field name
  isGenerated?: boolean;
  isPrimaryKey?: boolean;
  is_primary_key?: boolean; // Alternative field name
  isIndexed?: boolean;
  is_indexed?: boolean; // Alternative field name
  columnDefault?: string;
  column_default?: string; // Alternative field name
  defaultValue?: string;
  default_value?: string; // Alternative field name
  isAutoIncrement?: boolean;
  is_auto_increment?: boolean; // Alternative field name
  varcharLength?: number;
  varchar_length?: number; // Alternative field name
  dataCategory?: string;
  data_category?: string; // Alternative field name
  // Updated privileged data fields based on new schema structure
  isPrivileged?: boolean;
  is_privileged?: boolean; // Alternative field name
  privilegedClassification?: string;
  privileged_classification?: string; // Alternative field name
  detectionConfidence?: number;
  detection_confidence?: number; // Alternative field name
  detectionMethod?: string;
  detection_method?: string; // Alternative field name
  // Legacy fields for backward compatibility
  isPrivilegedData?: boolean;
  is_privileged_data?: boolean;
  privilegedConfidence?: number;
  privileged_confidence?: number;
  privilegedDescription?: string;
  privileged_description?: string;
  classificationConfidence?: number;
  classification_confidence?: number; // Alternative field name
  constraints?: string[];
  ordinalPosition?: number;
  ordinal_position?: number; // Alternative field name
}

// New Schema Item Structure (from resource_items)
export interface SchemaItem {
  item_name: string;
  item_display_name?: string;
  data_type: string;
  unified_data_type?: string;
  is_nullable: boolean;
  is_primary_key: boolean;
  is_unique: boolean;
  is_indexed: boolean;
  is_required: boolean;
  is_array: boolean;
  default_value?: string;
  constraints?: Array<Record<string, any>>;
  is_privileged: boolean;
  privileged_classification?: string;
  detection_confidence?: number;
  detection_method?: string;
  ordinal_position: number;
  max_length?: number;
  precision?: number;
  scale?: number;
  item_comment?: string;
}

// Schema Table Classification Score
export interface ClassificationScore {
  score: number;
  reason?: string;
  category: string;
}

// Legacy Schema Table Structure (backward compatibility)
export interface SchemaTable {
  name: string;
  // New structure fields from containers
  object_type?: string;
  database_type?: string;
  container_classification?: string;
  container_classification_confidence?: number;
  container_classification_source?: string;
  item_count?: number;
  status?: string;
  // Legacy fields
  schema?: string;
  engine?: string;
  columns: SchemaColumn[];
  indexes?: any;
  tableType?: string;
  table_type?: string; // Alternative field name
  primaryKey?: any;
  primaryCategory?: string;
  primary_category?: string; // Alternative field name
  constraints?: any;
  classificationScores?: ClassificationScore[];
  classification_scores?: ClassificationScore[]; // Alternative field name
  classificationConfidence?: number;
  classification_confidence?: number; // Alternative field name
}

// New Schema Container Structure (from resource_containers)
export interface SchemaContainer {
  object_type: string;
  object_name: string;
  container_classification?: string;
  container_classification_confidence?: number;
  container_classification_source: string;
  container_metadata?: Record<string, any>;
  enriched_metadata?: Record<string, any>;
  database_type?: string;
  vendor?: string;
  item_count: number;
  status: string;
  items: SchemaItem[];
}

// Database Schema Structure
export interface DatabaseSchema {
  // New structure
  containers?: SchemaContainer[];
  // Legacy structure (backward compatibility)
  tables?: SchemaTable[];
  schemas?: any;
  triggers?: any;
  enumTypes?: any;
  enums?: any;
  functions?: any;
  sequences?: any;
  extensions?: any;
  views?: any[];
  procedures?: any[];
  [key: string]: any;
}

export interface GetDatabaseSchemaResponse {
  message: string;
  success: boolean;
  status: string;
  schema: DatabaseSchema;
}

// Instance Types
export interface Instance {
  tenant_id: string;
  workspace_id: string;
  environment_id?: string;
  instance_id: string;
  instance_name: string;
  instance_description?: string;
  instance_type: string;
  instance_vendor: string;
  instance_version?: string;
  instance_unique_identifier?: string;
  connected_to_node_id?: string;
  instance_host: string;
  instance_port: number;
  instance_username?: string;
  instance_password?: string;
  instance_system_db_name?: string;
  instance_enabled: boolean;
  instance_ssl?: boolean;
  instance_ssl_mode?: string;
  instance_ssl_cert?: string;
  instance_ssl_key?: string;
  instance_ssl_root_cert?: string;
  policy_ids?: string[];
  owner_id?: string;
  instance_status_message?: string;
  status: string;
  database_count?: number;
  created?: string;
  updated?: string;
}

export interface ListInstancesResponse {
  instances: Instance[];
}

export interface ShowInstanceResponse {
  instance: Instance;
}

export interface ConnectInstanceRequest {
  instance_name: string;
  instance_description?: string;
  instance_type: string;
  instance_vendor: string;
  host: string;
  port: number;
  username: string;
  password: string;
  node_id?: string;
  enabled?: boolean;
  ssl?: boolean;
  ssl_mode?: string;
  ssl_cert?: string;
  ssl_key?: string;
  ssl_root_cert?: string;
  environment_id?: string;
}

export interface ConnectInstanceResponse {
  message: string;
  success: boolean;
  instance: Instance;
  status: string;
}

export interface ModifyInstanceRequest {
  instance_name?: string;
  instance_description?: string;
  instance_type?: string;
  instance_vendor?: string;
  host?: string;
  port?: number;
  username?: string;
  password?: string;
  enabled?: boolean;
  ssl?: boolean;
  ssl_mode?: string;
  ssl_cert?: string;
  ssl_key?: string;
  ssl_root_cert?: string;
  environment_id?: string;
  node_id?: string;
}

export interface DisconnectInstanceRequest {
  delete_instance?: boolean;
}

// Mapping Types
export interface MappingRuleMetadata {
  generated_at?: string;
  match_score?: number;
  match_type?: string;
  source_column?: string;
  source_table?: string;
  target_column?: string;
  target_table?: string;
  type_compatible?: boolean;
  source_database_id?: string;
  source_database_name?: string;
  target_database_id?: string;
  target_database_name?: string;
  source_resource_uri?: string;
  target_resource_uri?: string;
  transformation_name?: string;
  transformation_options?: any;
}

export interface MappingRule {
  mapping_rule_id: string;
  mapping_rule_name: string;
  mapping_rule_description?: string;
  mapping_rule_metadata?: MappingRuleMetadata;
  mapping_rule_source: string;
  mapping_rule_target: string;
  mapping_rule_transformation_id?: string;
  mapping_rule_transformation_name?: string;
  mapping_rule_transformation_options?: string;
  source_items?: ResourceItem[];
  target_items?: ResourceItem[];
}

export interface RelationshipInfo {
  relationship_name: string;
  status: string;
}

export interface Mapping {
  tenant_id: string;
  workspace_id: string;
  mapping_id: string;
  mapping_name: string;
  mapping_description?: string;
  mapping_type?: string;
  mapping_source_type?: string;
  mapping_target_type?: string;
  mapping_source?: string;
  mapping_target?: string;
  mapping_source_container_id?: string;
  mapping_target_container_id?: string;
  policy_id?: string;
  map_object?: any;
  owner_id?: string;
  mapping_rule_count?: number;
  mapping_rules?: MappingRule[];
  validated?: boolean;
  validated_at?: string;
  validation_errors?: string[];
  validation_warnings?: string[];
  relationship_names?: string[];
  relationship_infos?: RelationshipInfo[];
  mcp_resource_names?: string[];
  mcp_tool_names?: string[];
  source_container_items?: ResourceItem[];
  target_container_items?: ResourceItem[];
  source_database_id?: string;
  source_database_name?: string;
  source_table_name?: string;
  target_database_id?: string;
  target_database_name?: string;
  target_table_name?: string;
  created?: string;
  updated?: string;
}

export interface ListMappingsResponse {
  mappings: Mapping[];
}

export interface ValidateMappingResponse {
  data: {
    is_valid: boolean;
    errors: string[];
    warnings: string[];
    validated_at: string;
  };
  message: string;
  status: string;
}

export interface ShowMappingResponse {
  mapping: Mapping;
}

export interface CreateMappingRequest {
  mapping_name: string;
  mapping_description: string;
  mapping_source_type: string;
  mapping_target_type: string;
  mapping_source: string;
  mapping_target: string;
  policy_id?: string;
  map_object?: any;
}

export interface CreateDatabaseMappingRequest {
  mapping_name: string;
  mapping_description: string;
  mapping_source_database_name: string;
  mapping_target_database_name: string;
  policy_id?: string;
}

export interface CreateResourceMappingRequest {
  mapping_name: string;
  mapping_description: string;
  source_type: ResourceType;
  source_uri: string;
  target_type: ResourceType;
  target_uri: string;
  generate_rules?: boolean;
  policy_id?: string;
}

export interface CreateMappingResponse {
  message: string;
  success: boolean;
  mapping: Mapping;
  status: string;
}

export interface ModifyMappingRequest {
  mapping_name?: string;
  mapping_description?: string;
  mapping_source_type?: string;
  mapping_target_type?: string;
  mapping_source?: string;
  mapping_target?: string;
  policy_id?: string;
  map_object?: any;
}

// Relationship Types
export interface Relationship {
  tenant_id: string;
  workspace_id: string;
  relationship_id: string;
  relationship_name: string;
  relationship_description?: string;
  relationship_type: string;
  relationship_source?: string;
  relationship_target?: string;
  relationship_source_database_id?: string;
  relationship_source_database_name?: string;
  relationship_source_database_type?: string;
  relationship_source_table_name?: string;
  relationship_target_database_id?: string;
  relationship_target_database_name?: string;
  relationship_target_database_type?: string;
  relationship_target_table_name?: string;
  mapping_id: string;
  mapping_name?: string;
  policy_id?: string;
  status_message?: string;
  status: string;
  owner_id?: string;
}

export interface ListRelationshipsResponse {
  relationships: Relationship[];
}

export interface ShowRelationshipResponse {
  relationship: Relationship;
}

export interface CreateRelationshipRequest {
  relationship_name: string;
  relationship_description: string;
  relationship_type: string;
  relationship_source_database_id: string;
  relationship_source_table_name: string;
  relationship_target_database_id: string;
  relationship_target_table_name: string;
  mapping_id: string;
  policy_id?: string;
}

export interface CreateRelationshipResponse {
  message: string;
  success: boolean;
  relationship: Relationship;
  status: string;
}

export interface ModifyRelationshipRequest {
  relationship_name?: string;
  relationship_description?: string;
  relationship_type?: string;
  relationship_source?: string;
  relationship_target?: string;
  mapping_id?: string;
  policy_id?: string;
}

export interface StartRelationshipRequest {
  batch_size?: number;
  parallel_workers?: number;
}

export interface StartRelationshipResponse {
  message: string;
  success: boolean;
  status: string;
}

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export interface StopRelationshipRequest {
  // Empty request body
}

export interface StopRelationshipResponse {
  message: string;
  success: boolean;
  status: string;
}

// Relationship Metrics Types (to be implemented by backend)
export interface RelationshipMetrics {
  relationship_name: string;
  
  // Current metrics
  current_replication_lag_seconds: number;
  current_throughput_records_per_second: number;
  current_error_rate: number;
  current_bytes_per_second: number;
  
  // Time series data (last 24 hours, 5-minute intervals)
  time_series: {
    timestamp: string; // ISO 8601 format
    replication_lag_seconds: number;
    throughput_records_per_second: number;
    error_rate: number;
    bytes_transferred: number;
  }[];
  
  // Aggregate statistics
  total_records_replicated: number;
  total_bytes_transferred: number;
  total_errors: number;
  uptime_percentage: number;
  
  // Additional metrics
  average_record_size_bytes: number;
  last_sync_timestamp: string;
  next_sync_timestamp?: string;
}

export interface GetRelationshipMetricsResponse {
  metrics: RelationshipMetrics;
}

// MCP Server Types
export interface MCPServer {
  tenant_id: string;
  workspace_id: string;
  mcp_server_id: string;
  mcp_server_name: string;
  mcp_server_description?: string;
  mcp_server_host_ids?: string[];
  mcp_server_port: number;
  mcp_server_enabled: boolean;
  policy_ids?: string[];
  owner_id?: string;
  status_message?: string;
  status: string;
  created?: string;
  updated?: string;
}

export interface ListMCPServersResponse {
  mcp_servers: MCPServer[];
}

export interface ShowMCPServerResponse {
  mcp_server: MCPServer;
}

export interface AddMCPServerRequest {
  mcp_server_name: string;
  mcp_server_description?: string;
  mcp_server_port: number;
  mcp_server_host_ids?: string[];
  mcp_server_enabled?: boolean;
  policy_ids?: string[];
}

export interface AddMCPServerResponse {
  message: string;
  success: boolean;
  mcp_server: MCPServer;
  status: string;
}

// Workspace Types
export interface Workspace {
  tenant_id: string;
  workspace_id: string;
  workspace_name: string;
  workspace_description?: string;
  owner_id?: string;
  created?: string;
  updated?: string;
}

export interface ListWorkspacesResponse {
  workspaces: Workspace[];
}

// Repository Types
export interface Repository {
  tenant_id: string;
  workspace_id: string;
  repo_id: string;
  repo_name: string;
  repo_description?: string;
  repo_type?: string;
  owner_id?: string;
  branch_count?: number;
  branches?: Branch[];  // Nested branches when fetching single repository
  created?: string;
  updated?: string;
  status?: string;
}

export interface ListRepositoriesResponse {
  repos: Repository[];
}

export interface ShowRepositoryResponse {
  repo: Repository;
}

export interface AddRepositoryRequest {
  repo_name: string;
  repo_description?: string;
  repo_type?: string;
}

export interface AddRepositoryResponse {
  message: string;
  success: boolean;
  repository: Repository;
  status: string;
}

export interface ModifyRepositoryRequest {
  repo_name?: string;
  repo_description?: string;
}

export interface DeleteRepositoryRequest {
  delete_branches?: boolean;
}

// Branch Types
export interface Branch {
  tenant_id: string;
  workspace_id: string;
  repo_id: string;
  repo_name: string;
  branch_id: string;
  branch_name: string;
  branch_description?: string;
  parent_branch_id?: string;
  parent_branch_name?: string;
  attached_database_id?: string;
  attached_database_name?: string;
  commit_count?: number;
  commits?: Commit[];  // Nested commits when fetching single branch
  branches?: Branch[];  // Nested child branches when fetching single branch
  owner_id?: string;
  created?: string;
  updated?: string;
  status?: string;
}

export interface ListBranchesResponse {
  branches: Branch[];
}

export interface ShowBranchResponse {
  branch: Branch;
}

export interface CreateBranchRequest {
  branch_name: string;
  branch_description?: string;
  parent_branch_name?: string;
}

export interface CreateBranchResponse {
  message: string;
  success: boolean;
  branch: Branch;
  status: string;
}

export interface ModifyBranchRequest {
  branch_name?: string;
  branch_description?: string;
}

export interface AttachBranchRequest {
  database_name: string;
}

export interface AttachBranchResponse {
  message: string;
  success: boolean;
  branch: Branch;
  status: string;
}

export interface DetachBranchResponse {
  message: string;
  success: boolean;
  branch: Branch;
  status: string;
}

// Commit Types
export interface Commit {
  tenant_id: string;
  workspace_id: string;
  repo_id: string;
  repo_name: string;
  branch_id: string;
  branch_name: string;
  commit_id: string;
  commit_code: string;
  commit_message?: string;
  commit_description?: string;
  schema_structure?: string;
  parent_commit_id?: string;
  parent_commit_code?: string;
  owner_id?: string;
  created?: string;
  updated?: string;
  status?: string;
}

export interface ListCommitsResponse {
  commits: Commit[];
}

export interface ShowCommitResponse {
  commit: Commit;
}

export interface BranchCommitRequest {
  new_branch_name: string;
}

export interface BranchCommitResponse {
  message: string;
  success: boolean;
  commit: Commit;
  status: string;
}

export interface MergeCommitResponse {
  message: string;
  success: boolean;
  commit: Commit;
  status: string;
}

export interface DeployCommitResponse {
  message: string;
  success: boolean;
  status: string;
}

// Environment Types
export interface Environment {
  tenant_id: string;
  workspace_id: string;
  environment_id: string;
  environment_name: string;
  environment_description?: string;
  environment_production: boolean;
  environment_criticality?: number;
  environment_priority?: number;
  owner_id?: string;
  instance_count?: number;
  database_count?: number;
  created?: string;
  updated?: string;
  status?: string;
}

export interface ListEnvironmentsResponse {
  environments: Environment[];
}

export interface ShowEnvironmentResponse {
  environment: Environment;
}

export interface CreateEnvironmentRequest {
  environment_name: string;
  environment_description?: string;
  environment_production: boolean;
  environment_criticality?: number;
  environment_priority?: number;
}

export interface CreateEnvironmentResponse {
  message: string;
  success: boolean;
  environment: Environment;
  status: string;
}

export interface ModifyEnvironmentRequest {
  environment_name?: string;
  environment_description?: string;
  environment_production?: boolean;
  environment_criticality?: number;
  environment_priority?: number;
}

// Mesh and Node Types
export interface MeshConnection {
  node_id: string;
  node_address?: string;
  connection_state?: string;
  last_ping?: string;
}

export interface Node {
  tenant_id: string;
  node_id: string;
  node_name?: string;
  node_address: string;
  node_port?: number;
  node_region?: string;
  mesh_id?: string;
  is_local?: boolean;
  connections?: MeshConnection[];
  instance_count?: number;
  database_count?: number;
  status?: string;
  status_message?: string;
  created?: string;
  updated?: string;
}

export interface Mesh {
  tenant_id: string;
  mesh_id: string;
  mesh_name?: string;
  node_count?: number;
  nodes?: Node[];
  created?: string;
  updated?: string;
  status?: string;
}

export interface ShowMeshResponse {
  mesh: Mesh;
}

export interface ListNodesResponse {
  nodes: Node[];
}

export interface ShowNodeResponse {
  node: Node;
}

export interface NodeStatusResponse {
  node: Node;
  mesh: Mesh;
  status: string;
}

// Region Types
export interface Region {
  tenant_id: string;
  region_id: string;
  region_name: string;
  region_description?: string;
  region_type: string;
  region_location?: string;
  region_latitude?: number;
  region_longitude?: number;
  node_count?: number;
  instance_count?: number;
  database_count?: number;
  owner_id?: string;
  created?: string;
  updated?: string;
  status?: string;
}

export interface ListRegionsResponse {
  regions: Region[];
}

export interface ShowRegionResponse {
  region: Region;
}

export interface CreateRegionRequest {
  region_name: string;
  region_description?: string;
  region_type: string;
  region_location?: string;
  region_latitude?: number;
  region_longitude?: number;
}

export interface CreateRegionResponse {
  message: string;
  success: boolean;
  region: Region;
  status: string;
}

export interface ModifyRegionRequest {
  region_name?: string;
  region_description?: string;
  region_location?: string;
  region_latitude?: number;
  region_longitude?: number;
}

// Transformation Types
export interface Transformation {
  transformation_id: string;
  transformation_name: string;
  transformation_description?: string;
  transformation_type: string;
  transformation_builtin: boolean;
  transformation_config?: any;
  created?: string;
  updated?: string;
}

export interface ListTransformationsResponse {
  transformations: Transformation[];
}

export interface ShowTransformationResponse {
  transformation: Transformation;
}

// Mapping Rule Types (extending existing MappingRule)
export interface AddMappingRuleRequest {
  rule_name: string;
  source: string;
  target: string;
  transformation: string;
  order?: number;
}

export interface AddMappingRuleResponse {
  message: string;
  success: boolean;
  rule: MappingRule;
  status: string;
}

export interface ModifyMappingRuleRequest {
  rule_name?: string;
  source?: string;
  target?: string;
  transformation?: string;
  order?: number;
}

export interface ListMappingRulesResponse {
  rules: MappingRule[];
}

export interface RemoveMappingRuleRequest {
  delete_rule?: boolean;
}

// MCP Tool Types
export interface MCPTool {
  tenant_id: string;
  workspace_id: string;
  mcp_tool_id: string;
  mcp_tool_name: string;
  mcp_tool_description?: string;
  mcp_tool_mapping_id?: string;
  mcp_tool_mapping_name?: string;
  mcp_tool_config?: any;
  mcp_server_ids?: string[];
  policy_ids?: string[];
  owner_id?: string;
  created?: string;
  updated?: string;
  status?: string;
}

export interface ListMCPToolsResponse {
  mcp_tools: MCPTool[];
}

export interface ShowMCPToolResponse {
  mcp_tool: MCPTool;
}

export interface AddMCPToolRequest {
  mcp_tool_name: string;
  mcp_tool_description?: string;
  mcp_tool_mapping_name: string;
  mcp_tool_config?: any;
  policy_ids?: string[];
}

export interface AddMCPToolResponse {
  message: string;
  success: boolean;
  mcp_tool: MCPTool;
  status: string;
}

export interface AttachMCPToolRequest {
  mcp_server_name: string;
}

export interface AttachMCPToolResponse {
  message: string;
  success: boolean;
  status: string;
}

export interface DetachMCPToolResponse {
  message: string;
  success: boolean;
  status: string;
}

// MCP Resource Types
export interface MCPResource {
  tenant_id: string;
  workspace_id: string;
  mcp_resource_id: string;
  mcp_resource_name: string;
  mcp_resource_description?: string;
  mcp_resource_mapping_id?: string;
  mcp_resource_mapping_name?: string;
  mcp_resource_config?: any;
  mcp_server_ids?: string[];
  policy_ids?: string[];
  owner_id?: string;
  created?: string;
  updated?: string;
  status?: string;
}

export interface ListMCPResourcesResponse {
  mcp_resources: MCPResource[];
}

export interface ShowMCPResourceResponse {
  mcp_resource: MCPResource;
}

export interface AddMCPResourceRequest {
  mcp_resource_name: string;
  mcp_resource_description?: string;
  mcp_resource_mapping_name: string;
  mcp_resource_config?: any;
  policy_ids?: string[];
}

export interface AddMCPResourceResponse {
  message: string;
  success: boolean;
  mcp_resource: MCPResource;
  status: string;
}

export interface AttachMCPResourceRequest {
  mcp_server_name: string;
}

export interface AttachMCPResourceResponse {
  message: string;
  success: boolean;
  status: string;
}

export interface DetachMCPResourceResponse {
  message: string;
  success: boolean;
  status: string;
}

// Webhook Types (Placeholder for future implementation)
export interface Webhook {
  tenant_id: string;
  workspace_id: string;
  webhook_id: string;
  webhook_name: string;
  webhook_description?: string;
  webhook_url?: string;
  webhook_method?: string;
  webhook_headers?: Record<string, string>;
  webhook_auth_type?: string;
  mapping_id?: string;
  policy_ids?: string[];
  owner_id?: string;
  status?: string;
  created?: string;
  updated?: string;
}

export interface ListWebhooksResponse {
  webhooks: Webhook[];
}

export interface ShowWebhookResponse {
  webhook: Webhook;
}

// User Types
export interface User {
  tenant_id: string;
  user_id: string;
  user_name: string;
  user_email: string;
  user_first_name?: string;
  user_last_name?: string;
  user_role?: string;
  user_enabled: boolean;
  workspace_ids?: string[];
  created?: string;
  updated?: string;
}

export interface ListUsersResponse {
  users: User[];
}

export interface ShowUserResponse {
  user: User;
}

export interface AddUserRequest {
  user_name: string;
  user_email: string;
  user_password: string;
  user_first_name?: string;
  user_last_name?: string;
  user_role?: string;
  user_enabled?: boolean;
}

export interface AddUserResponse {
  message: string;
  success: boolean;
  user: User;
  status: string;
}

export interface ModifyUserRequest {
  user_name?: string;
  user_email?: string;
  user_first_name?: string;
  user_last_name?: string;
  user_role?: string;
  user_enabled?: boolean;
}

export interface ChangePasswordRequest {
  old_password: string;
  new_password: string;
}

export interface ChangePasswordResponse {
  message: string;
  success: boolean;
  status: string;
}

// Session Types
export interface Session {
  session_id: string;
  user_id: string;
  session_name?: string;
  user_agent?: string;
  ip_address?: string;
  created: string;
  expires: string;
  last_active?: string;
  is_current?: boolean;
}

export interface ListSessionsResponse {
  sessions: Session[];
}

export interface LogoutSessionResponse {
  message: string;
  success: boolean;
  status: string;
}


// Table Data Types
export interface TableColumnSchema {
  name: string;
  item_display_name?: string;
  data_type: string;
  unified_data_type?: string;
  is_nullable: boolean;
  is_primary_key: boolean;
  is_unique: boolean;
  is_indexed: boolean;
  is_required?: boolean;
  is_array?: boolean;
  default_value?: string;
  constraints: string[];
  is_privileged: boolean;
  privileged_classification?: string;
  privileged_confidence?: number;
  detection_method?: string;
  data_category?: string;
  classification_confidence?: number;
  ordinal_position: number;
  max_length?: number;
  precision?: number;
  scale?: number;
  item_comment?: string;
}

export interface TableDataRow {
  [key: string]: any;
}

export interface FetchTableDataResponse {
  message: string;
  success: boolean;
  status: string;
  data: TableDataRow[];
  total_rows: number;
  page: number;
  page_size: number;
  total_pages: number;
  column_schemas: TableColumnSchema[];
}

export interface WipeTableResponse {
  message: string;
  success: boolean;
  status: string;
  rows_affected: number;
}

export interface DropTableResponse {
  message: string;
  success: boolean;
  status: string;
}

export interface UpdateTableDataRequest {
  updates: Array<{
    where: Record<string, any>;
    set: Record<string, any>;
  }>;
}

export interface UpdateTableDataResponse {
  message: string;
  success: boolean;
  status: string;
  rows_affected: number;
}
