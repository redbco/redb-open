import { apiClient } from './client';
import type {
  LoginRequest,
  LoginResponse,
  ListDatabasesResponse,
  ShowDatabaseResponse,
  ConnectDatabaseRequest,
  ConnectDatabaseStringRequest,
  ConnectDatabaseResponse,
  ModifyDatabaseRequest,
  DisconnectDatabaseRequest,
  GetDatabaseDisconnectMetadataResponse,
  GetDatabaseSchemaResponse,
  FetchTableDataResponse,
  WipeTableResponse,
  DropTableResponse,
  UpdateTableDataRequest,
  UpdateTableDataResponse,
  ListInstancesResponse,
  ShowInstanceResponse,
  ConnectInstanceRequest,
  ConnectInstanceResponse,
  ModifyInstanceRequest,
  DisconnectInstanceRequest,
  ListMappingsResponse,
  ShowMappingResponse,
  CreateMappingRequest,
  CreateMappingWithDeployRequest,
  CreateMappingWithDeployResponse,
  CreateDatabaseMappingRequest,
  CreateMappingResponse,
  ModifyMappingRequest,
  ValidateMappingResponse,
  ListRelationshipsResponse,
  ShowRelationshipResponse,
  CreateRelationshipRequest,
  CreateRelationshipResponse,
  ModifyRelationshipRequest,
  StartRelationshipRequest,
  StartRelationshipResponse,
  StopRelationshipResponse,
  GetRelationshipMetricsResponse,
  ListWorkspacesResponse,
  AddMCPServerRequest,
  AddMCPServerResponse,
  ListMCPServersResponse,
  ShowMCPServerResponse,
  ListRepositoriesResponse,
  ShowRepositoryResponse,
  AddRepositoryRequest,
  AddRepositoryResponse,
  ModifyRepositoryRequest,
  DeleteRepositoryRequest,
  ShowBranchResponse,
  ShowCommitResponse,
  ListEnvironmentsResponse,
  ShowEnvironmentResponse,
  CreateEnvironmentRequest,
  CreateEnvironmentResponse,
  ModifyEnvironmentRequest,
  ShowMeshResponse,
  ListNodesResponse,
  ShowNodeResponse,
  NodeStatusResponse,
  ListRegionsResponse,
  ShowRegionResponse,
  CreateRegionRequest,
  CreateRegionResponse,
  ModifyRegionRequest,
  ListTransformationsResponse,
  ShowTransformationResponse,
  AddMappingRuleRequest,
  AddMappingRuleResponse,
  ModifyMappingRuleRequest,
  ListMappingRulesResponse,
  RemoveMappingRuleRequest,
  ListMCPToolsResponse,
  ShowMCPToolResponse,
  AddMCPToolRequest,
  AddMCPToolResponse,
  AttachMCPToolRequest,
  AttachMCPToolResponse,
  DetachMCPToolResponse,
  ListMCPResourcesResponse,
  ShowMCPResourceResponse,
  AddMCPResourceRequest,
  AddMCPResourceResponse,
  AttachMCPResourceRequest,
  AttachMCPResourceResponse,
  DetachMCPResourceResponse,
  ListUsersResponse,
  ShowUserResponse,
  AddUserRequest,
  AddUserResponse,
  ModifyUserRequest,
  ChangePasswordRequest,
  ChangePasswordResponse,
  ListSessionsResponse,
  LogoutSessionResponse,
  ListResourceContainersResponse,
  ListResourceItemsResponse,
  ShowResourceContainerResponse,
  GetContainerStatsResponse,
  ResourceContainerFilter,
  ResourceItemFilter,
  ResourceItem,
  ListStreamsResponse,
  ShowStreamResponse,
  ConnectStreamRequest,
  ConnectStreamResponse,
  ModifyStreamRequest,
  ModifyStreamResponse,
  ReconnectStreamResponse,
  DisconnectStreamRequest,
  DisconnectStreamResponse,
  ListTopicsResponse,
  GetTopicSchemaResponse,
  ListDataProductsResponse,
  ShowDataProductResponse,
  CreateDataProductRequest,
  CreateDataProductResponse,
  ModifyDataProductRequest,
  ModifyDataProductResponse,
  DeleteDataProductResponse,
  Database,
  Instance,
  Mapping,
  Relationship,
  Repository,
  Environment,
  Region,
  MappingRule,
} from './types';

// Authentication Endpoints
export const authEndpoints = {
  login: (request: LoginRequest) =>
    apiClient.post<LoginResponse>('api/v1/auth/login', request),

  logout: () =>
    apiClient.post<{ success: boolean; message: string }>('api/v1/auth/logout'),
};

// Workspace Endpoints
export const workspaceEndpoints = {
  list: () =>
    apiClient.get<ListWorkspacesResponse>('api/v1/workspaces'),

  // Note: Additional workspace endpoints can be added as needed
};

// Database Endpoints
export const databaseEndpoints = {
  list: (workspaceName: string) =>
    apiClient.get<ListDatabasesResponse>(`api/v1/workspaces/${workspaceName}/databases`),

  show: (workspaceName: string, databaseName: string) =>
    apiClient.get<ShowDatabaseResponse>(`api/v1/workspaces/${workspaceName}/databases/${databaseName}`),

  connect: (workspaceName: string, request: ConnectDatabaseRequest) =>
    apiClient.post<ConnectDatabaseResponse>(`api/v1/workspaces/${workspaceName}/databases/connect`, request),

  connectString: (workspaceName: string, request: ConnectDatabaseStringRequest) =>
    apiClient.post<ConnectDatabaseResponse>(`api/v1/workspaces/${workspaceName}/databases/connect-string`, request),

  modify: (workspaceName: string, databaseName: string, request: ModifyDatabaseRequest) =>
    apiClient.put<{ success: boolean; message: string; database: Database }>(
      `api/v1/workspaces/${workspaceName}/databases/${databaseName}`,
      request
    ),

  disconnect: (workspaceName: string, databaseName: string, request?: DisconnectDatabaseRequest) =>
    apiClient.post<{ success: boolean; message: string }>(
      `api/v1/workspaces/${workspaceName}/databases/${databaseName}/disconnect`,
      request
    ),

  getDisconnectMetadata: (workspaceName: string, databaseName: string) =>
    apiClient.get<GetDatabaseDisconnectMetadataResponse>(
      `api/v1/workspaces/${workspaceName}/databases/${databaseName}/disconnect-metadata`
    ),

  getSchema: (workspaceName: string, databaseName: string) =>
    apiClient.get<GetDatabaseSchemaResponse>(`api/v1/workspaces/${workspaceName}/databases/${databaseName}/schema`),

  wipe: (workspaceName: string, databaseName: string) =>
    apiClient.post<{ success: boolean; message: string }>(
      `api/v1/workspaces/${workspaceName}/databases/${databaseName}/wipe`
    ),

  drop: (workspaceName: string, databaseName: string) =>
    apiClient.post<{ success: boolean; message: string }>(
      `api/v1/workspaces/${workspaceName}/databases/${databaseName}/drop`
    ),

  // Table data operations
  fetchTableData: (workspaceName: string, databaseName: string, tableName: string, page: number = 1, pageSize: number = 25) =>
    apiClient.get<FetchTableDataResponse>(
      `api/v1/workspaces/${workspaceName}/databases/${databaseName}/tables/${tableName}/data?page=${page}&page_size=${pageSize}`
    ),

  wipeTable: (workspaceName: string, databaseName: string, tableName: string) =>
    apiClient.post<WipeTableResponse>(
      `api/v1/workspaces/${workspaceName}/databases/${databaseName}/tables/${tableName}/wipe`
    ),

  dropTable: (workspaceName: string, databaseName: string, tableName: string) =>
    apiClient.post<DropTableResponse>(
      `api/v1/workspaces/${workspaceName}/databases/${databaseName}/tables/${tableName}/drop`
    ),

  updateTableData: (workspaceName: string, databaseName: string, tableName: string, request: UpdateTableDataRequest) =>
    apiClient.put<UpdateTableDataResponse>(
      `api/v1/workspaces/${workspaceName}/databases/${databaseName}/tables/${tableName}/data`,
      request
    ),
};

// Instance Endpoints
export const instanceEndpoints = {
  list: (workspaceName: string) =>
    apiClient.get<ListInstancesResponse>(`api/v1/workspaces/${workspaceName}/instances`),

  show: (workspaceName: string, instanceName: string) =>
    apiClient.get<ShowInstanceResponse>(`api/v1/workspaces/${workspaceName}/instances/${instanceName}`),

  connect: (workspaceName: string, request: ConnectInstanceRequest) =>
    apiClient.post<ConnectInstanceResponse>(`api/v1/workspaces/${workspaceName}/instances/connect`, request),

  modify: (workspaceName: string, instanceName: string, request: ModifyInstanceRequest) =>
    apiClient.put<{ success: boolean; message: string; instance: Instance }>(
      `api/v1/workspaces/${workspaceName}/instances/${instanceName}`,
      request
    ),

  disconnect: (workspaceName: string, instanceName: string, request?: DisconnectInstanceRequest) =>
    apiClient.post<{ success: boolean; message: string }>(
      `api/v1/workspaces/${workspaceName}/instances/${instanceName}/disconnect`,
      request
    ),

  reconnect: (workspaceName: string, instanceName: string) =>
    apiClient.post<{ success: boolean; message: string }>(
      `api/v1/workspaces/${workspaceName}/instances/${instanceName}/reconnect`
    ),
};

// Mapping Endpoints
export const mappingEndpoints = {
  list: (workspaceName: string) =>
    apiClient.get<ListMappingsResponse>(`api/v1/workspaces/${workspaceName}/mappings`),

  show: (workspaceName: string, mappingId: string) =>
    apiClient.get<ShowMappingResponse>(`api/v1/workspaces/${workspaceName}/mappings/${mappingId}`),

  create: (workspaceName: string, request: CreateMappingRequest) =>
    apiClient.post<CreateMappingResponse>(`api/v1/workspaces/${workspaceName}/mappings`, request),

  createWithDeploy: (workspaceName: string, request: CreateMappingWithDeployRequest) =>
    apiClient.post<CreateMappingWithDeployResponse>(`api/v1/workspaces/${workspaceName}/mappings/table-with-deploy`, request),

  createDatabaseMapping: (workspaceName: string, request: CreateDatabaseMappingRequest) =>
    apiClient.post<CreateMappingResponse>(`api/v1/workspaces/${workspaceName}/mappings/database`, request),

  modify: (workspaceName: string, mappingId: string, request: ModifyMappingRequest) =>
    apiClient.put<{ success: boolean; message: string; mapping: Mapping }>(
      `api/v1/workspaces/${workspaceName}/mappings/${mappingId}`,
      request
    ),

  delete: (workspaceName: string, mappingId: string) =>
    apiClient.delete<{ success: boolean; message: string }>(
      `api/v1/workspaces/${workspaceName}/mappings/${mappingId}`
    ),

  validate: (workspaceName: string, mappingName: string) =>
    apiClient.post<ValidateMappingResponse>(
      `api/v1/workspaces/${workspaceName}/mappings/${mappingName}/validate`,
      {}
    ),
};

// Relationship Endpoints
export const relationshipEndpoints = {
  list: (workspaceName: string) =>
    apiClient.get<ListRelationshipsResponse>(`api/v1/workspaces/${workspaceName}/relationships`),

  show: (workspaceName: string, relationshipId: string) =>
    apiClient.get<ShowRelationshipResponse>(`api/v1/workspaces/${workspaceName}/relationships/${relationshipId}`),

  create: (workspaceName: string, request: CreateRelationshipRequest) =>
    apiClient.post<CreateRelationshipResponse>(`api/v1/workspaces/${workspaceName}/relationships`, request),

  modify: (workspaceName: string, relationshipId: string, request: ModifyRelationshipRequest) =>
    apiClient.put<{ success: boolean; message: string; relationship: Relationship }>(
      `api/v1/workspaces/${workspaceName}/relationships/${relationshipId}`,
      request
    ),

  delete: (workspaceName: string, relationshipId: string) =>
    apiClient.delete<{ success: boolean; message: string }>(
      `api/v1/workspaces/${workspaceName}/relationships/${relationshipId}`
    ),

  start: (workspaceName: string, relationshipName: string, request?: StartRelationshipRequest) =>
    apiClient.post<StartRelationshipResponse>(
      `api/v1/workspaces/${workspaceName}/relationships/${relationshipName}/start`,
      request || {}
    ),

  stop: (workspaceName: string, relationshipName: string) =>
    apiClient.post<StopRelationshipResponse>(
      `api/v1/workspaces/${workspaceName}/relationships/${relationshipName}/stop`,
      {}
    ),

  remove: (workspaceName: string, relationshipName: string, force?: boolean) => {
    const url = `api/v1/workspaces/${workspaceName}/relationships/${relationshipName}/remove${force ? '?force=true' : ''}`;
    return apiClient.delete<{ success: boolean; message: string }>(url);
  },

  getMetrics: (workspaceName: string, relationshipName: string) =>
    apiClient.get<GetRelationshipMetricsResponse>(
      `api/v1/workspaces/${workspaceName}/relationships/${relationshipName}/metrics`
    ),
};

// MCP Server Endpoints
export const mcpServerEndpoints = {
  list: (workspaceName: string) =>
    apiClient.get<ListMCPServersResponse>(`api/v1/workspaces/${workspaceName}/mcpservers`),

  show: (workspaceName: string, serverName: string) =>
    apiClient.get<ShowMCPServerResponse>(`api/v1/workspaces/${workspaceName}/mcpservers/${serverName}`),

  add: (workspaceName: string, request: AddMCPServerRequest) =>
    apiClient.post<AddMCPServerResponse>(`api/v1/workspaces/${workspaceName}/mcpservers`, request),

  delete: (workspaceName: string, serverName: string) =>
    apiClient.delete<{ success: boolean; message: string }>(
      `api/v1/workspaces/${workspaceName}/mcpservers/${serverName}`
    ),
};

// Repository Endpoints
export const repositoryEndpoints = {
  list: (workspaceName: string) =>
    apiClient.get<ListRepositoriesResponse>(`api/v1/workspaces/${workspaceName}/repos`),

  show: (workspaceName: string, repoName: string) =>
    apiClient.get<ShowRepositoryResponse>(`api/v1/workspaces/${workspaceName}/repos/${repoName}`),

  add: (workspaceName: string, request: AddRepositoryRequest) =>
    apiClient.post<AddRepositoryResponse>(`api/v1/workspaces/${workspaceName}/repos`, request),

  modify: (workspaceName: string, repoName: string, request: ModifyRepositoryRequest) =>
    apiClient.put<{ success: boolean; message: string; repository: Repository }>(
      `api/v1/workspaces/${workspaceName}/repos/${repoName}`,
      request
    ),

  delete: (workspaceName: string, repoName: string, request?: DeleteRepositoryRequest) =>
    apiClient.delete<{ success: boolean; message: string }>(
      `api/v1/workspaces/${workspaceName}/repos/${repoName}`,
      request ? { body: JSON.stringify(request) } : undefined
    ),
};

// Branch Endpoints
export const branchEndpoints = {
  show: (workspaceName: string, repoName: string, branchName: string) =>
    apiClient.get<ShowBranchResponse>(
      `api/v1/workspaces/${workspaceName}/repos/${repoName}/branches/${branchName}`
    ),
};

// Commit Endpoints
export const commitEndpoints = {
  show: (workspaceName: string, repoName: string, branchName: string, commitCode: string) =>
    apiClient.get<ShowCommitResponse>(
      `api/v1/workspaces/${workspaceName}/repos/${repoName}/branches/${branchName}/commits/${commitCode}`
    ),
};

// Environment Endpoints
export const environmentEndpoints = {
  list: (workspaceName: string) =>
    apiClient.get<ListEnvironmentsResponse>(`api/v1/workspaces/${workspaceName}/environments`),

  show: (workspaceName: string, environmentName: string) =>
    apiClient.get<ShowEnvironmentResponse>(`api/v1/workspaces/${workspaceName}/environments/${environmentName}`),

  create: (workspaceName: string, request: CreateEnvironmentRequest) =>
    apiClient.post<CreateEnvironmentResponse>(`api/v1/workspaces/${workspaceName}/environments`, request),

  modify: (workspaceName: string, environmentName: string, request: ModifyEnvironmentRequest) =>
    apiClient.put<{ success: boolean; message: string; environment: Environment }>(
      `api/v1/workspaces/${workspaceName}/environments/${environmentName}`,
      request
    ),

  delete: (workspaceName: string, environmentName: string) =>
    apiClient.delete<{ success: boolean; message: string }>(
      `api/v1/workspaces/${workspaceName}/environments/${environmentName}`
    ),
};

// Mesh Endpoints (tenant-level)
export const meshEndpoints = {
  show: () =>
    apiClient.get<ShowMeshResponse>('api/v1/mesh'),

  listNodes: () =>
    apiClient.get<ListNodesResponse>('api/v1/mesh/nodes'),

  showNode: (nodeId: string) =>
    apiClient.get<ShowNodeResponse>(`api/v1/mesh/nodes/${nodeId}`),

  nodeStatus: () =>
    apiClient.get<NodeStatusResponse>('api/v1/node/status'),
};

// Region Endpoints (tenant-level)
export const regionEndpoints = {
  list: () =>
    apiClient.get<ListRegionsResponse>('api/v1/regions'),

  show: (regionName: string) =>
    apiClient.get<ShowRegionResponse>(`api/v1/regions/${regionName}`),

  create: (request: CreateRegionRequest) =>
    apiClient.post<CreateRegionResponse>('api/v1/regions', request),

  modify: (regionName: string, request: ModifyRegionRequest) =>
    apiClient.put<{ success: boolean; message: string; region: Region }>(
      `api/v1/regions/${regionName}`,
      request
    ),

  delete: (regionName: string) =>
    apiClient.delete<{ success: boolean; message: string }>(
      `api/v1/regions/${regionName}`
    ),
};

// Transformation Endpoints
export const transformationEndpoints = {
  list: () =>
    apiClient.get<ListTransformationsResponse>('api/v1/transformations?builtin=true'),

  show: (transformationName: string) =>
    apiClient.get<ShowTransformationResponse>(`api/v1/transformations/${transformationName}`),
};

// Mapping Rule Endpoints (extends mapping endpoints)
export const mappingRuleEndpoints = {
  list: (workspaceName: string, mappingName: string) =>
    apiClient.get<ListMappingRulesResponse>(`api/v1/workspaces/${workspaceName}/mappings/${mappingName}/rules`),

  add: (workspaceName: string, mappingName: string, request: AddMappingRuleRequest) =>
    apiClient.post<AddMappingRuleResponse>(
      `api/v1/workspaces/${workspaceName}/mappings/${mappingName}/rules`,
      request
    ),

  modify: (workspaceName: string, mappingName: string, ruleName: string, request: ModifyMappingRuleRequest) =>
    apiClient.put<{ success: boolean; message: string; rule: MappingRule }>(
      `api/v1/workspaces/${workspaceName}/mappings/${mappingName}/rules/${ruleName}`,
      request
    ),

  remove: (workspaceName: string, mappingName: string, ruleName: string, request?: RemoveMappingRuleRequest) =>
    apiClient.delete<{ success: boolean; message: string }>(
      `api/v1/workspaces/${workspaceName}/mappings/${mappingName}/rules/${ruleName}`,
      request ? { body: JSON.stringify(request) } : undefined
    ),
};

// MCP Tool Endpoints
export const mcpToolEndpoints = {
  list: (workspaceName: string) =>
    apiClient.get<ListMCPToolsResponse>(`api/v1/workspaces/${workspaceName}/mcptools`),

  show: (workspaceName: string, toolName: string) =>
    apiClient.get<ShowMCPToolResponse>(`api/v1/workspaces/${workspaceName}/mcptools/${toolName}`),

  add: (workspaceName: string, request: AddMCPToolRequest) =>
    apiClient.post<AddMCPToolResponse>(`api/v1/workspaces/${workspaceName}/mcptools`, request),

  attach: (workspaceName: string, toolName: string, request: AttachMCPToolRequest) =>
    apiClient.post<AttachMCPToolResponse>(
      `api/v1/workspaces/${workspaceName}/mcptools/${toolName}/attach`,
      request
    ),

  detach: (workspaceName: string, toolName: string, serverName: string) =>
    apiClient.post<DetachMCPToolResponse>(
      `api/v1/workspaces/${workspaceName}/mcptools/${toolName}/detach`,
      { mcp_server_name: serverName }
    ),

  delete: (workspaceName: string, toolName: string) =>
    apiClient.delete<{ success: boolean; message: string }>(
      `api/v1/workspaces/${workspaceName}/mcptools/${toolName}`
    ),
};

// MCP Resource Endpoints
export const mcpResourceEndpoints = {
  list: (workspaceName: string) =>
    apiClient.get<ListMCPResourcesResponse>(`api/v1/workspaces/${workspaceName}/mcpresources`),

  show: (workspaceName: string, resourceName: string) =>
    apiClient.get<ShowMCPResourceResponse>(`api/v1/workspaces/${workspaceName}/mcpresources/${resourceName}`),

  add: (workspaceName: string, request: AddMCPResourceRequest) =>
    apiClient.post<AddMCPResourceResponse>(`api/v1/workspaces/${workspaceName}/mcpresources`, request),

  attach: (workspaceName: string, resourceName: string, request: AttachMCPResourceRequest) =>
    apiClient.post<AttachMCPResourceResponse>(
      `api/v1/workspaces/${workspaceName}/mcpresources/${resourceName}/attach`,
      request
    ),

  detach: (workspaceName: string, resourceName: string, serverName: string) =>
    apiClient.post<DetachMCPResourceResponse>(
      `api/v1/workspaces/${workspaceName}/mcpresources/${resourceName}/detach`,
      { mcp_server_name: serverName }
    ),

  delete: (workspaceName: string, resourceName: string) =>
    apiClient.delete<{ success: boolean; message: string }>(
      `api/v1/workspaces/${workspaceName}/mcpresources/${resourceName}`
    ),
};

// User Endpoints (tenant-level)
export const userEndpoints = {
  list: () =>
    apiClient.get<ListUsersResponse>('api/v1/users'),

  show: (userId: string) =>
    apiClient.get<ShowUserResponse>(`api/v1/users/${userId}`),

  add: (request: AddUserRequest) =>
    apiClient.post<AddUserResponse>('api/v1/users', request),

  modify: (userId: string, request: ModifyUserRequest) =>
    apiClient.put<{ success: boolean; message: string; user: ShowUserResponse }>(
      `api/v1/users/${userId}`,
      request
    ),

  delete: (userId: string) =>
    apiClient.delete<{ success: boolean; message: string }>(
      `api/v1/users/${userId}`
    ),

  changePassword: (request: ChangePasswordRequest) =>
    apiClient.post<ChangePasswordResponse>('api/v1/auth/password', request),

  listSessions: () =>
    apiClient.get<ListSessionsResponse>('api/v1/auth/sessions'),

  logoutSession: (sessionId: string) =>
    apiClient.post<LogoutSessionResponse>(`api/v1/auth/sessions/${sessionId}/logout`),

  logoutAllSessions: () =>
    apiClient.post<{ success: boolean; message: string }>('api/v1/auth/sessions/logout-all'),
};

// Resource Endpoints (for resource containers and items)
export const resourceEndpoints = {
  listContainers: (workspaceName: string, filters?: ResourceContainerFilter) => {
    const params = new URLSearchParams();
    if (filters) {
      Object.entries(filters).forEach(([key, value]) => {
        if (value !== undefined && value !== null) {
          params.append(key, String(value));
        }
      });
    }
    const queryString = params.toString();
    return apiClient.get<ListResourceContainersResponse>(
      `api/v1/workspaces/${workspaceName}/resources/containers${queryString ? `?${queryString}` : ''}`
    );
  },

  showContainer: (workspaceName: string, containerId: string) =>
    apiClient.get<ShowResourceContainerResponse>(
      `api/v1/workspaces/${workspaceName}/resources/containers/${containerId}`
    ),

  listItems: (workspaceName: string, filters?: ResourceItemFilter) => {
    const params = new URLSearchParams();
    if (filters) {
      Object.entries(filters).forEach(([key, value]) => {
        if (value !== undefined && value !== null) {
          params.append(key, String(value));
        }
      });
    }
    const queryString = params.toString();
    return apiClient.get<ListResourceItemsResponse>(
      `api/v1/workspaces/${workspaceName}/resources/items${queryString ? `?${queryString}` : ''}`
    );
  },

  listItemsForContainer: (workspaceName: string, containerId: string) =>
    apiClient.get<ListResourceItemsResponse>(
      `api/v1/workspaces/${workspaceName}/resources/containers/${containerId}/items`
    ),

  modifyItem: (workspaceName: string, itemId: string, data: { item_display_name?: string }) =>
    apiClient.patch<{ success: boolean; message: string; item: ResourceItem }>(
      `api/v1/workspaces/${workspaceName}/resources/items/${itemId}`,
      data
    ),

  getContainerStats: (workspaceName: string) =>
    apiClient.get<GetContainerStatsResponse>(
      `api/v1/workspaces/${workspaceName}/resources/container-stats`
    ),
};

// Data Product Endpoints
export const dataProductEndpoints = {
  list: (workspaceName: string) =>
    apiClient.get<ListDataProductsResponse>(`api/v1/workspaces/${workspaceName}/dataproducts`),

  show: (workspaceName: string, productName: string) =>
    apiClient.get<ShowDataProductResponse>(`api/v1/workspaces/${workspaceName}/dataproducts/${productName}`),

  create: (workspaceName: string, request: CreateDataProductRequest) =>
    apiClient.post<CreateDataProductResponse>(`api/v1/workspaces/${workspaceName}/dataproducts`, request),

  modify: (workspaceName: string, productName: string, request: ModifyDataProductRequest) =>
    apiClient.put<ModifyDataProductResponse>(
      `api/v1/workspaces/${workspaceName}/dataproducts/${productName}`,
      request
    ),

  delete: (workspaceName: string, productName: string) =>
    apiClient.delete<DeleteDataProductResponse>(
      `api/v1/workspaces/${workspaceName}/dataproducts/${productName}`
    ),
};

// Webhook Endpoints (Placeholders)
export const webhookEndpoints = {
  list: () =>
    // Placeholder: Returns empty array until webhooks are implemented
    Promise.resolve({ webhooks: [] }),

  show: () =>
    // Placeholder: Returns 404 until webhooks are implemented
    Promise.reject(new Error('Webhooks not yet implemented')),
};

// Stream Endpoints
export const streamEndpoints = {
  list: (workspaceName: string) =>
    apiClient.get<ListStreamsResponse>(`api/v1/workspaces/${workspaceName}/streams`),

  show: (workspaceName: string, streamName: string) =>
    apiClient.get<ShowStreamResponse>(`api/v1/workspaces/${workspaceName}/streams/${streamName}`),

  connect: (workspaceName: string, request: ConnectStreamRequest) =>
    apiClient.post<ConnectStreamResponse>(`api/v1/workspaces/${workspaceName}/streams/connect`, request),

  modify: (workspaceName: string, streamName: string, request: ModifyStreamRequest) =>
    apiClient.put<ModifyStreamResponse>(`api/v1/workspaces/${workspaceName}/streams/${streamName}`, request),

  reconnect: (workspaceName: string, streamName: string) =>
    apiClient.post<ReconnectStreamResponse>(`api/v1/workspaces/${workspaceName}/streams/${streamName}/reconnect`, {}),

  disconnect: (workspaceName: string, streamName: string, request?: DisconnectStreamRequest) =>
    apiClient.post<DisconnectStreamResponse>(`api/v1/workspaces/${workspaceName}/streams/${streamName}/disconnect`, request || { delete_stream: false }),

  listTopics: (workspaceName: string, streamName: string) =>
    apiClient.get<ListTopicsResponse>(`api/v1/workspaces/${workspaceName}/streams/${streamName}/topics`),

  getTopicSchema: (workspaceName: string, streamName: string, topicName: string) =>
    apiClient.get<GetTopicSchemaResponse>(`api/v1/workspaces/${workspaceName}/streams/${streamName}/topics/${topicName}/schema`),
};

// Export all endpoints
export const api = {
  auth: authEndpoints,
  workspaces: workspaceEndpoints,
  databases: databaseEndpoints,
  instances: instanceEndpoints,
  mappings: mappingEndpoints,
  relationships: relationshipEndpoints,
  mcpServers: mcpServerEndpoints,
  repositories: repositoryEndpoints,
  branches: branchEndpoints,
  commits: commitEndpoints,
  environments: environmentEndpoints,
  mesh: meshEndpoints,
  regions: regionEndpoints,
  transformations: transformationEndpoints,
  mappingRules: mappingRuleEndpoints,
  mcpTools: mcpToolEndpoints,
  mcpResources: mcpResourceEndpoints,
  users: userEndpoints,
  resources: resourceEndpoints,
  dataProducts: dataProductEndpoints,
  webhooks: webhookEndpoints,
  streams: streamEndpoints,
};

