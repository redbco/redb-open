import { apiClient } from './client';
import type {
  LoginRequest,
  LoginResponse,
  ListDatabasesResponse,
  ShowDatabaseResponse,
  ConnectDatabaseRequest,
  ConnectDatabaseResponse,
  ModifyDatabaseRequest,
  DisconnectDatabaseRequest,
  GetDatabaseSchemaResponse,
  ListInstancesResponse,
  ShowInstanceResponse,
  ConnectInstanceRequest,
  ConnectInstanceResponse,
  ModifyInstanceRequest,
  DisconnectInstanceRequest,
  ListMappingsResponse,
  ShowMappingResponse,
  CreateMappingRequest,
  CreateDatabaseMappingRequest,
  CreateMappingResponse,
  ModifyMappingRequest,
  ListRelationshipsResponse,
  ShowRelationshipResponse,
  CreateRelationshipRequest,
  CreateRelationshipResponse,
  ModifyRelationshipRequest,
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
  ListBranchesResponse,
  ShowBranchResponse,
  CreateBranchRequest,
  CreateBranchResponse,
  ModifyBranchRequest,
  AttachBranchRequest,
  AttachBranchResponse,
  DetachBranchResponse,
  ListCommitsResponse,
  ShowCommitResponse,
  BranchCommitRequest,
  BranchCommitResponse,
  MergeCommitResponse,
  DeployCommitResponse,
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

  modify: (workspaceName: string, databaseName: string, request: ModifyDatabaseRequest) =>
    apiClient.put<{ success: boolean; message: string; database: any }>(
      `api/v1/workspaces/${workspaceName}/databases/${databaseName}`,
      request
    ),

  disconnect: (workspaceName: string, databaseName: string, request?: DisconnectDatabaseRequest) =>
    apiClient.post<{ success: boolean; message: string }>(
      `api/v1/workspaces/${workspaceName}/databases/${databaseName}/disconnect`,
      request
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
    apiClient.put<{ success: boolean; message: string; instance: any }>(
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

  createDatabaseMapping: (workspaceName: string, request: CreateDatabaseMappingRequest) =>
    apiClient.post<CreateMappingResponse>(`api/v1/workspaces/${workspaceName}/mappings/database`, request),

  modify: (workspaceName: string, mappingId: string, request: ModifyMappingRequest) =>
    apiClient.put<{ success: boolean; message: string; mapping: any }>(
      `api/v1/workspaces/${workspaceName}/mappings/${mappingId}`,
      request
    ),

  delete: (workspaceName: string, mappingId: string) =>
    apiClient.delete<{ success: boolean; message: string }>(
      `api/v1/workspaces/${workspaceName}/mappings/${mappingId}`
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
    apiClient.put<{ success: boolean; message: string; relationship: any }>(
      `api/v1/workspaces/${workspaceName}/relationships/${relationshipId}`,
      request
    ),

  delete: (workspaceName: string, relationshipId: string) =>
    apiClient.delete<{ success: boolean; message: string }>(
      `api/v1/workspaces/${workspaceName}/relationships/${relationshipId}`
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
    apiClient.put<{ success: boolean; message: string; repository: any }>(
      `api/v1/workspaces/${workspaceName}/repos/${repoName}`,
      request
    ),

  delete: (workspaceName: string, repoName: string, request?: DeleteRepositoryRequest) =>
    apiClient.delete<{ success: boolean; message: string }>(
      `api/v1/workspaces/${workspaceName}/repos/${repoName}`,
      { body: JSON.stringify(request) } as any
    ),
};

// Branch Endpoints
export const branchEndpoints = {
  show: (workspaceName: string, repoName: string, branchName: string) =>
    apiClient.get<ShowBranchResponse>(
      `api/v1/workspaces/${workspaceName}/repos/${repoName}/branches/${branchName}`
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
    apiClient.put<{ success: boolean; message: string; environment: any }>(
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
    apiClient.put<{ success: boolean; message: string; region: any }>(
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
    apiClient.put<{ success: boolean; message: string; rule: any }>(
      `api/v1/workspaces/${workspaceName}/mappings/${mappingName}/rules/${ruleName}`,
      request
    ),

  remove: (workspaceName: string, mappingName: string, ruleName: string, request?: RemoveMappingRuleRequest) =>
    apiClient.delete<{ success: boolean; message: string }>(
      `api/v1/workspaces/${workspaceName}/mappings/${mappingName}/rules/${ruleName}`,
      { body: JSON.stringify(request) } as any
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
    apiClient.put<{ success: boolean; message: string; user: any }>(
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
  environments: environmentEndpoints,
  mesh: meshEndpoints,
  regions: regionEndpoints,
  transformations: transformationEndpoints,
  mappingRules: mappingRuleEndpoints,
  mcpTools: mcpToolEndpoints,
  mcpResources: mcpResourceEndpoints,
  users: userEndpoints,
};

