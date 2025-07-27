# Mesh API Endpoints

This document describes the REST API endpoints for mesh management in the Client API service.

## Base URL

All mesh endpoints are tenant-level and follow this pattern:
```
/{tenant_url}/api/v1/meshes
```

## Authentication

All endpoints require authentication via Bearer token in the Authorization header:
```
Authorization: Bearer <token>
```

## Endpoints

### 1. Seed Mesh

**POST** `/{tenant_url}/api/v1/meshes/seed`

Creates a new mesh and seeds the first node.

#### Request Body
```json
{
  "mesh_name": "production-mesh",
  "mesh_description": "Production mesh network",
  "node_name": "seed-node",
  "node_description": "Initial seed node",
  "allow_join": true,
  "join_key": "secure-join-key-123"
}
```

#### Response
```json
{
  "message": "Mesh seeded successfully",
  "success": true,
  "mesh": {
    "mesh_id": "mesh_01HGQK8F3VWXYZ123456789ABC",
    "mesh_name": "production-mesh",
    "mesh_description": "Production mesh network",
    "public_key": "ssh-rsa AAAAB3NzaC1yc2E...",
    "allow_join": true,
    "node_count": 1,
    "status": "healthy"
  },
  "status": "success"
}
```

### 2. Join Mesh

**POST** `/{tenant_url}/api/v1/meshes/{mesh_id}/join`

Joins an existing mesh with a new node.

#### Path Parameters
- `mesh_id` (string, required): The mesh ID

#### Request Body
```json
{
  "node_name": "worker-node-1",
  "node_description": "Worker node in production mesh",
  "join_key": "secure-join-key-123"
}
```

### 3. Show Mesh

**GET** `/{tenant_url}/api/v1/meshes/{mesh_id}`

Shows details of a specific mesh.

### 4. List Nodes

**GET** `/{tenant_url}/api/v1/meshes/{mesh_id}/nodes`

Lists all nodes in a mesh.

### 5. Show Node

**GET** `/{tenant_url}/api/v1/meshes/{mesh_id}/nodes/{node_id}`

Shows details of a specific node in a mesh.

### 6. Modify Mesh

**PUT** `/{tenant_url}/api/v1/meshes/{mesh_id}`

Updates mesh properties.

### 7. Modify Node

**PUT** `/{tenant_url}/api/v1/meshes/{mesh_id}/nodes/{node_id}`

Updates node properties.

### 8. Evict Node

**POST** `/{tenant_url}/api/v1/meshes/{mesh_id}/nodes/{node_id}/evict`

Evicts a node from the mesh.

### 9. Show Topology

**GET** `/{tenant_url}/api/v1/meshes/{mesh_id}/topology`

Shows the mesh topology (routes between nodes).

### 10. Add Mesh Route

**POST** `/{tenant_url}/api/v1/meshes/{mesh_id}/routes`

Adds a route between two nodes in the mesh.

#### Request Body
```json
{
  "source_node_id": "node_01HGQK8F3VWXYZ123456789ABC",
  "target_node_id": "node_02HGQK8F3VWXYZ123456789ABC",
  "route_bidirectional": true,
  "route_latency": 10.5,
  "route_bandwidth": 1000.0,
  "route_cost": 10
}
```

### 11. Modify Mesh Route

**PUT** `/{tenant_url}/api/v1/meshes/{mesh_id}/routes/{source_node_id}/{target_node_id}`

Updates properties of an existing route.

### 12. Delete Mesh Route

**DELETE** `/{tenant_url}/api/v1/meshes/{mesh_id}/routes/{source_node_id}/{target_node_id}`

Deletes a route between two nodes.

### 13. Leave Mesh

**POST** `/{tenant_url}/api/v1/meshes/{mesh_id}/leave`

Removes a node from the mesh.

#### Request Body
```json
{
  "node_id": "node_01HGQK8F3VWXYZ123456789ABC"
}
```

## Error Handling

All endpoints return appropriate HTTP status codes and follow the standard error response format. 