# New Mesh Management API Endpoints

This document describes the new REST API endpoints for mesh management in the Client API service.

## Base URL

All mesh endpoints are global and follow this pattern:
```
/api/v1/mesh
/api/v1/node
```

## Authentication

All endpoints require authentication via Bearer token in the Authorization header:
```
Authorization: Bearer <token>
```

## Core Mesh Operations

### 1. Seed Mesh

**POST** `/api/v1/mesh/seed`

Creates a new mesh without connecting to any other nodes. The local node becomes the first member.

#### Request Body
```json
{
  "mesh_name": "production-mesh",
  "mesh_description": "Production mesh network"
}
```

#### Response
```json
{
  "message": "Successfully seeded mesh 'production-mesh' with local node 'node-1'",
  "success": true,
  "mesh": {
    "mesh_id": "mesh_01HGQK8F3VWXYZ123456789ABC",
    "mesh_name": "production-mesh",
    "mesh_description": "Production mesh network",
    "allow_join": true,
    "node_count": 1,
    "connection_count": 0,
    "status": "created",
    "created_at": 1703123456,
    "updated_at": 1703123456
  },
  "status": "created"
}
```

### 2. Join Mesh

**POST** `/api/v1/mesh/join`

Joins an existing mesh by connecting to a node in that mesh. The local node must be "clean" (not part of any mesh).

#### Request Body
```json
{
  "target_address": "192.168.1.100:8443",
  "strategy": "inherit",
  "timeout_seconds": 30
}
```

#### Strategy Options
- `inherit` (default): Inherit configuration from the mesh
- `merge`: Merge local and mesh configurations
- `overwrite`: Overwrite mesh configuration with local config

#### Response
```json
{
  "message": "Successfully joined mesh via peer node 12345",
  "success": true,
  "mesh": {
    "mesh_id": "mesh_01HGQK8F3VWXYZ123456789ABC",
    "mesh_name": "production-mesh",
    "mesh_description": "Production mesh network",
    "allow_join": true,
    "node_count": 2,
    "connection_count": 1,
    "status": "active",
    "created_at": 1703123456,
    "updated_at": 1703123456
  },
  "peer_node_id": 12345,
  "remote_addr": "192.168.1.100:8443",
  "status": "created"
}
```

### 3. Extend Mesh

**POST** `/api/v1/mesh/extend`

Extends the current mesh to a clean node. The local node must be part of a mesh.

#### Request Body
```json
{
  "target_address": "192.168.1.200:8443",
  "strategy": "inherit",
  "timeout_seconds": 30
}
```

#### Response
```json
{
  "message": "Successfully extended mesh to peer node 67890",
  "success": true,
  "peer_node_id": 67890,
  "remote_addr": "192.168.1.200:8443",
  "status": "created"
}
```

### 4. Leave Mesh

**POST** `/api/v1/mesh/leave`

Removes the current node from its mesh gracefully.

#### Request Body
```json
{
  "force": false
}
```

#### Response
```json
{
  "message": "Successfully left mesh, dropped 2 connections",
  "success": true,
  "connections_dropped": 2,
  "status": "success"
}
```

### 5. Evict Node

**POST** `/api/v1/mesh/evict`

Forcefully removes another node from the mesh.

#### Request Body
```json
{
  "target_node_id": 12345,
  "clean_target": true
}
```

#### Response
```json
{
  "message": "Successfully evicted node 12345 from mesh",
  "success": true,
  "target_cleaned": true,
  "status": "success"
}
```

## Connection Management

### 6. Add Connection

**POST** `/api/v1/mesh/connections`

Adds a connection to another node in the same mesh.

#### Request Body
```json
{
  "target_node_id": 12345,
  "timeout_seconds": 30
}
```

#### Response
```json
{
  "message": "Successfully connected to node 12345",
  "success": true,
  "connection": {
    "peer_node_id": 12345,
    "peer_node_name": "worker-node-1",
    "remote_addr": "192.168.1.100:8443",
    "status": "connected",
    "rtt_microseconds": 1500,
    "bytes_sent": 1024,
    "bytes_received": 2048,
    "is_tls": true,
    "connected_at": 1703123456
  },
  "status": "created"
}
```

### 7. Drop Connection

**DELETE** `/api/v1/mesh/connections/{peer_node_id}`

Drops a connection to another node.

#### Response
```json
{
  "message": "Successfully dropped connection to node 12345",
  "success": true,
  "status": "success"
}
```

### 8. List Connections

**GET** `/api/v1/mesh/connections`

Lists all active connections for the current node.

#### Response
```json
{
  "connections": [
    {
      "peer_node_id": 12345,
      "peer_node_name": "worker-node-1",
      "remote_addr": "192.168.1.100:8443",
      "status": "connected",
      "rtt_microseconds": 1500,
      "bytes_sent": 1024,
      "bytes_received": 2048,
      "is_tls": true,
      "connected_at": 1703123456
    }
  ]
}
```

## Information and Status

### 9. Show Mesh

**GET** `/api/v1/mesh`

Shows details of the current mesh (if node is part of one).

#### Response
```json
{
  "mesh": {
    "mesh_id": "mesh_01HGQK8F3VWXYZ123456789ABC",
    "mesh_name": "production-mesh",
    "mesh_description": "Production mesh network",
    "allow_join": true,
    "node_count": 3,
    "connection_count": 4,
    "status": "active",
    "created_at": 1703123456,
    "updated_at": 1703123456
  }
}
```

### 10. List Nodes

**GET** `/api/v1/mesh/nodes`

Lists all nodes in the current mesh.

#### Response
```json
{
  "nodes": [
    {
      "node_id": 12345,
      "node_name": "seed-node",
      "node_description": "Initial seed node",
      "node_platform": "reDB",
      "node_version": "1.0.0",
      "region_id": "us-east-1",
      "region_name": "US East 1",
      "ip_address": "192.168.1.100",
      "port": 8443,
      "node_status": "active",
      "mesh_id": "mesh_01HGQK8F3VWXYZ123456789ABC",
      "created_at": 1703123456,
      "updated_at": 1703123456
    }
  ]
}
```

### 11. Show Node

**GET** `/api/v1/mesh/nodes/{node_id}` or **GET** `/api/v1/mesh/nodes`

Shows details of a specific node or the current node (if no ID provided).

#### Response
```json
{
  "node": {
    "node_id": 12345,
    "node_name": "seed-node",
    "node_description": "Initial seed node",
    "node_platform": "reDB",
    "node_version": "1.0.0",
    "region_id": "us-east-1",
    "region_name": "US East 1",
    "ip_address": "192.168.1.100",
    "port": 8443,
    "node_status": "active",
    "mesh_id": "mesh_01HGQK8F3VWXYZ123456789ABC",
    "created_at": 1703123456,
    "updated_at": 1703123456
  }
}
```

### 12. Get Node Status

**GET** `/api/v1/node/status`

Gets comprehensive status information for the current node.

#### Response
```json
{
  "node": {
    "node_id": 12345,
    "node_name": "seed-node",
    "node_description": "Initial seed node",
    "node_platform": "reDB",
    "node_version": "1.0.0",
    "region_id": "us-east-1",
    "region_name": "US East 1",
    "ip_address": "192.168.1.100",
    "port": 8443,
    "node_status": "active",
    "mesh_id": "mesh_01HGQK8F3VWXYZ123456789ABC",
    "created_at": 1703123456,
    "updated_at": 1703123456
  },
  "connections": [
    {
      "peer_node_id": 67890,
      "peer_node_name": "worker-node-1",
      "remote_addr": "192.168.1.200:8443",
      "status": "connected",
      "rtt_microseconds": 1500,
      "bytes_sent": 1024,
      "bytes_received": 2048,
      "is_tls": true,
      "connected_at": 1703123456
    }
  ],
  "mesh": {
    "mesh_id": "mesh_01HGQK8F3VWXYZ123456789ABC",
    "mesh_name": "production-mesh",
    "mesh_description": "Production mesh network",
    "allow_join": true,
    "node_count": 3,
    "connection_count": 4,
    "status": "active",
    "created_at": 1703123456,
    "updated_at": 1703123456
  }
}
```

## Node Status Values

- `clean`: Node has identity but no mesh membership
- `joining`: Node is joining a mesh
- `active`: Node is active in mesh
- `leaving`: Node is leaving mesh
- `offline`: Node is offline but still in mesh

## Connection Status Values

- `connecting`: Connection is being established
- `connected`: Connection is active
- `disconnecting`: Connection is being terminated
- `failed`: Connection failed

## Error Handling

All endpoints return appropriate HTTP status codes and follow the standard error response format:

```json
{
  "error": "Error message",
  "message": "Detailed error description",
  "status": "error"
}
```

### Common HTTP Status Codes

- `200 OK`: Successful operation
- `201 Created`: Resource created successfully
- `400 Bad Request`: Invalid request parameters
- `401 Unauthorized`: Authentication required
- `403 Forbidden`: Permission denied
- `404 Not Found`: Resource not found
- `409 Conflict`: Resource already exists
- `412 Precondition Failed`: Operation precondition not met (e.g., node already in mesh)
- `500 Internal Server Error`: Server error
- `503 Service Unavailable`: Service temporarily unavailable
- `504 Gateway Timeout`: Operation timed out
