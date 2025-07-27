package engine

import "context"

// Helper functions to get counts for related entities
func (s *Server) getInstanceCount(ctx context.Context, tenantId, workspaceId string) (int32, error) {
	query := "SELECT COUNT(*) FROM instances WHERE tenant_id = $1 AND workspace_id = $2"
	var count int32
	err := s.engine.db.Pool().QueryRow(ctx, query, tenantId, workspaceId).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *Server) getDatabaseCount(ctx context.Context, tenantId, workspaceId string) (int32, error) {
	query := "SELECT COUNT(*) FROM databases WHERE tenant_id = $1 AND workspace_id = $2"
	var count int32
	err := s.engine.db.Pool().QueryRow(ctx, query, tenantId, workspaceId).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *Server) getRepoCount(ctx context.Context, tenantId, workspaceId string) (int32, error) {
	query := "SELECT COUNT(*) FROM repos WHERE tenant_id = $1 AND workspace_id = $2"
	var count int32
	err := s.engine.db.Pool().QueryRow(ctx, query, tenantId, workspaceId).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *Server) getMappingCount(ctx context.Context, tenantId, workspaceId string) (int32, error) {
	query := "SELECT COUNT(*) FROM mappings WHERE tenant_id = $1 AND workspace_id = $2"
	var count int32
	err := s.engine.db.Pool().QueryRow(ctx, query, tenantId, workspaceId).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *Server) getRelationshipCount(ctx context.Context, tenantId, workspaceId string) (int32, error) {
	query := "SELECT COUNT(*) FROM relationships WHERE tenant_id = $1 AND workspace_id = $2"
	var count int32
	err := s.engine.db.Pool().QueryRow(ctx, query, tenantId, workspaceId).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *Server) isDatabaseExists(ctx context.Context, tenantId, workspaceId, databaseId string) (bool, error) {
	query := "SELECT EXISTS(SELECT 1 FROM databases WHERE tenant_id = $1 AND workspace_id = $2 AND database_id = $3)"
	var exists bool
	err := s.engine.db.Pool().QueryRow(ctx, query, tenantId, workspaceId, databaseId).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}
