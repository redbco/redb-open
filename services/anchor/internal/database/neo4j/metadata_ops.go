package neo4j

import (
	"context"
	"fmt"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

type MetadataOps struct {
	conn *Connection
}

func (m *MetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata, err := CollectDatabaseMetadata(ctx, m.conn.driver)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Neo4j, "collect_database_metadata", err)
	}
	return metadata, nil
}

func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata, err := CollectInstanceMetadata(ctx, m.conn.driver)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Neo4j, "collect_instance_metadata", err)
	}
	return metadata, nil
}

func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	session := m.conn.driver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeRead})
	defer session.Close(ctx)

	result, err := session.Run(ctx, "CALL dbms.components() YIELD name, versions, edition UNWIND versions AS version RETURN version LIMIT 1", nil)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.Neo4j, "get_version", err)
	}

	if result.Next(ctx) {
		record := result.Record()
		if version, ok := record.Get("version"); ok {
			return version.(string), nil
		}
	}
	return "", adapter.NewDatabaseError(dbcapabilities.Neo4j, "get_version", fmt.Errorf("version not found"))
}

func (m *MetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	session := m.conn.driver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeRead})
	defer session.Close(ctx)

	result, err := session.Run(ctx, "CALL dbms.cluster.overview() YIELD id RETURN id LIMIT 1", nil)
	if err != nil {
		// If not in cluster mode, return database name
		return m.conn.config.DatabaseName, nil
	}

	if result.Next(ctx) {
		record := result.Record()
		if id, ok := record.Get("id"); ok {
			return id.(string), nil
		}
	}
	return m.conn.config.DatabaseName, nil
}

func (m *MetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	session := m.conn.driver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeRead})
	defer session.Close(ctx)

	result, err := session.Run(ctx, "CALL dbms.queryJmx('org.neo4j:instance=kernel#0,name=Store file sizes') YIELD attributes RETURN attributes.TotalStoreSize.value AS size", nil)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.Neo4j, "get_database_size", err)
	}

	if result.Next(ctx) {
		record := result.Record()
		if size, ok := record.Get("size"); ok {
			if sizeInt, ok := size.(int64); ok {
				return sizeInt, nil
			}
		}
	}
	return 0, nil
}

func (m *MetadataOps) GetTableCount(ctx context.Context) (int, error) {
	// In Neo4j, count node labels
	session := m.conn.driver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeRead})
	defer session.Close(ctx)

	result, err := session.Run(ctx, "CALL db.labels() YIELD label RETURN count(label) AS count", nil)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.Neo4j, "get_table_count", err)
	}

	if result.Next(ctx) {
		record := result.Record()
		if count, ok := record.Get("count"); ok {
			if countInt, ok := count.(int64); ok {
				return int(countInt), nil
			}
		}
	}
	return 0, nil
}

func (m *MetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	session := m.conn.driver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeWrite})
	defer session.Close(ctx)

	_, err := session.Run(ctx, command, nil)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Neo4j, "execute_command", err)
	}

	result := fmt.Sprintf(`{"success": true, "command": "%s"}`, command)
	return []byte(result), nil
}

type InstanceMetadataOps struct {
	conn *InstanceConnection
}

func (i *InstanceMetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(dbcapabilities.Neo4j, "collect database metadata", "not available on instance connections")
}

func (i *InstanceMetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata, err := CollectInstanceMetadata(ctx, i.conn.driver)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Neo4j, "collect_instance_metadata", err)
	}
	return metadata, nil
}

func (i *InstanceMetadataOps) GetVersion(ctx context.Context) (string, error) {
	session := i.conn.driver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeRead})
	defer session.Close(ctx)

	result, err := session.Run(ctx, "CALL dbms.components() YIELD name, versions, edition UNWIND versions AS version RETURN version LIMIT 1", nil)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.Neo4j, "get_version", err)
	}

	if result.Next(ctx) {
		record := result.Record()
		if version, ok := record.Get("version"); ok {
			return version.(string), nil
		}
	}
	return "", nil
}

func (i *InstanceMetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	session := i.conn.driver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeRead})
	defer session.Close(ctx)

	result, err := session.Run(ctx, "CALL dbms.cluster.overview() YIELD id RETURN id LIMIT 1", nil)
	if err != nil {
		return i.conn.config.UniqueIdentifier, nil
	}

	if result.Next(ctx) {
		record := result.Record()
		if id, ok := record.Get("id"); ok {
			return id.(string), nil
		}
	}
	return i.conn.config.UniqueIdentifier, nil
}

func (i *InstanceMetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.Neo4j, "get database size", "not available on instance connections")
}

func (i *InstanceMetadataOps) GetTableCount(ctx context.Context) (int, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.Neo4j, "get table count", "not available on instance connections")
}

func (i *InstanceMetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	session := i.conn.driver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeWrite})
	defer session.Close(ctx)

	_, err := session.Run(ctx, command, nil)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Neo4j, "execute_command", err)
	}

	result := fmt.Sprintf(`{"success": true, "command": "%s"}`, command)
	return []byte(result), nil
}
