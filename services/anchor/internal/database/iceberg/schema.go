package iceberg

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// DiscoverSchema fetches the current schema of an Apache Iceberg catalog and returns a UnifiedModel
func DiscoverSchema(db interface{}) (*unifiedmodel.UnifiedModel, error) {
	client, ok := db.(*IcebergClient)
	if !ok {
		return nil, fmt.Errorf("invalid database connection type")
	}

	// Create the unified model
	um := &unifiedmodel.UnifiedModel{
		DatabaseType:   dbcapabilities.Iceberg,
		Tables:         make(map[string]unifiedmodel.Table),
		Namespaces:     make(map[string]unifiedmodel.Namespace),
		Views:          make(map[string]unifiedmodel.View),
		ExternalTables: make(map[string]unifiedmodel.ExternalTable),
		Snapshots:      make(map[string]unifiedmodel.Snapshot),
	}

	var err error

	// Discover namespaces (equivalent to schemas/databases)
	namespaces, err := discoverNamespaces(client)
	if err != nil {
		return nil, fmt.Errorf("error discovering namespaces: %v", err)
	}

	// Convert namespaces to unified model
	for _, namespace := range namespaces {
		um.Namespaces[namespace.Name] = ConvertIcebergNamespace(namespace)
	}

	// Discover tables across all namespaces
	tables, err := discoverTables(client)
	if err != nil {
		return nil, fmt.Errorf("error discovering tables: %v", err)
	}

	// Convert tables to unified model (Iceberg tables are external tables)
	for _, table := range tables {
		// Create external table for Iceberg
		um.ExternalTables[table.Name] = unifiedmodel.ExternalTable{
			Name:     table.Name,
			Location: table.Schema, // Use schema as location reference
		}

		// Also create regular table entry for compatibility
		convertedTable := unifiedmodel.Table{
			Name:        table.Name,
			Comment:     table.Schema,
			Columns:     make(map[string]unifiedmodel.Column),
			Indexes:     make(map[string]unifiedmodel.Index),
			Constraints: make(map[string]unifiedmodel.Constraint),
		}

		// Convert columns
		for _, col := range table.Columns {
			var defaultValue string
			if col.ColumnDefault != nil {
				defaultValue = *col.ColumnDefault
			}
			convertedTable.Columns[col.Name] = unifiedmodel.Column{
				Name:         col.Name,
				DataType:     col.DataType,
				Nullable:     col.IsNullable,
				Default:      defaultValue,
				IsPrimaryKey: col.IsPrimaryKey,
			}
		}

		um.Tables[table.Name] = convertedTable
	}

	// Discover views (if supported by the catalog)
	views, err := discoverViews(client)
	if err != nil {
		return nil, fmt.Errorf("error discovering views: %v", err)
	}

	// Convert views to unified model
	for _, view := range views {
		um.Views[view.Name] = unifiedmodel.View{
			Name:       view.Name,
			Definition: view.Definition,
		}
	}

	return um, nil
}

// CreateStructure creates database objects based on the provided parameters
func CreateStructure(db interface{}, params common.StructureParams) error {
	client, ok := db.(*IcebergClient)
	if !ok {
		return fmt.Errorf("invalid database connection type")
	}

	// Create namespaces first
	for _, schema := range params.Schemas {
		if err := createNamespace(client, schema.Name); err != nil {
			return fmt.Errorf("error creating namespace %s: %v", schema.Name, err)
		}
	}

	// Create tables
	for _, table := range params.Tables {
		if err := createTable(client, table); err != nil {
			return fmt.Errorf("error creating table %s.%s: %v", table.Schema, table.Name, err)
		}
	}

	// Create views (if supported)
	for _, view := range params.Views {
		if err := createView(client, view); err != nil {
			return fmt.Errorf("error creating view %s.%s: %v", view.Schema, view.Name, err)
		}
	}

	return nil
}

// discoverNamespaces discovers all namespaces in the Iceberg catalog
func discoverNamespaces(client *IcebergClient) ([]IcebergNamespaceInfo, error) {
	switch client.CatalogType {
	case "rest":
		return discoverNamespacesREST(client)
	case "hive":
		return discoverNamespacesHive(client)
	case "hadoop":
		return discoverNamespacesHadoop(client)
	default:
		return nil, fmt.Errorf("unsupported catalog type: %s", client.CatalogType)
	}
}

// discoverNamespacesREST discovers namespaces using REST catalog API
func discoverNamespacesREST(client *IcebergClient) ([]IcebergNamespaceInfo, error) {
	if client.HTTPClient == nil {
		return nil, fmt.Errorf("HTTP client not initialized")
	}

	httpClient, ok := client.HTTPClient.(*http.Client)
	if !ok {
		return nil, fmt.Errorf("invalid HTTP client type")
	}

	// Call REST API to list namespaces
	namespacesURL := fmt.Sprintf("%s/v1/namespaces", strings.TrimSuffix(client.BaseURL, "/"))

	req, err := http.NewRequest("GET", namespacesURL, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("REST API returned status %d", resp.StatusCode)
	}

	var response struct {
		Namespaces [][]string `json:"namespaces"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("error decoding response: %v", err)
	}

	// Convert to IcebergNamespaceInfo
	namespaces := make([]IcebergNamespaceInfo, len(response.Namespaces))
	for i, ns := range response.Namespaces {
		namespaces[i] = IcebergNamespaceInfo{
			Name: strings.Join(ns, "."),
		}
	}

	return namespaces, nil
}

// discoverNamespacesHive discovers namespaces using Hive metastore
func discoverNamespacesHive(client *IcebergClient) ([]IcebergNamespaceInfo, error) {
	// In a real implementation, you'd use Hive metastore client to list databases
	// For now, return a placeholder
	return []IcebergNamespaceInfo{
		{Name: "default", Description: "Default namespace"},
	}, nil
}

// discoverNamespacesHadoop discovers namespaces using Hadoop catalog
func discoverNamespacesHadoop(client *IcebergClient) ([]IcebergNamespaceInfo, error) {
	// In a real implementation, you'd list directories in the warehouse path
	// For now, return a placeholder
	return []IcebergNamespaceInfo{
		{Name: "default", Description: "Default namespace"},
	}, nil
}

// discoverTables discovers all tables in the Iceberg catalog
func discoverTables(client *IcebergClient) ([]common.TableInfo, error) {
	switch client.CatalogType {
	case "rest":
		return discoverTablesREST(client)
	case "hive":
		return discoverTablesHive(client)
	case "hadoop":
		return discoverTablesHadoop(client)
	default:
		return nil, fmt.Errorf("unsupported catalog type: %s", client.CatalogType)
	}
}

// discoverTablesREST discovers tables using REST catalog API
func discoverTablesREST(client *IcebergClient) ([]common.TableInfo, error) {
	if client.HTTPClient == nil {
		return nil, fmt.Errorf("HTTP client not initialized")
	}

	httpClient, ok := client.HTTPClient.(*http.Client)
	if !ok {
		return nil, fmt.Errorf("invalid HTTP client type")
	}

	// First get all namespaces
	namespaces, err := discoverNamespacesREST(client)
	if err != nil {
		return nil, fmt.Errorf("error discovering namespaces: %v", err)
	}

	var allTables []common.TableInfo

	// For each namespace, get its tables
	for _, namespace := range namespaces {
		tablesURL := fmt.Sprintf("%s/v1/namespaces/%s/tables",
			strings.TrimSuffix(client.BaseURL, "/"),
			strings.ReplaceAll(namespace.Name, ".", "%2E"))

		req, err := http.NewRequest("GET", tablesURL, nil)
		if err != nil {
			continue // Skip this namespace on error
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			continue // Skip this namespace on error
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			continue // Skip this namespace on error
		}

		var response struct {
			Identifiers []struct {
				Namespace []string `json:"namespace"`
				Name      string   `json:"name"`
			} `json:"identifiers"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
			resp.Body.Close()
			continue // Skip this namespace on error
		}
		resp.Body.Close()

		// Convert to TableInfo and get detailed schema
		for _, identifier := range response.Identifiers {
			tableInfo, err := getTableSchemaREST(client, namespace.Name, identifier.Name)
			if err != nil {
				// Create basic table info if detailed schema fails
				tableInfo = &common.TableInfo{
					Schema:    namespace.Name,
					Name:      identifier.Name,
					TableType: "BASE TABLE",
					Columns:   []common.ColumnInfo{},
				}
			}
			allTables = append(allTables, *tableInfo)
		}
	}

	return allTables, nil
}

// getTableSchemaREST gets detailed schema for a specific table using REST API
func getTableSchemaREST(client *IcebergClient, namespace, tableName string) (*common.TableInfo, error) {
	if client.HTTPClient == nil {
		return nil, fmt.Errorf("HTTP client not initialized")
	}

	httpClient, ok := client.HTTPClient.(*http.Client)
	if !ok {
		return nil, fmt.Errorf("invalid HTTP client type")
	}

	// Get table metadata
	tableURL := fmt.Sprintf("%s/v1/namespaces/%s/tables/%s",
		strings.TrimSuffix(client.BaseURL, "/"),
		strings.ReplaceAll(namespace, ".", "%2E"),
		tableName)

	req, err := http.NewRequest("GET", tableURL, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("REST API returned status %d", resp.StatusCode)
	}

	var tableMetadata struct {
		Metadata IcebergTableMetadata `json:"metadata"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&tableMetadata); err != nil {
		return nil, fmt.Errorf("error decoding response: %v", err)
	}

	// Convert Iceberg schema to common.TableInfo
	return convertIcebergTableToCommon(namespace, tableName, &tableMetadata.Metadata), nil
}

// convertIcebergTableToCommon converts Iceberg table metadata to common.TableInfo
func convertIcebergTableToCommon(namespace, tableName string, metadata *IcebergTableMetadata) *common.TableInfo {
	tableInfo := &common.TableInfo{
		Schema:    namespace,
		Name:      tableName,
		TableType: "BASE TABLE",
		Columns:   []common.ColumnInfo{},
	}

	// Find current schema
	var currentSchema *IcebergSchemaDefinition
	for _, schema := range metadata.Schemas {
		if schema.SchemaID == metadata.CurrentSchemaID {
			currentSchema = &schema
			break
		}
	}

	if currentSchema != nil {
		// Convert fields to columns
		for _, field := range currentSchema.Fields {
			column := common.ColumnInfo{
				Name:         field.Name,
				DataType:     convertIcebergTypeToSQL(field.Type),
				IsNullable:   !field.Required,
				IsPrimaryKey: false, // Would need to check identifier fields
			}
			tableInfo.Columns = append(tableInfo.Columns, column)
		}

		// Set primary key from identifier fields
		if len(currentSchema.IdentifierFieldIDs) > 0 {
			for _, fieldID := range currentSchema.IdentifierFieldIDs {
				for i, field := range currentSchema.Fields {
					if field.ID == fieldID {
						tableInfo.Columns[i].IsPrimaryKey = true
						tableInfo.PrimaryKey = append(tableInfo.PrimaryKey, field.Name)
						break
					}
				}
			}
		}
	}

	return tableInfo
}

// convertIcebergTypeToSQL converts Iceberg type to SQL type string
func convertIcebergTypeToSQL(icebergType string) string {
	// Basic type mapping - in practice, this would be more comprehensive
	switch {
	case icebergType == "boolean":
		return "BOOLEAN"
	case icebergType == "int":
		return "INTEGER"
	case icebergType == "long":
		return "BIGINT"
	case icebergType == "float":
		return "REAL"
	case icebergType == "double":
		return "DOUBLE PRECISION"
	case icebergType == "string":
		return "VARCHAR"
	case icebergType == "binary":
		return "BYTEA"
	case icebergType == "date":
		return "DATE"
	case icebergType == "time":
		return "TIME"
	case icebergType == "timestamp":
		return "TIMESTAMP"
	case icebergType == "timestamptz":
		return "TIMESTAMP WITH TIME ZONE"
	case strings.HasPrefix(icebergType, "decimal("):
		return strings.ToUpper(icebergType)
	case strings.HasPrefix(icebergType, "fixed("):
		return "BYTEA"
	case strings.HasPrefix(icebergType, "list<"):
		return "ARRAY"
	case strings.HasPrefix(icebergType, "map<"):
		return "JSON"
	case strings.HasPrefix(icebergType, "struct<"):
		return "JSON"
	default:
		return "VARCHAR" // Default fallback
	}
}

// discoverTablesHive discovers tables using Hive metastore
func discoverTablesHive(client *IcebergClient) ([]common.TableInfo, error) {
	// In a real implementation, you'd use Hive metastore client
	return []common.TableInfo{}, nil
}

// discoverTablesHadoop discovers tables using Hadoop catalog
func discoverTablesHadoop(client *IcebergClient) ([]common.TableInfo, error) {
	// In a real implementation, you'd scan the warehouse directory structure
	return []common.TableInfo{}, nil
}

// discoverViews discovers views in the Iceberg catalog
func discoverViews(client *IcebergClient) ([]common.ViewInfo, error) {
	// Iceberg views are not widely supported yet, return empty slice
	return []common.ViewInfo{}, nil
}

// createNamespace creates a new namespace in the catalog
func createNamespace(client *IcebergClient, namespaceName string) error {
	switch client.CatalogType {
	case "rest":
		return createNamespaceRESTForSchema(client, namespaceName)
	case "hive":
		return createNamespaceHiveForSchema(client, namespaceName)
	case "hadoop":
		return createNamespaceHadoopForSchema(client, namespaceName)
	default:
		return fmt.Errorf("unsupported catalog type: %s", client.CatalogType)
	}
}

// createNamespaceRESTForSchema creates a namespace using REST API (schema version)
func createNamespaceRESTForSchema(client *IcebergClient, namespaceName string) error {
	if client.HTTPClient == nil {
		return fmt.Errorf("HTTP client not initialized")
	}

	httpClient, ok := client.HTTPClient.(*http.Client)
	if !ok {
		return fmt.Errorf("invalid HTTP client type")
	}

	// Prepare request body
	requestBody := map[string]interface{}{
		"namespace": strings.Split(namespaceName, "."),
		"properties": map[string]string{
			"description": fmt.Sprintf("Namespace %s", namespaceName),
		},
	}

	bodyBytes, err := json.Marshal(requestBody)
	if err != nil {
		return fmt.Errorf("error marshaling request: %v", err)
	}

	// Create namespace
	namespacesURL := fmt.Sprintf("%s/v1/namespaces", strings.TrimSuffix(client.BaseURL, "/"))

	req, err := http.NewRequest("POST", namespacesURL, strings.NewReader(string(bodyBytes)))
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error making request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("REST API returned status %d", resp.StatusCode)
	}

	return nil
}

// createNamespaceHiveForSchema creates a namespace using Hive metastore (schema version)
func createNamespaceHiveForSchema(client *IcebergClient, namespaceName string) error {
	// In a real implementation, you'd use Hive metastore client
	return fmt.Errorf("Hive namespace creation not implemented")
}

// createNamespaceHadoopForSchema creates a namespace using Hadoop catalog (schema version)
func createNamespaceHadoopForSchema(client *IcebergClient, namespaceName string) error {
	// In a real implementation, you'd create directory structure
	return fmt.Errorf("Hadoop namespace creation not implemented")
}

// createTable creates a new table in the catalog
func createTable(client *IcebergClient, tableInfo common.TableInfo) error {
	switch client.CatalogType {
	case "rest":
		return createTableREST(client, tableInfo)
	case "hive":
		return createTableHive(client, tableInfo)
	case "hadoop":
		return createTableHadoop(client, tableInfo)
	default:
		return fmt.Errorf("unsupported catalog type: %s", client.CatalogType)
	}
}

// createTableREST creates a table using REST API
func createTableREST(client *IcebergClient, tableInfo common.TableInfo) error {
	if client.HTTPClient == nil {
		return fmt.Errorf("HTTP client not initialized")
	}

	httpClient, ok := client.HTTPClient.(*http.Client)
	if !ok {
		return fmt.Errorf("invalid HTTP client type")
	}

	// Convert common.TableInfo to Iceberg table schema
	schema := convertCommonTableToIceberg(tableInfo)

	// Prepare request body
	requestBody := map[string]interface{}{
		"name":           tableInfo.Name,
		"schema":         schema,
		"partition-spec": []interface{}{}, // Empty partition spec for now
		"write-order":    []interface{}{}, // Empty write order for now
		"stage-create":   false,
		"properties": map[string]string{
			"created-by": "reDB",
		},
	}

	bodyBytes, err := json.Marshal(requestBody)
	if err != nil {
		return fmt.Errorf("error marshaling request: %v", err)
	}

	// Create table
	tablesURL := fmt.Sprintf("%s/v1/namespaces/%s/tables",
		strings.TrimSuffix(client.BaseURL, "/"),
		strings.ReplaceAll(tableInfo.Schema, ".", "%2E"))

	req, err := http.NewRequest("POST", tablesURL, strings.NewReader(string(bodyBytes)))
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error making request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("REST API returned status %d", resp.StatusCode)
	}

	return nil
}

// convertCommonTableToIceberg converts common.TableInfo to Iceberg schema
func convertCommonTableToIceberg(tableInfo common.TableInfo) map[string]interface{} {
	fields := make([]map[string]interface{}, len(tableInfo.Columns))

	for i, column := range tableInfo.Columns {
		fields[i] = map[string]interface{}{
			"id":       i + 1,
			"name":     column.Name,
			"required": !column.IsNullable,
			"type":     convertSQLTypeToIceberg(column.DataType),
		}
	}

	// Set identifier fields (primary key)
	identifierFieldIDs := make([]int, 0)
	for i, column := range tableInfo.Columns {
		if column.IsPrimaryKey {
			identifierFieldIDs = append(identifierFieldIDs, i+1)
		}
	}

	schema := map[string]interface{}{
		"type":                 "struct",
		"schema-id":            0,
		"identifier-field-ids": identifierFieldIDs,
		"fields":               fields,
	}

	return schema
}

// convertSQLTypeToIceberg converts SQL type to Iceberg type
func convertSQLTypeToIceberg(sqlType string) string {
	sqlType = strings.ToUpper(strings.TrimSpace(sqlType))

	switch {
	case sqlType == "BOOLEAN":
		return "boolean"
	case sqlType == "INTEGER" || sqlType == "INT":
		return "int"
	case sqlType == "BIGINT":
		return "long"
	case sqlType == "REAL" || sqlType == "FLOAT":
		return "float"
	case sqlType == "DOUBLE PRECISION" || sqlType == "DOUBLE":
		return "double"
	case strings.HasPrefix(sqlType, "VARCHAR") || sqlType == "TEXT":
		return "string"
	case sqlType == "BYTEA" || sqlType == "BINARY":
		return "binary"
	case sqlType == "DATE":
		return "date"
	case sqlType == "TIME":
		return "time"
	case sqlType == "TIMESTAMP":
		return "timestamp"
	case sqlType == "TIMESTAMP WITH TIME ZONE":
		return "timestamptz"
	case strings.HasPrefix(sqlType, "DECIMAL") || strings.HasPrefix(sqlType, "NUMERIC"):
		return strings.ToLower(sqlType)
	case strings.HasPrefix(sqlType, "ARRAY"):
		return "list<string>" // Simplified - would need proper element type parsing
	case sqlType == "JSON" || sqlType == "JSONB":
		return "string" // JSON stored as string in Iceberg
	default:
		return "string" // Default fallback
	}
}

// createTableHive creates a table using Hive metastore
func createTableHive(client *IcebergClient, tableInfo common.TableInfo) error {
	// In a real implementation, you'd use Hive metastore client
	return fmt.Errorf("Hive table creation not implemented")
}

// createTableHadoop creates a table using Hadoop catalog
func createTableHadoop(client *IcebergClient, tableInfo common.TableInfo) error {
	// In a real implementation, you'd create table metadata files
	return fmt.Errorf("Hadoop table creation not implemented")
}

// createView creates a new view in the catalog
func createView(client *IcebergClient, viewInfo common.ViewInfo) error {
	// Iceberg views are not widely supported yet
	return fmt.Errorf("Iceberg views are not supported")
}
