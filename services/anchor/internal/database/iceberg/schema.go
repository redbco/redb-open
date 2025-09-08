package iceberg

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
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

	// Discover namespaces directly as unifiedmodel types
	err = discoverNamespacesUnified(client, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering namespaces: %v", err)
	}

	// Discover tables directly as unifiedmodel types
	err = discoverTablesUnified(client, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering tables: %v", err)
	}

	// Discover views directly as unifiedmodel types
	err = discoverViewsUnified(client, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering views: %v", err)
	}

	// Views are handled in discoverViewsUnified function

	return um, nil
}

// CreateStructure creates database objects based on the provided parameters
func CreateStructure(db interface{}, um *unifiedmodel.UnifiedModel) error {
	if um == nil {
		return fmt.Errorf("unified model cannot be nil")
	}

	client, ok := db.(*IcebergClient)
	if !ok {
		return fmt.Errorf("invalid database connection type")
	}

	// Create namespaces from UnifiedModel
	for _, namespace := range um.Namespaces {
		if err := createNamespaceFromUnified(client, namespace); err != nil {
			return fmt.Errorf("error creating namespace %s: %v", namespace.Name, err)
		}
	}

	// Create tables from UnifiedModel
	for _, table := range um.Tables {
		if err := createTableFromUnified(client, table); err != nil {
			return fmt.Errorf("error creating table %s: %v", table.Name, err)
		}
	}

	// Create external tables from UnifiedModel
	for _, externalTable := range um.ExternalTables {
		if err := createExternalTableFromUnified(client, externalTable); err != nil {
			return fmt.Errorf("error creating external table %s: %v", externalTable.Name, err)
		}
	}

	// Create views from UnifiedModel
	for _, view := range um.Views {
		if err := createViewFromUnified(client, view); err != nil {
			return fmt.Errorf("error creating view %s: %v", view.Name, err)
		}
	}

	return nil
}

// discoverNamespacesUnified discovers namespaces directly into UnifiedModel
func discoverNamespacesUnified(client *IcebergClient, um *unifiedmodel.UnifiedModel) error {
	switch client.CatalogType {
	case "rest":
		return discoverNamespacesRESTUnified(client, um)
	case "hive":
		return discoverNamespacesHiveUnified(client, um)
	case "hadoop":
		return discoverNamespacesHadoopUnified(client, um)
	default:
		return fmt.Errorf("unsupported catalog type: %s", client.CatalogType)
	}
}

// discoverNamespacesRESTUnified discovers namespaces using REST catalog API directly into UnifiedModel
func discoverNamespacesRESTUnified(client *IcebergClient, um *unifiedmodel.UnifiedModel) error {
	if client.HTTPClient == nil {
		return fmt.Errorf("HTTP client not initialized")
	}

	httpClient, ok := client.HTTPClient.(*http.Client)
	if !ok {
		return fmt.Errorf("invalid HTTP client type")
	}

	// Call REST API to list namespaces
	namespacesURL := fmt.Sprintf("%s/v1/namespaces", strings.TrimSuffix(client.BaseURL, "/"))

	req, err := http.NewRequest("GET", namespacesURL, nil)
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error making request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("API request failed with status: %d", resp.StatusCode)
	}

	var response struct {
		Namespaces [][]string `json:"namespaces"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return fmt.Errorf("error decoding response: %v", err)
	}

	// Convert namespaces to unified model
	for _, namespace := range response.Namespaces {
		if len(namespace) > 0 {
			namespaceName := strings.Join(namespace, ".")
			um.Namespaces[namespaceName] = unifiedmodel.Namespace{
				Name: namespaceName,
			}
		}
	}

	return nil
}

// discoverNamespacesHiveUnified discovers namespaces using Hive catalog directly into UnifiedModel
func discoverNamespacesHiveUnified(client *IcebergClient, um *unifiedmodel.UnifiedModel) error {
	// Placeholder for Hive catalog implementation
	return fmt.Errorf("hive catalog not implemented")
}

// discoverNamespacesHadoopUnified discovers namespaces using Hadoop catalog directly into UnifiedModel
func discoverNamespacesHadoopUnified(client *IcebergClient, um *unifiedmodel.UnifiedModel) error {
	// Placeholder for Hadoop catalog implementation
	return fmt.Errorf("hadoop catalog not implemented")
}

// discoverTablesUnified discovers tables directly into UnifiedModel
func discoverTablesUnified(client *IcebergClient, um *unifiedmodel.UnifiedModel) error {
	switch client.CatalogType {
	case "rest":
		return discoverTablesRESTUnified(client, um)
	case "hive":
		return discoverTablesHiveUnified(client, um)
	case "hadoop":
		return discoverTablesHadoopUnified(client, um)
	default:
		return fmt.Errorf("unsupported catalog type: %s", client.CatalogType)
	}
}

// discoverTablesRESTUnified discovers tables using REST catalog API directly into UnifiedModel
func discoverTablesRESTUnified(client *IcebergClient, um *unifiedmodel.UnifiedModel) error {
	if client.HTTPClient == nil {
		return fmt.Errorf("HTTP client not initialized")
	}

	httpClient, ok := client.HTTPClient.(*http.Client)
	if !ok {
		return fmt.Errorf("invalid HTTP client type")
	}

	// Iterate through all namespaces to discover tables
	for namespaceName := range um.Namespaces {
		// Call REST API to list tables in namespace
		tablesURL := fmt.Sprintf("%s/v1/namespaces/%s/tables", strings.TrimSuffix(client.BaseURL, "/"), namespaceName)

		req, err := http.NewRequest("GET", tablesURL, nil)
		if err != nil {
			return fmt.Errorf("error creating request: %v", err)
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("error making request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			continue // Skip if namespace has no tables or access denied
		}

		var response struct {
			Identifiers []struct {
				Namespace []string `json:"namespace"`
				Name      string   `json:"name"`
			} `json:"identifiers"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
			return fmt.Errorf("error decoding response: %v", err)
		}

		// Get detailed table information for each table
		for _, identifier := range response.Identifiers {
			tableName := identifier.Name

			// Get table schema
			err := getTableSchemaRESTUnified(client, namespaceName, tableName, um)
			if err != nil {
				// Log error but continue with other tables
				continue
			}
		}
	}

	return nil
}

// getTableSchemaRESTUnified gets table schema using REST API directly into UnifiedModel
func getTableSchemaRESTUnified(client *IcebergClient, namespace, tableName string, um *unifiedmodel.UnifiedModel) error {
	if client.HTTPClient == nil {
		return fmt.Errorf("HTTP client not initialized")
	}

	httpClient, ok := client.HTTPClient.(*http.Client)
	if !ok {
		return fmt.Errorf("invalid HTTP client type")
	}

	// Call REST API to get table metadata
	tableURL := fmt.Sprintf("%s/v1/namespaces/%s/tables/%s", strings.TrimSuffix(client.BaseURL, "/"), namespace, tableName)

	req, err := http.NewRequest("GET", tableURL, nil)
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error making request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("API request failed with status: %d", resp.StatusCode)
	}

	var metadata IcebergTableMetadata
	if err := json.NewDecoder(resp.Body).Decode(&metadata); err != nil {
		return fmt.Errorf("error decoding response: %v", err)
	}

	// Convert Iceberg table metadata to UnifiedModel
	fullTableName := fmt.Sprintf("%s.%s", namespace, tableName)

	// Create external table for Iceberg
	um.ExternalTables[fullTableName] = unifiedmodel.ExternalTable{
		Name:     tableName,
		Location: metadata.Location,
		Format:   "iceberg",
		Options: map[string]any{
			"namespace":      namespace,
			"table_uuid":     metadata.TableUUID,
			"format_version": metadata.FormatVersion,
		},
	}

	// Also create regular table entry for compatibility
	table := unifiedmodel.Table{
		Name:        tableName,
		Comment:     fmt.Sprintf("Iceberg table in namespace %s", namespace),
		Columns:     make(map[string]unifiedmodel.Column),
		Indexes:     make(map[string]unifiedmodel.Index),
		Constraints: make(map[string]unifiedmodel.Constraint),
	}

	// Convert schema fields to columns
	if len(metadata.Schemas) > 0 {
		// Find the current schema by ID
		var currentSchema *IcebergSchemaDefinition
		for _, schema := range metadata.Schemas {
			if schema.SchemaID == metadata.CurrentSchemaID {
				currentSchema = &schema
				break
			}
		}

		// If no current schema found, use the first one
		if currentSchema == nil && len(metadata.Schemas) > 0 {
			currentSchema = &metadata.Schemas[0]
		}

		if currentSchema != nil {
			for _, field := range currentSchema.Fields {
				column := unifiedmodel.Column{
					Name:     field.Name,
					DataType: convertIcebergTypeToSQL(field.Type),
					Nullable: !field.Required,
					Options: map[string]any{
						"field_id": field.ID,
					},
				}

				table.Columns[field.Name] = column
			}
		}
	}

	um.Tables[fullTableName] = table

	return nil
}

// discoverTablesHiveUnified discovers tables using Hive catalog directly into UnifiedModel
func discoverTablesHiveUnified(client *IcebergClient, um *unifiedmodel.UnifiedModel) error {
	// Placeholder for Hive catalog implementation
	return fmt.Errorf("hive catalog not implemented")
}

// discoverTablesHadoopUnified discovers tables using Hadoop catalog directly into UnifiedModel
func discoverTablesHadoopUnified(client *IcebergClient, um *unifiedmodel.UnifiedModel) error {
	// Placeholder for Hadoop catalog implementation
	return fmt.Errorf("hadoop catalog not implemented")
}

// discoverViewsUnified discovers views directly into UnifiedModel
func discoverViewsUnified(client *IcebergClient, um *unifiedmodel.UnifiedModel) error {
	// Iceberg views are not widely supported yet, so this is mostly a placeholder
	return nil
}

// createNamespaceFromUnified creates a namespace from UnifiedModel Namespace
func createNamespaceFromUnified(client *IcebergClient, namespace unifiedmodel.Namespace) error {
	if namespace.Name == "" {
		return fmt.Errorf("namespace name cannot be empty")
	}

	switch client.CatalogType {
	case "rest":
		return createNamespaceRESTFromUnified(client, namespace)
	case "hive":
		return createNamespaceHiveFromUnified(client, namespace)
	case "hadoop":
		return createNamespaceHadoopFromUnified(client, namespace)
	default:
		return fmt.Errorf("unsupported catalog type: %s", client.CatalogType)
	}
}

// createNamespaceRESTFromUnified creates a namespace using REST API from UnifiedModel
func createNamespaceRESTFromUnified(client *IcebergClient, namespace unifiedmodel.Namespace) error {
	if client.HTTPClient == nil {
		return fmt.Errorf("HTTP client not initialized")
	}

	httpClient, ok := client.HTTPClient.(*http.Client)
	if !ok {
		return fmt.Errorf("invalid HTTP client type")
	}

	// Prepare namespace creation request
	namespaceParts := strings.Split(namespace.Name, ".")
	requestBody := map[string]interface{}{
		"namespace": namespaceParts,
	}

	if namespace.Options != nil {
		if comment, ok := namespace.Options["comment"].(string); ok && comment != "" {
			requestBody["properties"] = map[string]string{
				"comment": comment,
			}
		}
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return fmt.Errorf("error marshaling request: %v", err)
	}

	// Call REST API to create namespace
	namespacesURL := fmt.Sprintf("%s/v1/namespaces", strings.TrimSuffix(client.BaseURL, "/"))

	req, err := http.NewRequest("POST", namespacesURL, strings.NewReader(string(jsonBody)))
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
		return fmt.Errorf("API request failed with status: %d", resp.StatusCode)
	}

	return nil
}

// createNamespaceHiveFromUnified creates a namespace using Hive catalog from UnifiedModel
func createNamespaceHiveFromUnified(client *IcebergClient, namespace unifiedmodel.Namespace) error {
	// Placeholder for Hive catalog implementation
	return fmt.Errorf("hive catalog not implemented")
}

// createNamespaceHadoopFromUnified creates a namespace using Hadoop catalog from UnifiedModel
func createNamespaceHadoopFromUnified(client *IcebergClient, namespace unifiedmodel.Namespace) error {
	// Placeholder for Hadoop catalog implementation
	return fmt.Errorf("hadoop catalog not implemented")
}

// createTableFromUnified creates a table from UnifiedModel Table
func createTableFromUnified(client *IcebergClient, table unifiedmodel.Table) error {
	if table.Name == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	switch client.CatalogType {
	case "rest":
		return createTableRESTFromUnified(client, table)
	case "hive":
		return createTableHiveFromUnified(client, table)
	case "hadoop":
		return createTableHadoopFromUnified(client, table)
	default:
		return fmt.Errorf("unsupported catalog type: %s", client.CatalogType)
	}
}

// createTableRESTFromUnified creates a table using REST API from UnifiedModel
func createTableRESTFromUnified(client *IcebergClient, table unifiedmodel.Table) error {
	if client.HTTPClient == nil {
		return fmt.Errorf("HTTP client not initialized")
	}

	httpClient, ok := client.HTTPClient.(*http.Client)
	if !ok {
		return fmt.Errorf("invalid HTTP client type")
	}

	// Convert UnifiedModel Table to Iceberg schema
	schema := convertUnifiedTableToIceberg(table)

	// Prepare table creation request
	requestBody := map[string]interface{}{
		"name":   table.Name,
		"schema": schema,
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return fmt.Errorf("error marshaling request: %v", err)
	}

	// Assume default namespace if not specified
	namespace := "default"
	if strings.Contains(table.Name, ".") {
		parts := strings.Split(table.Name, ".")
		if len(parts) >= 2 {
			namespace = strings.Join(parts[:len(parts)-1], ".")
		}
	}

	// Call REST API to create table
	tablesURL := fmt.Sprintf("%s/v1/namespaces/%s/tables", strings.TrimSuffix(client.BaseURL, "/"), namespace)

	req, err := http.NewRequest("POST", tablesURL, strings.NewReader(string(jsonBody)))
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
		return fmt.Errorf("API request failed with status: %d", resp.StatusCode)
	}

	return nil
}

// createTableHiveFromUnified creates a table using Hive catalog from UnifiedModel
func createTableHiveFromUnified(client *IcebergClient, table unifiedmodel.Table) error {
	// Placeholder for Hive catalog implementation
	return fmt.Errorf("hive catalog not implemented")
}

// createTableHadoopFromUnified creates a table using Hadoop catalog from UnifiedModel
func createTableHadoopFromUnified(client *IcebergClient, table unifiedmodel.Table) error {
	// Placeholder for Hadoop catalog implementation
	return fmt.Errorf("hadoop catalog not implemented")
}

// createExternalTableFromUnified creates an external table from UnifiedModel ExternalTable
func createExternalTableFromUnified(client *IcebergClient, externalTable unifiedmodel.ExternalTable) error {
	if externalTable.Name == "" {
		return fmt.Errorf("external table name cannot be empty")
	}

	// For Iceberg, external tables are just regular tables with location specified
	// Convert to regular table and create
	table := unifiedmodel.Table{
		Name:        externalTable.Name,
		Comment:     fmt.Sprintf("External table at %s", externalTable.Location),
		Columns:     make(map[string]unifiedmodel.Column),
		Indexes:     make(map[string]unifiedmodel.Index),
		Constraints: make(map[string]unifiedmodel.Constraint),
	}

	return createTableFromUnified(client, table)
}

// createViewFromUnified creates a view from UnifiedModel View
func createViewFromUnified(client *IcebergClient, view unifiedmodel.View) error {
	// Iceberg views are not widely supported yet
	return fmt.Errorf("iceberg views are not supported")
}

// convertUnifiedTableToIceberg converts UnifiedModel Table to Iceberg schema
func convertUnifiedTableToIceberg(table unifiedmodel.Table) map[string]interface{} {
	schema := map[string]interface{}{
		"type":   "struct",
		"fields": []map[string]interface{}{},
	}

	fieldID := 1
	fields := []map[string]interface{}{}

	for _, column := range table.Columns {
		field := map[string]interface{}{
			"id":       fieldID,
			"name":     column.Name,
			"required": !column.Nullable,
			"type":     convertSQLTypeToIceberg(column.DataType),
		}

		fields = append(fields, field)
		fieldID++
	}

	schema["fields"] = fields
	return schema
}

// convertSQLTypeToIceberg converts SQL data types to Iceberg types
func convertSQLTypeToIceberg(sqlType string) string {
	switch strings.ToLower(sqlType) {
	case "boolean", "bool":
		return "boolean"
	case "int", "integer", "int32":
		return "int"
	case "bigint", "long", "int64":
		return "long"
	case "float", "real":
		return "float"
	case "double", "double precision":
		return "double"
	case "decimal", "numeric":
		return "decimal(38,18)" // Default precision and scale
	case "date":
		return "date"
	case "time":
		return "time"
	case "timestamp", "datetime":
		return "timestamp"
	case "string", "varchar", "text", "char":
		return "string"
	case "binary", "varbinary", "blob":
		return "binary"
	case "uuid":
		return "uuid"
	default:
		return "string" // Default fallback
	}
}

// convertIcebergTypeToSQL converts Iceberg data types to SQL types
func convertIcebergTypeToSQL(icebergType interface{}) string {
	switch t := icebergType.(type) {
	case string:
		switch t {
		case "boolean":
			return "BOOLEAN"
		case "int":
			return "INTEGER"
		case "long":
			return "BIGINT"
		case "float":
			return "FLOAT"
		case "double":
			return "DOUBLE"
		case "date":
			return "DATE"
		case "time":
			return "TIME"
		case "timestamp":
			return "TIMESTAMP"
		case "string":
			return "VARCHAR"
		case "binary":
			return "BINARY"
		case "uuid":
			return "UUID"
		default:
			if strings.HasPrefix(t, "decimal") {
				return "DECIMAL" + strings.TrimPrefix(t, "decimal")
			}
			return "VARCHAR" // Default fallback
		}
	case map[string]interface{}:
		if typeStr, ok := t["type"].(string); ok {
			switch typeStr {
			case "struct":
				return "STRUCT"
			case "list":
				return "ARRAY"
			case "map":
				return "MAP"
			default:
				return "VARCHAR"
			}
		}
	}
	return "VARCHAR" // Default fallback
}
