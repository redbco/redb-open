package edgedb

import (
	"context"
	"fmt"
	"strings"

	gel "github.com/geldata/gel-go"
)

// FetchData retrieves data from a specified type
func FetchData(client *gel.Client, typeName string, limit int) ([]map[string]interface{}, error) {
	if typeName == "" {
		return nil, fmt.Errorf("type name cannot be empty")
	}

	ctx := context.Background()

	// Split module and type name if provided in format "module::type"
	var module, typeOnly string
	if strings.Contains(typeName, "::") {
		parts := strings.Split(typeName, "::")
		module = parts[0]
		typeOnly = parts[1]
	} else {
		// Assume default module if not specified
		module = "default"
		typeOnly = typeName
	}

	// Get properties for the type
	properties, err := getTypeProperties(client, module, typeOnly)
	if err != nil {
		return nil, err
	}

	// Build and execute query
	query := fmt.Sprintf("SELECT %s::%s {%s}", module, typeOnly, strings.Join(properties, ", "))
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	var rawData []map[string]interface{}
	err = client.Query(ctx, query, &rawData)
	if err != nil {
		return nil, fmt.Errorf("error querying type %s: %v", typeName, err)
	}

	return rawData, nil
}

// InsertData inserts data into a specified type
func InsertData(client *gel.Client, typeName string, data []map[string]interface{}) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	ctx := context.Background()

	// Split module and type name if provided in format "module::type"
	var module, typeOnly string
	if strings.Contains(typeName, "::") {
		parts := strings.Split(typeName, "::")
		module = parts[0]
		typeOnly = parts[1]
	} else {
		// Assume default module if not specified
		module = "default"
		typeOnly = typeName
	}

	// Since the Gel client doesn't have a Begin method, we'll execute each insert individually
	var totalRowsAffected int64

	// Insert each row
	for _, row := range data {
		// Build property assignments
		var assignments []string
		var params []interface{}
		paramIndex := 0

		for prop, value := range row {
			assignments = append(assignments, fmt.Sprintf("%s := $%d", prop, paramIndex+1))
			params = append(params, value)
			paramIndex++
		}

		// Build and execute insert query
		query := fmt.Sprintf("INSERT %s::%s {%s}",
			module,
			typeOnly,
			strings.Join(assignments, ", "))

		// Execute the query
		err := client.Execute(ctx, query, params...)
		if err != nil {
			return totalRowsAffected, fmt.Errorf("error inserting data: %v", err)
		}

		// Since we can't get rows affected, assume 1 row per insert
		totalRowsAffected++
	}

	return totalRowsAffected, nil
}

// WipeDatabase removes all data from the database
func WipeDatabase(client *gel.Client) error {
	ctx := context.Background()

	// Get all object types in the database
	query := `
		SELECT schema::ObjectType {
			name,
			module: {
				name
			}
		}
		FILTER .module.name NOT LIKE 'std::%'
		   AND .module.name NOT LIKE 'schema::%'
		   AND .module.name NOT LIKE 'sys::%'
		   AND .module.name NOT LIKE 'cfg::%'
		   AND .module.name NOT LIKE 'math::%'
		   AND .module.name NOT LIKE 'cal::%'
		   AND .module.name NOT LIKE 'ext::%'
	`

	type ObjectTypeInfo struct {
		Name   string `edgedb:"name"`
		Module struct {
			Name string `edgedb:"name"`
		} `edgedb:"module"`
	}

	var types []ObjectTypeInfo
	err := client.Query(ctx, query, &types)
	if err != nil {
		return fmt.Errorf("error fetching object types: %v", err)
	}

	// Since the Gel client doesn't have transaction support, we'll execute each delete individually
	// Delete all data from each type
	for _, t := range types {
		deleteQuery := fmt.Sprintf("DELETE %s::%s", t.Module.Name, t.Name)
		err := client.Execute(ctx, deleteQuery)
		if err != nil {
			return fmt.Errorf("error deleting data from %s::%s: %v", t.Module.Name, t.Name, err)
		}
	}

	return nil
}

func getTypeProperties(client *gel.Client, module, typeName string) ([]string, error) {
	ctx := context.Background()

	query := `
		SELECT schema::ObjectType {
			properties: {
				name
			}
		}
		FILTER .name = <str>$0 AND .module.name = <str>$1
	`

	type TypeWithProperties struct {
		Properties []struct {
			Name string `edgedb:"name"`
		} `edgedb:"properties"`
	}

	var result TypeWithProperties
	err := client.QuerySingle(ctx, query, &result, typeName, module)
	if err != nil {
		return nil, fmt.Errorf("error fetching properties for type %s::%s: %v", module, typeName, err)
	}

	var properties []string
	for _, prop := range result.Properties {
		properties = append(properties, prop.Name)
	}

	return properties, nil
}
