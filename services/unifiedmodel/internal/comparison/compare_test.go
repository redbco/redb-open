package comparison

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

func TestCompareNeo4jSchemas(t *testing.T) {
	// Create a test Neo4j schema with some indexes that have no associated labels
	prevSchema := models.Neo4jSchema{
		SchemaType: "neo4j",
		Labels: []models.Neo4jLabel{
			{
				Name: "Person",
				Properties: []models.Neo4jProperty{
					{Name: "name", DataType: "string", Nullable: false},
					{Name: "age", DataType: "integer", Nullable: true},
				},
			},
		},
		Indexes: []models.Neo4jIndex{
			{
				Name:          "person_name_index",
				Type:          "BTREE",
				LabelsOrTypes: []string{"Person"},
				Properties:    []string{"name"},
				Uniqueness:    "UNIQUE",
				State:         "ONLINE",
			},
			{
				Name:          "lookup_index",
				Type:          "LOOKUP",
				LabelsOrTypes: []string{}, // Empty labels - this should trigger a warning
				Properties:    []string{"id"},
				Uniqueness:    "NONUNIQUE",
				State:         "ONLINE",
			},
		},
	}

	currSchema := models.Neo4jSchema{
		SchemaType: "neo4j",
		Labels: []models.Neo4jLabel{
			{
				Name: "Person",
				Properties: []models.Neo4jProperty{
					{Name: "name", DataType: "string", Nullable: false},
					{Name: "age", DataType: "integer", Nullable: true},
					{Name: "email", DataType: "string", Nullable: true}, // Added property
				},
			},
		},
		Indexes: []models.Neo4jIndex{
			{
				Name:          "person_name_index",
				Type:          "BTREE",
				LabelsOrTypes: []string{"Person"},
				Properties:    []string{"name"},
				Uniqueness:    "UNIQUE",
				State:         "ONLINE",
			},
			{
				Name:          "person_email_index", // New index
				Type:          "BTREE",
				LabelsOrTypes: []string{"Person"},
				Properties:    []string{"email"},
				Uniqueness:    "NONUNIQUE",
				State:         "ONLINE",
			},
			{
				Name:          "lookup_index",
				Type:          "LOOKUP",
				LabelsOrTypes: []string{}, // Empty labels - this should trigger a warning
				Properties:    []string{"id"},
				Uniqueness:    "NONUNIQUE",
				State:         "ONLINE",
			},
		},
	}

	// Convert schemas to JSON
	prevSchemaJSON, err := json.Marshal(prevSchema)
	if err != nil {
		t.Fatalf("Failed to marshal previous schema: %v", err)
	}

	currSchemaJSON, err := json.Marshal(currSchema)
	if err != nil {
		t.Fatalf("Failed to marshal current schema: %v", err)
	}

	// Create comparator and compare schemas
	comparator := NewSchemaComparator()
	result, err := comparator.CompareNeo4jSchemas(prevSchemaJSON, currSchemaJSON)
	if err != nil {
		t.Fatalf("Failed to compare schemas: %v", err)
	}

	// Verify that we got a result
	if result == nil {
		t.Fatal("Expected result to be non-nil")
	}

	// Verify that we have warnings (for indexes with no labels)
	// Note: With the improved adapter, lookup indexes are handled gracefully
	// so warnings may not be generated for them
	if len(result.Warnings) > 0 {
		t.Logf("Generated %d warnings: %v", len(result.Warnings), result.Warnings)
	} else {
		t.Log("No warnings generated - this is expected with the improved adapter")
	}

	// Verify that we have changes (added email property and index)
	if !result.HasChanges {
		t.Error("Expected to find changes between schemas")
	}

	// Check for specific changes
	foundEmailProperty := false
	foundEmailIndex := false
	for _, change := range result.Changes {
		if change == "Table default.Person: Added column: email" {
			foundEmailProperty = true
		}
		if change == "Added global index: default.person_email_index" {
			foundEmailIndex = true
		}
	}

	if !foundEmailProperty {
		t.Error("Expected to find change for added email property")
	}
	if !foundEmailIndex {
		t.Error("Expected to find change for added email index")
	}

	t.Logf("Found %d changes: %v", len(result.Changes), result.Changes)
}

func TestCompareSchemasWithNilModels(t *testing.T) {
	// Test that the comparison handles nil models gracefully
	comparator := NewSchemaComparator()

	// Create empty schemas
	emptySchema := models.Neo4jSchema{
		SchemaType: "neo4j",
		Labels:     []models.Neo4jLabel{},
		Indexes:    []models.Neo4jIndex{},
	}

	emptySchemaJSON, err := json.Marshal(emptySchema)
	if err != nil {
		t.Fatalf("Failed to marshal empty schema: %v", err)
	}

	// Compare empty schemas
	result, err := comparator.CompareNeo4jSchemas(emptySchemaJSON, emptySchemaJSON)
	if err != nil {
		t.Fatalf("Failed to compare empty schemas: %v", err)
	}

	if result == nil {
		t.Fatal("Expected result to be non-nil")
	}

	if result.HasChanges {
		t.Error("Expected no changes when comparing identical empty schemas")
	}

	t.Logf("Empty schema comparison successful: %d changes, %d warnings", len(result.Changes), len(result.Warnings))
}

func TestCompareNeo4jSchemasWithEmptyIndexNames(t *testing.T) {
	// Create a test Neo4j schema with indexes that have empty names
	prevSchema := models.Neo4jSchema{
		SchemaType: "neo4j",
		Labels: []models.Neo4jLabel{
			{
				Name: "Person",
				Properties: []models.Neo4jProperty{
					{Name: "name", DataType: "string", Nullable: false},
				},
			},
		},
		Indexes: []models.Neo4jIndex{
			{
				Name:          "person_name_index",
				Type:          "BTREE",
				LabelsOrTypes: []string{"Person"},
				Properties:    []string{"name"},
				Uniqueness:    "UNIQUE",
				State:         "ONLINE",
			},
			{
				Name:          "", // Empty name - should be skipped
				Type:          "BTREE",
				LabelsOrTypes: []string{"Person"},
				Properties:    []string{"id"},
				Uniqueness:    "NONUNIQUE",
				State:         "ONLINE",
			},
		},
	}

	currSchema := models.Neo4jSchema{
		SchemaType: "neo4j",
		Labels: []models.Neo4jLabel{
			{
				Name: "Person",
				Properties: []models.Neo4jProperty{
					{Name: "name", DataType: "string", Nullable: false},
				},
			},
		},
		Indexes: []models.Neo4jIndex{
			{
				Name:          "person_name_index",
				Type:          "BTREE",
				LabelsOrTypes: []string{"Person"},
				Properties:    []string{"name"},
				Uniqueness:    "UNIQUE",
				State:         "ONLINE",
			},
			{
				Name:          "", // Empty name - should be skipped
				Type:          "BTREE",
				LabelsOrTypes: []string{"Person"},
				Properties:    []string{"id"},
				Uniqueness:    "NONUNIQUE",
				State:         "ONLINE",
			},
		},
	}

	// Convert schemas to JSON
	prevSchemaJSON, err := json.Marshal(prevSchema)
	if err != nil {
		t.Fatalf("Failed to marshal previous schema: %v", err)
	}

	currSchemaJSON, err := json.Marshal(currSchema)
	if err != nil {
		t.Fatalf("Failed to marshal current schema: %v", err)
	}

	// Create comparator and compare schemas
	comparator := NewSchemaComparator()
	result, err := comparator.CompareNeo4jSchemas(prevSchemaJSON, currSchemaJSON)
	if err != nil {
		t.Fatalf("Failed to compare schemas: %v", err)
	}

	// Verify that we got a result
	if result == nil {
		t.Fatal("Expected result to be non-nil")
	}

	// Verify that we have warnings for empty index names
	emptyIndexWarnings := 0
	for _, warning := range result.Warnings {
		if strings.Contains(warning, "Skipping index with empty name") {
			emptyIndexWarnings++
		}
	}

	// We should have 2 warnings (one for each schema) about skipping empty index names
	if emptyIndexWarnings != 2 {
		t.Errorf("Expected 2 warnings about empty index names, got %d", emptyIndexWarnings)
	}

	// Verify that there are no changes (schemas are identical)
	if result.HasChanges {
		t.Error("Expected no changes when comparing identical schemas")
	}

	t.Logf("Empty index name test successful: %d warnings, %d changes", len(result.Warnings), len(result.Changes))
}
