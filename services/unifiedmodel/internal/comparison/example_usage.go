package comparison

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

// ExampleUsage demonstrates how to use the improved schema comparison
// functionality for Neo4j schemas
func ExampleUsage() {
	// Create a comparator
	comparator := NewSchemaComparator()

	// Example Neo4j schemas
	previousSchema := models.Neo4jSchema{
		SchemaType: "neo4j",
		Labels: []models.Neo4jLabel{
			{
				Name: "User",
				Properties: []models.Neo4jProperty{
					{Name: "id", DataType: "string", Nullable: false},
					{Name: "name", DataType: "string", Nullable: false},
				},
			},
		},
		Indexes: []models.Neo4jIndex{
			{
				Name:          "user_id_index",
				Type:          "BTREE",
				LabelsOrTypes: []string{"User"},
				Properties:    []string{"id"},
				Uniqueness:    "UNIQUE",
				State:         "ONLINE",
			},
			{
				Name:          "lookup_index",
				Type:          "LOOKUP",
				LabelsOrTypes: []string{}, // This will be handled gracefully
				Properties:    []string{"id"},
				Uniqueness:    "NONUNIQUE",
				State:         "ONLINE",
			},
		},
	}

	currentSchema := models.Neo4jSchema{
		SchemaType: "neo4j",
		Labels: []models.Neo4jLabel{
			{
				Name: "User",
				Properties: []models.Neo4jProperty{
					{Name: "id", DataType: "string", Nullable: false},
					{Name: "name", DataType: "string", Nullable: false},
					{Name: "email", DataType: "string", Nullable: true}, // Added property
				},
			},
		},
		Indexes: []models.Neo4jIndex{
			{
				Name:          "user_id_index",
				Type:          "BTREE",
				LabelsOrTypes: []string{"User"},
				Properties:    []string{"id"},
				Uniqueness:    "UNIQUE",
				State:         "ONLINE",
			},
			{
				Name:          "user_email_index", // New index
				Type:          "BTREE",
				LabelsOrTypes: []string{"User"},
				Properties:    []string{"email"},
				Uniqueness:    "NONUNIQUE",
				State:         "ONLINE",
			},
			{
				Name:          "lookup_index",
				Type:          "LOOKUP",
				LabelsOrTypes: []string{}, // This will be handled gracefully
				Properties:    []string{"id"},
				Uniqueness:    "NONUNIQUE",
				State:         "ONLINE",
			},
		},
	}

	// Convert to JSON
	prevJSON, err := json.Marshal(previousSchema)
	if err != nil {
		log.Fatalf("Failed to marshal previous schema: %v", err)
	}

	currJSON, err := json.Marshal(currentSchema)
	if err != nil {
		log.Fatalf("Failed to marshal current schema: %v", err)
	}

	// Compare schemas using the specialized Neo4j method
	result, err := comparator.CompareNeo4jSchemas(prevJSON, currJSON)
	if err != nil {
		log.Fatalf("Failed to compare schemas: %v", err)
	}

	// Print results
	fmt.Printf("Schema comparison completed successfully!\n")
	fmt.Printf("Has changes: %t\n", result.HasChanges)
	fmt.Printf("Number of changes: %d\n", len(result.Changes))
	fmt.Printf("Number of warnings: %d\n", len(result.Warnings))

	if len(result.Changes) > 0 {
		fmt.Println("\nChanges detected:")
		for i, change := range result.Changes {
			fmt.Printf("  %d. %s\n", i+1, change)
		}
	}

	if len(result.Warnings) > 0 {
		fmt.Println("\nWarnings:")
		for i, warning := range result.Warnings {
			fmt.Printf("  %d. %s\n", i+1, warning)
		}
	}

	// You can also use the generic method for other database types
	// result, err = comparator.CompareSchemas("postgres", prevJSON, currJSON)
}
