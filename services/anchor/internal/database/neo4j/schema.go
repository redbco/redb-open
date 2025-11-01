package neo4j

import (
	"context"
	"fmt"
	"strings"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// DiscoverSchema fetches the current schema of a Neo4j database and returns a UnifiedModel
func DiscoverSchema(driver neo4j.DriverWithContext) (*unifiedmodel.UnifiedModel, error) {
	// Create the unified model
	um := &unifiedmodel.UnifiedModel{
		DatabaseType:  dbcapabilities.Neo4j,
		Graphs:        make(map[string]unifiedmodel.Graph),
		Nodes:         make(map[string]unifiedmodel.Node),
		Relationships: make(map[string]unifiedmodel.Relationship),
		Indexes:       make(map[string]unifiedmodel.Index),
		Constraints:   make(map[string]unifiedmodel.Constraint),
		Procedures:    make(map[string]unifiedmodel.Procedure),
		Functions:     make(map[string]unifiedmodel.Function),
	}

	ctx := context.Background()
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	var err error

	// Get node labels and their properties directly as unifiedmodel types
	err = discoverLabelsUnified(ctx, session, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering labels: %v", err)
	}

	// Get relationship types and their properties directly as unifiedmodel types
	err = discoverRelationshipTypesUnified(ctx, session, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering relationship types: %v", err)
	}

	// Create a main graph that contains all nodes and relationships
	mainGraph := unifiedmodel.Graph{
		Name:        "main",
		NodeLabels:  make(map[string]unifiedmodel.Node),
		RelTypes:    make(map[string]unifiedmodel.Relationship),
		Indexes:     make(map[string]unifiedmodel.Index),
		Constraints: make(map[string]unifiedmodel.Constraint),
	}

	// Add all nodes and relationships to the main graph
	for name, node := range um.Nodes {
		mainGraph.NodeLabels[name] = node
	}
	for name, rel := range um.Relationships {
		mainGraph.RelTypes[name] = rel
	}

	// Get constraints directly as unifiedmodel types
	err = discoverConstraintsUnified(ctx, session, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering constraints: %v", err)
	}

	// Get indexes directly as unifiedmodel types
	err = discoverIndexesUnified(ctx, session, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering indexes: %v", err)
	}

	// Add the main graph to the unified model
	um.Graphs["main"] = mainGraph

	// Get procedures directly as unifiedmodel types
	err = discoverProceduresUnified(ctx, session, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering procedures: %v", err)
	}

	// Get functions directly as unifiedmodel types
	err = discoverFunctionsUnified(ctx, session, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering functions: %v", err)
	}

	return um, nil
}

// CreateStructure creates database objects from a UnifiedModel
func CreateStructure(driver neo4j.DriverWithContext, um *unifiedmodel.UnifiedModel) error {
	if um == nil {
		return fmt.Errorf("unified model cannot be nil")
	}
	ctx := context.Background()
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	// Create constraints from UnifiedModel
	for _, constraint := range um.Constraints {
		if err := createConstraintFromUnified(ctx, session, constraint); err != nil {
			return fmt.Errorf("error creating constraint %s: %v", constraint.Name, err)
		}
	}

	// Create indexes from UnifiedModel
	for _, index := range um.Indexes {
		if err := createIndexFromUnified(ctx, session, index); err != nil {
			return fmt.Errorf("error creating index %s: %v", index.Name, err)
		}
	}

	return nil
}

// discoverLabelsUnified discovers node labels directly into UnifiedModel
func discoverLabelsUnified(ctx context.Context, session neo4j.SessionWithContext, um *unifiedmodel.UnifiedModel) error {
	// Get all labels
	query := "CALL db.labels() YIELD label RETURN label"
	result, err := session.Run(ctx, query, nil)
	if err != nil {
		return fmt.Errorf("error fetching labels: %v", err)
	}

	var labels []string
	for result.Next(ctx) {
		record := result.Record()
		if label, ok := record.Get("label"); ok {
			labels = append(labels, label.(string))
		}
	}

	// For each label, get its properties
	for _, label := range labels {
		node := unifiedmodel.Node{
			Label:      label,
			Properties: make(map[string]unifiedmodel.Property),
			Indexes:    make(map[string]unifiedmodel.Index),
		}

		// Get properties for this label
		propQuery := fmt.Sprintf("MATCH (n:%s) RETURN keys(n) as keys LIMIT 100", label)
		propResult, err := session.Run(ctx, propQuery, nil)
		if err != nil {
			continue // Skip if error getting properties
		}

		propertySet := make(map[string]bool)
		for propResult.Next(ctx) {
			record := propResult.Record()
			if keys, ok := record.Get("keys"); ok {
				if keyList, ok := keys.([]interface{}); ok {
					for _, key := range keyList {
						if keyStr, ok := key.(string); ok {
							propertySet[keyStr] = true
						}
					}
				}
			}
		}

		// Convert properties to unifiedmodel format
		for propName := range propertySet {
			// Note: Type inference is available via data_extraction.go InferPropertyType
			// For now, using "mixed" type. Can be enhanced with sample-based inference.
			node.Properties[propName] = unifiedmodel.Property{
				Name: propName,
				Type: "mixed", // Neo4j properties can be of various types
			}
		}

		um.Nodes[label] = node
	}

	return nil
}

// discoverRelationshipTypesUnified discovers relationship types directly into UnifiedModel
func discoverRelationshipTypesUnified(ctx context.Context, session neo4j.SessionWithContext, um *unifiedmodel.UnifiedModel) error {
	// Get all relationship types
	query := "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType"
	result, err := session.Run(ctx, query, nil)
	if err != nil {
		return fmt.Errorf("error fetching relationship types: %v", err)
	}

	var relTypes []string
	for result.Next(ctx) {
		record := result.Record()
		if relType, ok := record.Get("relationshipType"); ok {
			relTypes = append(relTypes, relType.(string))
		}
	}

	// For each relationship type, get its properties
	for _, relType := range relTypes {
		relationship := unifiedmodel.Relationship{
			Type:       relType,
			Properties: make(map[string]unifiedmodel.Property),
		}

		// Get properties for this relationship type
		propQuery := fmt.Sprintf("MATCH ()-[r:%s]-() RETURN keys(r) as keys LIMIT 100", relType)
		propResult, err := session.Run(ctx, propQuery, nil)
		if err != nil {
			continue // Skip if error getting properties
		}

		propertySet := make(map[string]bool)
		for propResult.Next(ctx) {
			record := propResult.Record()
			if keys, ok := record.Get("keys"); ok {
				if keyList, ok := keys.([]interface{}); ok {
					for _, key := range keyList {
						if keyStr, ok := key.(string); ok {
							propertySet[keyStr] = true
						}
					}
				}
			}
		}

		// Convert properties to unifiedmodel format
		for propName := range propertySet {
			relationship.Properties[propName] = unifiedmodel.Property{
				Name: propName,
				Type: "mixed", // Neo4j properties can be of various types
			}
		}

		um.Relationships[relType] = relationship
	}

	return nil
}

// discoverConstraintsUnified discovers constraints directly into UnifiedModel
func discoverConstraintsUnified(ctx context.Context, session neo4j.SessionWithContext, um *unifiedmodel.UnifiedModel) error {
	// Try Neo4j 4.x+ syntax first
	query := "SHOW CONSTRAINTS"
	result, err := session.Run(ctx, query, nil)
	if err != nil {
		// Try older syntax for Neo4j 3.x
		query = "CALL db.constraints() YIELD description RETURN description"
		result, err = session.Run(ctx, query, nil)
		if err != nil {
			return fmt.Errorf("error fetching constraints: %v", err)
		}
	}

	for result.Next(ctx) {
		record := result.Record()
		var constraintName, constraintType string
		var propertyKeys []string

		// Handle different Neo4j versions
		if description, ok := record.Get("description"); ok {
			// Parse constraint from description (Neo4j 3.x)
			descStr := description.(string)
			constraintName = fmt.Sprintf("constraint_%d", len(um.Constraints))
			if strings.Contains(descStr, "UNIQUE") {
				constraintType = "UNIQUE"
			} else if strings.Contains(descStr, "EXISTS") {
				constraintType = "NOT_NULL"
			}
			// Extract property keys from description (simplified)
			propertyKeys = []string{"id"} // Default fallback
		} else {
			// Handle Neo4j 4.x+ format
			if name, ok := record.Get("name"); ok {
				constraintName = name.(string)
			}
			if cType, ok := record.Get("type"); ok {
				constraintType = cType.(string)
			}
		}

		var unifiedType unifiedmodel.ConstraintType
		switch constraintType {
		case "UNIQUE", "UNIQUENESS":
			unifiedType = unifiedmodel.ConstraintTypeUnique
		case "EXISTS", "NODE_PROPERTY_EXISTENCE", "RELATIONSHIP_PROPERTY_EXISTENCE":
			unifiedType = unifiedmodel.ConstraintTypeNotNull
		default:
			unifiedType = unifiedmodel.ConstraintTypeUnique
		}

		constraint := unifiedmodel.Constraint{
			Name:    constraintName,
			Type:    unifiedType,
			Columns: propertyKeys,
		}

		um.Constraints[constraintName] = constraint

		// Also add to main graph if it exists
		if mainGraph, exists := um.Graphs["main"]; exists {
			mainGraph.Constraints[constraintName] = constraint
			um.Graphs["main"] = mainGraph
		}
	}

	return nil
}

// discoverIndexesUnified discovers indexes directly into UnifiedModel
func discoverIndexesUnified(ctx context.Context, session neo4j.SessionWithContext, um *unifiedmodel.UnifiedModel) error {
	// Try Neo4j 4.x+ syntax first
	query := "SHOW INDEXES"
	result, err := session.Run(ctx, query, nil)
	if err != nil {
		// Try older syntax for Neo4j 3.x
		query = "CALL db.indexes() YIELD description, state RETURN description, state"
		result, err = session.Run(ctx, query, nil)
		if err != nil {
			return fmt.Errorf("error fetching indexes: %v", err)
		}
	}

	for result.Next(ctx) {
		record := result.Record()
		var indexName string
		var properties []string
		var isUnique bool

		// Handle different Neo4j versions
		if description, ok := record.Get("description"); ok {
			// Parse index from description (Neo4j 3.x)
			descStr := description.(string)
			indexName = fmt.Sprintf("index_%d", len(um.Indexes))
			isUnique = strings.Contains(descStr, "UNIQUE")
			// Extract property names from description (simplified)
			properties = []string{"id"} // Default fallback
		} else {
			// Handle Neo4j 4.x+ format
			if name, ok := record.Get("name"); ok {
				indexName = name.(string)
			}
			if props, ok := record.Get("properties"); ok {
				if propList, ok := props.([]interface{}); ok {
					for _, prop := range propList {
						if propStr, ok := prop.(string); ok {
							properties = append(properties, propStr)
						}
					}
				}
			}
			if uniqueness, ok := record.Get("uniqueness"); ok {
				isUnique = uniqueness.(string) == "UNIQUE"
			}
		}

		index := unifiedmodel.Index{
			Name:    indexName,
			Columns: properties,
			Unique:  isUnique,
		}

		um.Indexes[indexName] = index

		// Also add to main graph if it exists
		if mainGraph, exists := um.Graphs["main"]; exists {
			mainGraph.Indexes[indexName] = index
			um.Graphs["main"] = mainGraph
		}
	}

	return nil
}

// discoverProceduresUnified discovers procedures directly into UnifiedModel
func discoverProceduresUnified(ctx context.Context, session neo4j.SessionWithContext, um *unifiedmodel.UnifiedModel) error {
	query := "SHOW PROCEDURES"
	result, err := session.Run(ctx, query, nil)
	if err != nil {
		// Try older syntax
		query = "CALL dbms.procedures() YIELD name, signature, description RETURN name, signature, description"
		result, err = session.Run(ctx, query, nil)
		if err != nil {
			return fmt.Errorf("error fetching procedures: %v", err)
		}
	}

	for result.Next(ctx) {
		record := result.Record()
		var name, signature, description string

		if n, ok := record.Get("name"); ok {
			name = n.(string)
		}
		if s, ok := record.Get("signature"); ok {
			signature = s.(string)
		}
		if d, ok := record.Get("description"); ok {
			description = d.(string)
		}

		procedure := unifiedmodel.Procedure{
			Name:       name,
			Language:   "cypher",
			Definition: fmt.Sprintf("%s - %s", signature, description),
		}

		um.Procedures[name] = procedure
	}

	return nil
}

// discoverFunctionsUnified discovers functions directly into UnifiedModel
func discoverFunctionsUnified(ctx context.Context, session neo4j.SessionWithContext, um *unifiedmodel.UnifiedModel) error {
	query := "SHOW FUNCTIONS"
	result, err := session.Run(ctx, query, nil)
	if err != nil {
		// Try older syntax
		query = "CALL dbms.functions() YIELD name, signature, description RETURN name, signature, description"
		result, err = session.Run(ctx, query, nil)
		if err != nil {
			return fmt.Errorf("error fetching functions: %v", err)
		}
	}

	for result.Next(ctx) {
		record := result.Record()
		var name, signature, description string

		if n, ok := record.Get("name"); ok {
			name = n.(string)
		}
		if s, ok := record.Get("signature"); ok {
			signature = s.(string)
		}
		if d, ok := record.Get("description"); ok {
			description = d.(string)
		}

		function := unifiedmodel.Function{
			Name:       name,
			Language:   "cypher",
			Returns:    "mixed",
			Definition: fmt.Sprintf("%s - %s", signature, description),
		}

		um.Functions[name] = function
	}

	return nil
}

// createConstraintFromUnified creates a constraint from UnifiedModel Constraint
func createConstraintFromUnified(ctx context.Context, session neo4j.SessionWithContext, constraint unifiedmodel.Constraint) error {
	var query string

	switch constraint.Type {
	case unifiedmodel.ConstraintTypeUnique:
		if len(constraint.Columns) > 0 {
			// Assume it's a node constraint for simplicity
			query = fmt.Sprintf("CREATE CONSTRAINT %s FOR (n:Node) REQUIRE n.%s IS UNIQUE",
				constraint.Name, constraint.Columns[0])
		}
	case unifiedmodel.ConstraintTypeNotNull:
		if len(constraint.Columns) > 0 {
			query = fmt.Sprintf("CREATE CONSTRAINT %s FOR (n:Node) REQUIRE n.%s IS NOT NULL",
				constraint.Name, constraint.Columns[0])
		}
	default:
		return fmt.Errorf("unsupported constraint type: %v", constraint.Type)
	}

	if query != "" {
		_, err := session.Run(ctx, query, nil)
		return err
	}

	return nil
}

// createIndexFromUnified creates an index from UnifiedModel Index
func createIndexFromUnified(ctx context.Context, session neo4j.SessionWithContext, index unifiedmodel.Index) error {
	if len(index.Columns) == 0 {
		return fmt.Errorf("index must have at least one column")
	}

	var query string
	if index.Unique {
		query = fmt.Sprintf("CREATE INDEX %s FOR (n:Node) ON (n.%s)",
			index.Name, strings.Join(index.Columns, ", n."))
	} else {
		query = fmt.Sprintf("CREATE INDEX %s FOR (n:Node) ON (n.%s)",
			index.Name, strings.Join(index.Columns, ", n."))
	}

	_, err := session.Run(ctx, query, nil)
	return err
}
