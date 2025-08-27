package neo4j

import (
	"context"
	"fmt"
	"strings"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
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

	// Get node labels and their properties, convert to nodes
	labels, err := discoverLabels(ctx, session)
	if err != nil {
		return nil, fmt.Errorf("error discovering labels: %v", err)
	}
	for _, label := range labels {
		node := ConvertNeo4jLabelToNode(label)
		um.Nodes[label.Name] = node
	}

	// Get relationship types and their properties, convert to relationships
	relationshipTypes, err := discoverRelationshipTypes(ctx, session)
	if err != nil {
		return nil, fmt.Errorf("error discovering relationship types: %v", err)
	}
	for _, relType := range relationshipTypes {
		relationship := ConvertNeo4jRelationshipType(relType)
		um.Relationships[relType.Name] = relationship
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

	// Get constraints
	constraints, err := discoverConstraints(ctx, session)
	if err != nil {
		return nil, fmt.Errorf("error discovering constraints: %v", err)
	}
	for _, constraint := range constraints {
		unifiedConstraint := ConvertNeo4jConstraint(constraint)
		um.Constraints[constraint.Name] = unifiedConstraint
		mainGraph.Constraints[constraint.Name] = unifiedConstraint
	}

	// Get indexes
	indexes, err := discoverIndexes(ctx, session)
	if err != nil {
		return nil, fmt.Errorf("error discovering indexes: %v", err)
	}
	for _, index := range indexes {
		unifiedIndex := unifiedmodel.Index{
			Name:    index.Name,
			Columns: index.Columns,
			Unique:  index.IsUnique,
		}
		um.Indexes[index.Name] = unifiedIndex
		mainGraph.Indexes[index.Name] = unifiedIndex
	}

	// Add the main graph to the unified model
	um.Graphs["main"] = mainGraph

	// Get procedures
	procedures, err := discoverProcedures(ctx, session)
	if err != nil {
		return nil, fmt.Errorf("error discovering procedures: %v", err)
	}
	for _, procedure := range procedures {
		um.Procedures[procedure.Name] = unifiedmodel.Procedure{
			Name:       procedure.Name,
			Language:   "cypher", // Neo4j procedures are typically written in Cypher or Java
			Definition: procedure.Body,
		}
	}

	// Get functions
	functions, err := discoverFunctions(ctx, session)
	if err != nil {
		return nil, fmt.Errorf("error discovering functions: %v", err)
	}
	for _, function := range functions {
		um.Functions[function.Name] = unifiedmodel.Function{
			Name:       function.Name,
			Language:   "cypher", // Neo4j functions are typically written in Cypher
			Returns:    function.ReturnType,
			Definition: function.Body,
		}
	}

	return um, nil
}

// CreateStructure creates database objects based on the provided parameters
func CreateStructure(driver neo4j.DriverWithContext, params common.StructureParams) error {
	ctx := context.Background()
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	// Create constraints first
	for _, constraint := range params.Constraints {
		if err := createConstraint(ctx, session, constraint); err != nil {
			return fmt.Errorf("error creating constraint %s: %v", constraint.Name, err)
		}
	}

	// Create indexes
	for _, index := range params.Indexes {
		if err := createIndex(ctx, session, index); err != nil {
			return fmt.Errorf("error creating index %s: %v", index.Name, err)
		}
	}

	return nil
}

func discoverLabels(ctx context.Context, session neo4j.SessionWithContext) ([]LabelInfo, error) {
	// Try Neo4j 4.x+ syntax first
	query := "SHOW DATABASE"
	result, err := session.Run(ctx, query, nil)
	if err != nil {
		// Try older syntax for Neo4j 3.x
		query = "CALL db.labels() YIELD label RETURN label"
		result, err = session.Run(ctx, query, nil)
		if err != nil {
			return nil, fmt.Errorf("error fetching labels: %v", err)
		}
	}

	var labels []string
	keys, err := result.Keys()
	if err != nil {
		return nil, fmt.Errorf("error getting result keys: %v", err)
	}

	if len(keys) > 0 && keys[0] == "name" {
		// Neo4j 4.x+ format - we need to get labels differently
		// Use a query to get all distinct labels from nodes
		labelQuery := "MATCH (n) UNWIND labels(n) AS label RETURN DISTINCT label"
		labelResult, err := session.Run(ctx, labelQuery, nil)
		if err != nil {
			return nil, fmt.Errorf("error fetching labels: %v", err)
		}

		for labelResult.Next(ctx) {
			record := labelResult.Record()
			label, _ := record.Get("label")
			if label != nil {
				if labelStr, ok := label.(string); ok {
					labels = append(labels, labelStr)
				} else {
					labels = append(labels, fmt.Sprintf("%v", label))
				}
			}
		}
	} else {
		// Neo4j 3.x format
		for result.Next(ctx) {
			record := result.Record()
			label, _ := record.Get("label")
			if label != nil {
				if labelStr, ok := label.(string); ok {
					labels = append(labels, labelStr)
				} else {
					labels = append(labels, fmt.Sprintf("%v", label))
				}
			}
		}
	}

	// For each label, get properties and count
	var labelInfos []LabelInfo
	for _, label := range labels {
		// Get count of nodes with this label
		countQuery := fmt.Sprintf("MATCH (n:`%s`) RETURN count(n) as count", label)
		countResult, err := session.Run(ctx, countQuery, nil)
		if err != nil {
			return nil, fmt.Errorf("error counting nodes with label %s: %v", label, err)
		}

		var count int64
		if countResult.Next(ctx) {
			record := countResult.Record()
			countVal, _ := record.Get("count")
			if countVal != nil {
				if countInt, ok := countVal.(int64); ok {
					count = countInt
				} else if countInt, ok := countVal.(int); ok {
					count = int64(countInt)
				} else {
					count = 0
				}
			}
		}

		// Get properties for this label
		propsQuery := fmt.Sprintf(
			"MATCH (n:`%s`) WHERE n IS NOT NULL "+
				"WITH n LIMIT 100 "+
				"UNWIND keys(n) AS prop "+
				"RETURN DISTINCT prop, "+
				"apoc.meta.type(n[prop]) AS type",
			label)

		propsResult, err := session.Run(ctx, propsQuery, nil)
		if err != nil {
			// If APOC is not available, try a simpler approach
			propsQuery = fmt.Sprintf(
				"MATCH (n:`%s`) WHERE n IS NOT NULL "+
					"WITH n LIMIT 100 "+
					"UNWIND keys(n) AS prop "+
					"RETURN DISTINCT prop",
				label)

			propsResult, err = session.Run(ctx, propsQuery, nil)
			if err != nil {
				return nil, fmt.Errorf("error fetching properties for label %s: %v", label, err)
			}
		}

		var properties []PropertyInfo
		for propsResult.Next(ctx) {
			record := propsResult.Record()
			prop, _ := record.Get("prop")

			propName := ""
			if prop != nil {
				if propStr, ok := prop.(string); ok {
					propName = propStr
				} else {
					propName = fmt.Sprintf("%v", prop)
				}
			}

			propInfo := PropertyInfo{
				Name:     propName,
				Nullable: true, // Neo4j properties are nullable by default
			}

			// Try to get type if available
			dataType, ok := record.Get("type")
			if ok && dataType != nil {
				propInfo.DataType = fmt.Sprintf("%v", dataType)
			} else {
				propInfo.DataType = "unknown"
			}

			properties = append(properties, propInfo)
		}

		labelInfos = append(labelInfos, LabelInfo{
			Name:       label,
			Properties: properties,
			Count:      count,
		})
	}

	return labelInfos, nil
}

func discoverRelationshipTypes(ctx context.Context, session neo4j.SessionWithContext) ([]RelationshipTypeInfo, error) {
	// Try Neo4j 4.x+ syntax first
	query := "SHOW DATABASE"
	result, err := session.Run(ctx, query, nil)
	if err != nil {
		// Try older syntax for Neo4j 3.x
		query = "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType"
		result, err = session.Run(ctx, query, nil)
		if err != nil {
			return nil, fmt.Errorf("error fetching relationship types: %v", err)
		}
	}

	var types []string
	keys, err := result.Keys()
	if err != nil {
		return nil, fmt.Errorf("error getting result keys: %v", err)
	}

	if len(keys) > 0 && keys[0] == "name" {
		// Neo4j 4.x+ format - we need to get relationship types differently
		// Use a query to get all distinct relationship types
		relTypeQuery := "MATCH ()-[r]->() RETURN DISTINCT type(r) AS relationshipType"
		relTypeResult, err := session.Run(ctx, relTypeQuery, nil)
		if err != nil {
			return nil, fmt.Errorf("error fetching relationship types: %v", err)
		}

		for relTypeResult.Next(ctx) {
			record := relTypeResult.Record()
			relType, _ := record.Get("relationshipType")
			if relType != nil {
				if relTypeStr, ok := relType.(string); ok {
					types = append(types, relTypeStr)
				} else {
					types = append(types, fmt.Sprintf("%v", relType))
				}
			}
		}
	} else {
		// Neo4j 3.x format
		for result.Next(ctx) {
			record := result.Record()
			relType, _ := record.Get("relationshipType")
			if relType != nil {
				if relTypeStr, ok := relType.(string); ok {
					types = append(types, relTypeStr)
				} else {
					types = append(types, fmt.Sprintf("%v", relType))
				}
			}
		}
	}

	// For each type, get properties and count
	var typeInfos []RelationshipTypeInfo
	for _, relType := range types {
		// Get count of relationships with this type
		countQuery := fmt.Sprintf("MATCH ()-[r:`%s`]->() RETURN count(r) as count", relType)
		countResult, err := session.Run(ctx, countQuery, nil)
		if err != nil {
			return nil, fmt.Errorf("error counting relationships with type %s: %v", relType, err)
		}

		var count int64
		if countResult.Next(ctx) {
			record := countResult.Record()
			countVal, _ := record.Get("count")
			if countVal != nil {
				if countInt, ok := countVal.(int64); ok {
					count = countInt
				} else if countInt, ok := countVal.(int); ok {
					count = int64(countInt)
				} else {
					count = 0
				}
			}
		}

		// Get properties for this relationship type
		propsQuery := fmt.Sprintf(
			"MATCH ()-[r:`%s`]->() WHERE r IS NOT NULL "+
				"WITH r LIMIT 100 "+
				"UNWIND keys(r) AS prop "+
				"RETURN DISTINCT prop, "+
				"apoc.meta.type(r[prop]) AS type",
			relType)

		propsResult, err := session.Run(ctx, propsQuery, nil)
		if err != nil {
			// If APOC is not available, try a simpler approach
			propsQuery = fmt.Sprintf(
				"MATCH ()-[r:`%s`]->() WHERE r IS NOT NULL "+
					"WITH r LIMIT 100 "+
					"UNWIND keys(r) AS prop "+
					"RETURN DISTINCT prop",
				relType)

			propsResult, err = session.Run(ctx, propsQuery, nil)
			if err != nil {
				return nil, fmt.Errorf("error fetching properties for relationship type %s: %v", relType, err)
			}
		}

		var properties []PropertyInfo
		for propsResult.Next(ctx) {
			record := propsResult.Record()
			prop, _ := record.Get("prop")

			propName := ""
			if prop != nil {
				if propStr, ok := prop.(string); ok {
					propName = propStr
				} else {
					propName = fmt.Sprintf("%v", prop)
				}
			}

			propInfo := PropertyInfo{
				Name:     propName,
				Nullable: true, // Neo4j properties are nullable by default
			}

			// Try to get type if available
			dataType, ok := record.Get("type")
			if ok && dataType != nil {
				propInfo.DataType = fmt.Sprintf("%v", dataType)
			} else {
				propInfo.DataType = "unknown"
			}

			properties = append(properties, propInfo)
		}

		// Try to determine common start and end labels
		labelsQuery := fmt.Sprintf(
			"MATCH (a)-[r:`%s`]->(b) "+
				"WITH labels(a) AS startLabels, labels(b) AS endLabels "+
				"LIMIT 100 "+
				"UNWIND startLabels AS startLabel "+
				"UNWIND endLabels AS endLabel "+
				"RETURN startLabel, endLabel, count(*) AS frequency "+
				"ORDER BY frequency DESC "+
				"LIMIT 1",
			relType)

		labelsResult, err := session.Run(ctx, labelsQuery, nil)
		if err != nil {
			// If there's an error, we'll just skip the label information
			typeInfos = append(typeInfos, RelationshipTypeInfo{
				Name:       relType,
				Properties: properties,
				Count:      count,
			})
			continue
		}

		var startLabel, endLabel string
		if labelsResult.Next(ctx) {
			record := labelsResult.Record()
			startLabelVal, _ := record.Get("startLabel")
			endLabelVal, _ := record.Get("endLabel")
			startLabel = fmt.Sprintf("%v", startLabelVal)
			endLabel = fmt.Sprintf("%v", endLabelVal)
		}

		typeInfos = append(typeInfos, RelationshipTypeInfo{
			Name:       relType,
			Properties: properties,
			Count:      count,
			StartLabel: startLabel,
			EndLabel:   endLabel,
		})
	}

	return typeInfos, nil
}

func discoverConstraints(ctx context.Context, session neo4j.SessionWithContext) ([]ConstraintInfo, error) {
	// The query depends on Neo4j version
	query := "SHOW CONSTRAINTS"

	result, err := session.Run(ctx, query, nil)
	if err != nil {
		// Try older syntax for Neo4j 3.x
		query = "CALL db.constraints()"
		result, err = session.Run(ctx, query, nil)
		if err != nil {
			return nil, fmt.Errorf("error fetching constraints: %v", err)
		}
	}

	var constraints []ConstraintInfo
	for result.Next(ctx) {
		record := result.Record()

		// Handle different formats based on Neo4j version
		keys := record.Keys

		if len(keys) > 0 && keys[0] == "name" {
			// Neo4j 4.x+ format
			name, _ := record.Get("name")
			constraintType, _ := record.Get("type")
			entityType, _ := record.Get("entityType")
			labelsOrTypes, _ := record.Get("labelsOrTypes")
			properties, _ := record.Get("properties")

			var labelOrType string
			if labelsOrTypesArr, ok := labelsOrTypes.([]interface{}); ok && len(labelsOrTypesArr) > 0 {
				labelOrType = fmt.Sprintf("%v", labelsOrTypesArr[0])
			}

			var propertyKeys []string
			if propertiesArr, ok := properties.([]interface{}); ok {
				for _, prop := range propertiesArr {
					propertyKeys = append(propertyKeys, fmt.Sprintf("%v", prop))
				}
			}

			isRelationship := entityType == "RELATIONSHIP"

			constraints = append(constraints, ConstraintInfo{
				Name:           fmt.Sprintf("%v", name),
				Type:           fmt.Sprintf("%v", constraintType),
				LabelOrType:    labelOrType,
				PropertyKeys:   propertyKeys,
				IsRelationship: isRelationship,
			})
		} else {
			// Neo4j 3.x format
			description, _ := record.Get("description")
			descStr := fmt.Sprintf("%v", description)

			// Parse the description to extract information
			constraint := parseConstraintDescription(descStr)
			constraints = append(constraints, constraint)
		}
	}

	return constraints, nil
}

func parseConstraintDescription(description string) ConstraintInfo {
	constraint := ConstraintInfo{}

	// Example: "CONSTRAINT ON ( book:Book ) ASSERT book.isbn IS UNIQUE"
	description = strings.TrimSpace(description)

	if strings.Contains(description, "ASSERT") && strings.Contains(description, "IS UNIQUE") {
		constraint.Type = "UNIQUENESS"
	} else if strings.Contains(description, "ASSERT exists") {
		constraint.Type = "NODE_PROPERTY_EXISTENCE"
	} else if strings.Contains(description, "ASSERT relationship") {
		constraint.Type = "RELATIONSHIP_PROPERTY_EXISTENCE"
		constraint.IsRelationship = true
	}

	// Extract label or type
	if constraint.IsRelationship {
		// For relationship constraints
		startIdx := strings.Index(description, "CONSTRAINT ON ") + 14
		endIdx := strings.Index(description[startIdx:], ")")
		if endIdx > 0 {
			relTypeInfo := description[startIdx : startIdx+endIdx]
			parts := strings.Split(relTypeInfo, ":")
			if len(parts) > 1 {
				constraint.LabelOrType = strings.TrimSpace(parts[1])
			}
		}
	} else {
		// For node constraints
		startIdx := strings.Index(description, ":")
		if startIdx > 0 {
			endIdx := strings.Index(description[startIdx:], ")")
			if endIdx > 0 {
				constraint.LabelOrType = description[startIdx+1 : startIdx+endIdx]
			}
		}
	}

	// Extract property keys
	propStartIdx := strings.LastIndex(description, ".")
	if propStartIdx > 0 {
		propEndIdx := strings.Index(description[propStartIdx:], " ")
		if propEndIdx > 0 {
			propName := description[propStartIdx+1 : propStartIdx+propEndIdx]
			constraint.PropertyKeys = []string{propName}
		}
	}

	// Generate a name if none exists
	if constraint.Name == "" {
		if constraint.IsRelationship {
			constraint.Name = fmt.Sprintf("constraint_%s_rel_%s", constraint.Type, constraint.LabelOrType)
		} else {
			constraint.Name = fmt.Sprintf("constraint_%s_%s", constraint.Type, constraint.LabelOrType)
		}
		if len(constraint.PropertyKeys) > 0 {
			constraint.Name += "_" + strings.Join(constraint.PropertyKeys, "_")
		}
	}

	return constraint
}

func discoverIndexes(ctx context.Context, session neo4j.SessionWithContext) ([]common.IndexInfo, error) {
	// The query depends on Neo4j version
	query := "SHOW INDEXES"

	result, err := session.Run(ctx, query, nil)
	if err != nil {
		// Try older syntax for Neo4j 3.x
		query = "CALL db.indexes()"
		result, err = session.Run(ctx, query, nil)
		if err != nil {
			return nil, fmt.Errorf("error fetching indexes: %v", err)
		}
	}

	var indexes []common.IndexInfo
	for result.Next(ctx) {
		record := result.Record()

		// Handle different formats based on Neo4j version
		keys := record.Keys

		if len(keys) > 0 && keys[0] == "name" {
			// Neo4j 4.x+ format
			name, _ := record.Get("name")
			indexType, _ := record.Get("type")
			properties, _ := record.Get("properties")

			var columns []string
			if propertiesArr, ok := properties.([]interface{}); ok {
				for _, prop := range propertiesArr {
					columns = append(columns, fmt.Sprintf("%v", prop))
				}
			}

			isUnique := fmt.Sprintf("%v", indexType) == "UNIQUE"

			indexes = append(indexes, common.IndexInfo{
				Name:     fmt.Sprintf("%v", name),
				Columns:  columns,
				IsUnique: isUnique,
			})
		} else {
			// Neo4j 3.x format
			description, _ := record.Get("description")
			descStr := fmt.Sprintf("%v", description)

			// Parse the description to extract information
			index := parseIndexDescription(descStr)
			indexes = append(indexes, index)
		}
	}

	return indexes, nil
}

func parseIndexDescription(description string) common.IndexInfo {
	index := common.IndexInfo{}

	// Example: "INDEX ON :Book(isbn)"
	description = strings.TrimSpace(description)

	// Extract name
	if strings.HasPrefix(description, "INDEX ON") {
		// Extract property names
		startIdx := strings.Index(description, "(")
		endIdx := strings.Index(description, ")")
		if startIdx > 0 && endIdx > startIdx {
			propertyStr := description[startIdx+1 : endIdx]
			properties := strings.Split(propertyStr, ",")
			for i, prop := range properties {
				properties[i] = strings.TrimSpace(prop)
			}
			index.Columns = properties
		}

		// Extract label
		labelStartIdx := strings.Index(description, ":")
		labelEndIdx := strings.Index(description, "(")
		if labelStartIdx > 0 && labelEndIdx > labelStartIdx {
			label := description[labelStartIdx+1 : labelEndIdx]

			// Generate a name based on label and properties
			index.Name = fmt.Sprintf("index_%s_%s",
				strings.TrimSpace(label),
				strings.Join(index.Columns, "_"))
		}
	}

	// Check if it's a unique index
	index.IsUnique = strings.Contains(strings.ToLower(description), "unique")

	return index
}

func discoverProcedures(ctx context.Context, session neo4j.SessionWithContext) ([]common.ProcedureInfo, error) {
	// Try Neo4j 4.x+ syntax first
	query := "SHOW PROCEDURES"
	result, err := session.Run(ctx, query, nil)
	if err != nil {
		// Try older syntax for Neo4j 3.x
		query = "CALL dbms.procedures() YIELD name, signature, description RETURN name, signature, description"
		result, err = session.Run(ctx, query, nil)
		if err != nil {
			return nil, fmt.Errorf("error fetching procedures: %v", err)
		}
	}

	var procedures []common.ProcedureInfo
	for result.Next(ctx) {
		record := result.Record()

		// Handle different formats based on Neo4j version
		keys := record.Keys

		if len(keys) > 0 && keys[0] == "name" {
			// Neo4j 4.x+ format
			name, _ := record.Get("name")
			signature, _ := record.Get("signature")
			description, _ := record.Get("description")

			// Safely convert values to strings with nil checks
			nameStr := ""
			if name != nil {
				if nameStrVal, ok := name.(string); ok {
					nameStr = nameStrVal
				} else {
					nameStr = fmt.Sprintf("%v", name)
				}
			}

			signatureStr := ""
			if signature != nil {
				if sigStrVal, ok := signature.(string); ok {
					signatureStr = sigStrVal
				} else {
					signatureStr = fmt.Sprintf("%v", signature)
				}
			}

			descriptionStr := ""
			if description != nil {
				if descStrVal, ok := description.(string); ok {
					descriptionStr = descStrVal
				} else {
					descriptionStr = fmt.Sprintf("%v", description)
				}
			}

			// Parse the name to extract namespace/schema
			nameParts := strings.Split(nameStr, ".")
			var schema string
			if len(nameParts) > 1 {
				schema = strings.Join(nameParts[:len(nameParts)-1], ".")
			}

			procedureName := nameStr
			if len(nameParts) > 0 {
				procedureName = nameParts[len(nameParts)-1]
			}

			procedures = append(procedures, common.ProcedureInfo{
				Name:              procedureName,
				Schema:            schema,
				Arguments:         signatureStr,
				RoutineDefinition: descriptionStr,
			})
		} else {
			// Neo4j 3.x format
			name, _ := record.Get("name")
			signature, _ := record.Get("signature")
			description, _ := record.Get("description")

			// Safely convert values to strings with nil checks
			nameStr := ""
			if name != nil {
				if nameStrVal, ok := name.(string); ok {
					nameStr = nameStrVal
				} else {
					nameStr = fmt.Sprintf("%v", name)
				}
			}

			signatureStr := ""
			if signature != nil {
				if sigStrVal, ok := signature.(string); ok {
					signatureStr = sigStrVal
				} else {
					signatureStr = fmt.Sprintf("%v", signature)
				}
			}

			descriptionStr := ""
			if description != nil {
				if descStrVal, ok := description.(string); ok {
					descriptionStr = descStrVal
				} else {
					descriptionStr = fmt.Sprintf("%v", description)
				}
			}

			// Parse the name to extract namespace/schema
			nameParts := strings.Split(nameStr, ".")
			var schema string
			if len(nameParts) > 1 {
				schema = strings.Join(nameParts[:len(nameParts)-1], ".")
			}

			procedureName := nameStr
			if len(nameParts) > 0 {
				procedureName = nameParts[len(nameParts)-1]
			}

			procedures = append(procedures, common.ProcedureInfo{
				Name:              procedureName,
				Schema:            schema,
				Arguments:         signatureStr,
				RoutineDefinition: descriptionStr,
			})
		}
	}

	return procedures, nil
}

func discoverFunctions(ctx context.Context, session neo4j.SessionWithContext) ([]common.FunctionInfo, error) {
	// Try Neo4j 4.x+ syntax first
	query := "SHOW FUNCTIONS"
	result, err := session.Run(ctx, query, nil)
	if err != nil {
		// Try older syntax for Neo4j 3.x
		query = "CALL dbms.functions() YIELD name, signature, description RETURN name, signature, description"
		result, err = session.Run(ctx, query, nil)
		if err != nil {
			return nil, fmt.Errorf("error fetching functions: %v", err)
		}
	}

	var functions []common.FunctionInfo
	for result.Next(ctx) {
		record := result.Record()

		// Handle different formats based on Neo4j version
		keys := record.Keys

		if len(keys) > 0 && keys[0] == "name" {
			// Neo4j 4.x+ format
			name, _ := record.Get("name")
			signature, _ := record.Get("signature")
			description, _ := record.Get("description")

			// Safely convert values to strings with nil checks
			nameStr := ""
			if name != nil {
				if nameStrVal, ok := name.(string); ok {
					nameStr = nameStrVal
				} else {
					nameStr = fmt.Sprintf("%v", name)
				}
			}

			signatureStr := ""
			if signature != nil {
				if sigStrVal, ok := signature.(string); ok {
					signatureStr = sigStrVal
				} else {
					signatureStr = fmt.Sprintf("%v", signature)
				}
			}

			descriptionStr := ""
			if description != nil {
				if descStrVal, ok := description.(string); ok {
					descriptionStr = descStrVal
				} else {
					descriptionStr = fmt.Sprintf("%v", description)
				}
			}

			// Parse the name to extract namespace/schema
			nameParts := strings.Split(nameStr, ".")
			var schema string
			if len(nameParts) > 1 {
				schema = strings.Join(nameParts[:len(nameParts)-1], ".")
			}

			functionName := nameStr
			if len(nameParts) > 0 {
				functionName = nameParts[len(nameParts)-1]
			}

			// Extract return type from signature if possible
			returnType := "unknown"
			returnTypeIdx := strings.LastIndex(signatureStr, "::")
			if returnTypeIdx > 0 {
				returnType = signatureStr[returnTypeIdx+2:]
			}

			functions = append(functions, common.FunctionInfo{
				Name:       functionName,
				Schema:     schema,
				Arguments:  signatureStr,
				ReturnType: returnType,
				Body:       descriptionStr,
			})
		} else {
			// Neo4j 3.x format
			name, _ := record.Get("name")
			signature, _ := record.Get("signature")
			description, _ := record.Get("description")

			// Safely convert values to strings with nil checks
			nameStr := ""
			if name != nil {
				if nameStrVal, ok := name.(string); ok {
					nameStr = nameStrVal
				} else {
					nameStr = fmt.Sprintf("%v", name)
				}
			}

			signatureStr := ""
			if signature != nil {
				if sigStrVal, ok := signature.(string); ok {
					signatureStr = sigStrVal
				} else {
					signatureStr = fmt.Sprintf("%v", signature)
				}
			}

			descriptionStr := ""
			if description != nil {
				if descStrVal, ok := description.(string); ok {
					descriptionStr = descStrVal
				} else {
					descriptionStr = fmt.Sprintf("%v", description)
				}
			}

			// Parse the name to extract namespace/schema
			nameParts := strings.Split(nameStr, ".")
			var schema string
			if len(nameParts) > 1 {
				schema = strings.Join(nameParts[:len(nameParts)-1], ".")
			}

			functionName := nameStr
			if len(nameParts) > 0 {
				functionName = nameParts[len(nameParts)-1]
			}

			// Extract return type from signature if possible
			returnType := "unknown"
			returnTypeIdx := strings.LastIndex(signatureStr, "::")
			if returnTypeIdx > 0 {
				returnType = signatureStr[returnTypeIdx+2:]
			}

			functions = append(functions, common.FunctionInfo{
				Name:       functionName,
				Schema:     schema,
				Arguments:  signatureStr,
				ReturnType: returnType,
				Body:       descriptionStr,
			})
		}
	}

	return functions, nil
}

func createConstraint(ctx context.Context, session neo4j.SessionWithContext, constraint common.Constraint) error {
	var query string

	switch constraint.Type {
	case "UNIQUENESS":
		if constraint.IsRelationship {
			return fmt.Errorf("uniqueness constraints are not supported on relationships")
		}
		query = fmt.Sprintf("CREATE CONSTRAINT %s IF NOT EXISTS ON (n:`%s`) ASSERT n.%s IS UNIQUE",
			constraint.Name, constraint.LabelOrType, strings.Join(constraint.PropertyKeys, ", n."))

	case "NODE_PROPERTY_EXISTENCE":
		query = fmt.Sprintf("CREATE CONSTRAINT %s IF NOT EXISTS ON (n:`%s`) ASSERT exists(n.%s)",
			constraint.Name, constraint.LabelOrType, strings.Join(constraint.PropertyKeys, "), exists(n."))

	case "RELATIONSHIP_PROPERTY_EXISTENCE":
		query = fmt.Sprintf("CREATE CONSTRAINT %s IF NOT EXISTS ON ()-[r:`%s`]-() ASSERT exists(r.%s)",
			constraint.Name, constraint.LabelOrType, strings.Join(constraint.PropertyKeys, "), exists(r."))

	default:
		return fmt.Errorf("unsupported constraint type: %s", constraint.Type)
	}

	_, err := session.Run(ctx, query, nil)
	return err
}

func createIndex(ctx context.Context, session neo4j.SessionWithContext, index common.IndexInfo) error {
	// Extract label from index name (assuming format like "index_Label_prop1_prop2")
	parts := strings.Split(index.Name, "_")
	if len(parts) < 2 {
		return fmt.Errorf("invalid index name format: %s", index.Name)
	}

	label := parts[1]

	var query string
	if index.IsUnique {
		// Unique indexes are better created as constraints in Neo4j
		query = fmt.Sprintf("CREATE CONSTRAINT %s IF NOT EXISTS ON (n:`%s`) ASSERT n.%s IS UNIQUE",
			index.Name, label, strings.Join(index.Columns, ", n."))
	} else {
		query = fmt.Sprintf("CREATE INDEX %s IF NOT EXISTS FOR (n:`%s`) ON (n.%s)",
			index.Name, label, strings.Join(index.Columns, ", n."))
	}

	_, err := session.Run(ctx, query, nil)
	return err
}
