package comparison

import (
	"fmt"

	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// UnifiedSchemaComparator handles comparison of UnifiedModel objects directly
type UnifiedSchemaComparator struct{}

// NewUnifiedSchemaComparator creates a new unified schema comparator
func NewUnifiedSchemaComparator() *UnifiedSchemaComparator {
	return &UnifiedSchemaComparator{}
}

// CompareResult represents the result of a schema comparison
type UnifiedCompareResult struct {
	HasChanges bool
	Changes    []string
	Warnings   []string
}

// CompareUnifiedModels compares two UnifiedModel objects directly
func (c *UnifiedSchemaComparator) CompareUnifiedModels(previousModel, currentModel *unifiedmodel.UnifiedModel) (*UnifiedCompareResult, error) {
	result := &UnifiedCompareResult{
		Changes:  make([]string, 0),
		Warnings: make([]string, 0),
	}

	// Ensure we have valid models to compare
	if previousModel == nil {
		previousModel = c.createEmptyUnifiedModel()
	}

	if currentModel == nil {
		currentModel = c.createEmptyUnifiedModel()
	}

	// Compare all schema components - organized by category

	// Structural organization
	c.compareCatalogs(previousModel, currentModel, result)
	c.compareDatabases(previousModel, currentModel, result)
	c.compareSchemas(previousModel, currentModel, result)

	// Primary Data Containers
	c.compareTables(previousModel, currentModel, result)
	c.compareCollections(previousModel, currentModel, result)
	c.compareNodes(previousModel, currentModel, result)
	c.compareMemoryTables(previousModel, currentModel, result)

	// Temporary Data Containers
	c.compareTemporaryTables(previousModel, currentModel, result)
	c.compareTransientTables(previousModel, currentModel, result)
	c.compareCaches(previousModel, currentModel, result)

	// Virtual Data Containers
	c.compareViews(previousModel, currentModel, result)
	c.compareLiveViews(previousModel, currentModel, result)
	c.compareWindowViews(previousModel, currentModel, result)
	c.compareMaterializedViews(previousModel, currentModel, result)
	c.compareExternalTables(previousModel, currentModel, result)
	c.compareForeignTables(previousModel, currentModel, result)

	// Graph / Vector / Search abstractions
	c.compareGraphs(previousModel, currentModel, result)
	c.compareVectorIndexes(previousModel, currentModel, result)
	c.compareSearchIndexes(previousModel, currentModel, result)

	// Specialized Data Containers
	c.compareVectors(previousModel, currentModel, result)
	c.compareEmbeddings(previousModel, currentModel, result)
	c.compareDocuments(previousModel, currentModel, result)
	c.compareEmbeddedDocuments(previousModel, currentModel, result)
	c.compareRelationships(previousModel, currentModel, result)
	c.comparePaths(previousModel, currentModel, result)

	// Data Organization Containers
	c.comparePartitions(previousModel, currentModel, result)
	c.compareSubPartitions(previousModel, currentModel, result)
	c.compareShards(previousModel, currentModel, result)
	c.compareKeyspaces(previousModel, currentModel, result)
	c.compareNamespaces(previousModel, currentModel, result)

	// Structural definition objects
	// Note: Columns, indexes, and constraints are compared within table comparison
	c.compareTypes(previousModel, currentModel, result)
	c.comparePropertyKeys(previousModel, currentModel, result)

	// Integrity, performance and identity objects
	c.compareSequences(previousModel, currentModel, result)
	c.compareIdentities(previousModel, currentModel, result)
	c.compareUUIDGenerators(previousModel, currentModel, result)

	// Executable code objects
	c.compareFunctions(previousModel, currentModel, result)
	c.compareProcedures(previousModel, currentModel, result)
	c.compareMethods(previousModel, currentModel, result)
	c.compareTriggers(previousModel, currentModel, result)
	c.compareEventTriggers(previousModel, currentModel, result)
	c.compareAggregates(previousModel, currentModel, result)
	c.compareOperators(previousModel, currentModel, result)
	c.compareModules(previousModel, currentModel, result)
	c.comparePackages(previousModel, currentModel, result)
	c.comparePackageBodies(previousModel, currentModel, result)
	c.compareMacros(previousModel, currentModel, result)
	c.compareRules(previousModel, currentModel, result)
	c.compareWindowFuncs(previousModel, currentModel, result)

	// Security and access control
	c.compareUsers(previousModel, currentModel, result)
	c.compareRoles(previousModel, currentModel, result)
	c.compareGrants(previousModel, currentModel, result)
	c.comparePolicies(previousModel, currentModel, result)

	// Physical storage and placement
	c.compareTablespaces(previousModel, currentModel, result)
	c.compareSegments(previousModel, currentModel, result)
	c.compareExtents(previousModel, currentModel, result)
	c.comparePages(previousModel, currentModel, result)
	c.compareFilegroups(previousModel, currentModel, result)
	c.compareDatafiles(previousModel, currentModel, result)

	// Connectivity and integration
	c.compareServers(previousModel, currentModel, result)
	c.compareConnections(previousModel, currentModel, result)
	c.compareEndpoints(previousModel, currentModel, result)
	c.compareForeignDataWrappers(previousModel, currentModel, result)
	c.compareUserMappings(previousModel, currentModel, result)
	c.compareFederations(previousModel, currentModel, result)
	c.compareReplicas(previousModel, currentModel, result)
	c.compareClusters(previousModel, currentModel, result)

	// Operational, pipelines and streaming
	c.compareTasks(previousModel, currentModel, result)
	c.compareJobs(previousModel, currentModel, result)
	c.compareSchedules(previousModel, currentModel, result)
	c.comparePipelines(previousModel, currentModel, result)
	c.compareStreams(previousModel, currentModel, result)

	// Monitoring and alerting
	c.compareEvents(previousModel, currentModel, result)
	c.compareNotifications(previousModel, currentModel, result)
	c.compareAlerts(previousModel, currentModel, result)
	c.compareStatistics(previousModel, currentModel, result)
	c.compareHistograms(previousModel, currentModel, result)
	c.compareMonitors(previousModel, currentModel, result)
	c.compareMonitorMetrics(previousModel, currentModel, result)
	c.compareThresholds(previousModel, currentModel, result)

	// Text processing / search configuration
	c.compareTextSearchComponents(previousModel, currentModel, result)

	// Metadata and documentation
	c.compareComments(previousModel, currentModel, result)
	c.compareAnnotations(previousModel, currentModel, result)
	c.compareTags(previousModel, currentModel, result)
	c.compareAliases(previousModel, currentModel, result)
	c.compareSynonyms(previousModel, currentModel, result)
	c.compareLabels(previousModel, currentModel, result)

	// Backup and recovery, versioning
	c.compareSnapshots(previousModel, currentModel, result)
	c.compareBackups(previousModel, currentModel, result)
	c.compareArchives(previousModel, currentModel, result)
	c.compareRecoveryPoints(previousModel, currentModel, result)
	c.compareVersions(previousModel, currentModel, result)
	c.compareMigrations(previousModel, currentModel, result)
	c.compareBranches(previousModel, currentModel, result)
	c.compareTimeTravel(previousModel, currentModel, result)

	// Extensions and customization
	c.compareExtensions(previousModel, currentModel, result)
	c.comparePlugins(previousModel, currentModel, result)
	c.compareModuleExtensions(previousModel, currentModel, result)
	c.compareTTLSettings(previousModel, currentModel, result)
	c.compareDimensions(previousModel, currentModel, result)
	c.compareDistanceMetrics(previousModel, currentModel, result)

	// Advanced analytics
	c.compareProjections(previousModel, currentModel, result)
	c.compareAnalyticsAggs(previousModel, currentModel, result)

	result.HasChanges = len(result.Changes) > 0
	return result, nil
}

func (c *UnifiedSchemaComparator) compareSchemas(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed schemas
	for schemaName := range prevModel.Schemas {
		if _, exists := currModel.Schemas[schemaName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed schema: %s", schemaName))
		}
	}

	// Check for added and modified schemas
	for schemaName, currSchema := range currModel.Schemas {
		if prevSchema, exists := prevModel.Schemas[schemaName]; exists {
			// Compare existing schema
			if prevSchema.Comment != currSchema.Comment {
				result.Changes = append(result.Changes, fmt.Sprintf("Schema %s comment changed: %s -> %s",
					schemaName, prevSchema.Comment, currSchema.Comment))
			}
		} else {
			// New schema
			result.Changes = append(result.Changes, fmt.Sprintf("Added schema: %s", schemaName))
		}
	}
}

func (c *UnifiedSchemaComparator) compareTables(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed tables
	for tableName := range prevModel.Tables {
		if _, exists := currModel.Tables[tableName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed table: %s", tableName))
		}
	}

	// Check for added and modified tables
	for tableName, currTable := range currModel.Tables {
		if prevTable, exists := prevModel.Tables[tableName]; exists {
			// Compare existing table
			c.compareTableStructure(tableName, prevTable, currTable, result)
		} else {
			// New table
			result.Changes = append(result.Changes, fmt.Sprintf("Added table: %s", tableName))
		}
	}
}

func (c *UnifiedSchemaComparator) compareTableStructure(tableName string, prevTable, currTable unifiedmodel.Table, result *UnifiedCompareResult) {
	// Compare table comment
	if prevTable.Comment != currTable.Comment {
		result.Changes = append(result.Changes, fmt.Sprintf("Table %s comment changed: %s -> %s",
			tableName, prevTable.Comment, currTable.Comment))
	}

	// Compare columns
	c.compareColumns(tableName, prevTable.Columns, currTable.Columns, result)

	// Compare indexes
	c.compareIndexes(tableName, prevTable.Indexes, currTable.Indexes, result)

	// Compare constraints
	c.compareConstraints(tableName, prevTable.Constraints, currTable.Constraints, result)
}

func (c *UnifiedSchemaComparator) compareColumns(tableName string, prevColumns, currColumns map[string]unifiedmodel.Column, result *UnifiedCompareResult) {
	// Check for removed columns
	for columnName := range prevColumns {
		if _, exists := currColumns[columnName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed column: %s.%s", tableName, columnName))
		}
	}

	// Check for added and modified columns
	for columnName, currColumn := range currColumns {
		if prevColumn, exists := prevColumns[columnName]; exists {
			// Compare existing column
			if prevColumn.DataType != currColumn.DataType {
				result.Changes = append(result.Changes, fmt.Sprintf("Column %s.%s data type changed: %s -> %s",
					tableName, columnName, prevColumn.DataType, currColumn.DataType))
			}
			if prevColumn.Nullable != currColumn.Nullable {
				result.Changes = append(result.Changes, fmt.Sprintf("Column %s.%s nullable changed: %t -> %t",
					tableName, columnName, prevColumn.Nullable, currColumn.Nullable))
			}
			if prevColumn.Default != currColumn.Default {
				result.Changes = append(result.Changes, fmt.Sprintf("Column %s.%s default changed: %s -> %s",
					tableName, columnName, prevColumn.Default, currColumn.Default))
			}
			if prevColumn.IsPrimaryKey != currColumn.IsPrimaryKey {
				result.Changes = append(result.Changes, fmt.Sprintf("Column %s.%s primary key changed: %t -> %t",
					tableName, columnName, prevColumn.IsPrimaryKey, currColumn.IsPrimaryKey))
			}
			if prevColumn.AutoIncrement != currColumn.AutoIncrement {
				result.Changes = append(result.Changes, fmt.Sprintf("Column %s.%s auto increment changed: %t -> %t",
					tableName, columnName, prevColumn.AutoIncrement, currColumn.AutoIncrement))
			}
		} else {
			// New column
			result.Changes = append(result.Changes, fmt.Sprintf("Added column: %s.%s", tableName, columnName))
		}
	}
}

func (c *UnifiedSchemaComparator) compareIndexes(tableName string, prevIndexes, currIndexes map[string]unifiedmodel.Index, result *UnifiedCompareResult) {
	// Check for removed indexes
	for indexName := range prevIndexes {
		if _, exists := currIndexes[indexName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed index: %s.%s", tableName, indexName))
		}
	}

	// Check for added and modified indexes
	for indexName, currIndex := range currIndexes {
		if prevIndex, exists := prevIndexes[indexName]; exists {
			// Compare existing index
			if prevIndex.Unique != currIndex.Unique {
				result.Changes = append(result.Changes, fmt.Sprintf("Index %s.%s unique changed: %t -> %t",
					tableName, indexName, prevIndex.Unique, currIndex.Unique))
			}
			// Compare columns (simplified)
			if len(prevIndex.Columns) != len(currIndex.Columns) {
				result.Changes = append(result.Changes, fmt.Sprintf("Index %s.%s columns changed", tableName, indexName))
			}
		} else {
			// New index
			result.Changes = append(result.Changes, fmt.Sprintf("Added index: %s.%s", tableName, indexName))
		}
	}
}

func (c *UnifiedSchemaComparator) compareConstraints(tableName string, prevConstraints, currConstraints map[string]unifiedmodel.Constraint, result *UnifiedCompareResult) {
	// Check for removed constraints
	for constraintName := range prevConstraints {
		if _, exists := currConstraints[constraintName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed constraint: %s.%s", tableName, constraintName))
		}
	}

	// Check for added and modified constraints
	for constraintName, currConstraint := range currConstraints {
		if prevConstraint, exists := prevConstraints[constraintName]; exists {
			// Compare existing constraint
			if prevConstraint.Type != currConstraint.Type {
				result.Changes = append(result.Changes, fmt.Sprintf("Constraint %s.%s type changed: %s -> %s",
					tableName, constraintName, prevConstraint.Type, currConstraint.Type))
			}
			if prevConstraint.Expression != currConstraint.Expression {
				result.Changes = append(result.Changes, fmt.Sprintf("Constraint %s.%s expression changed",
					tableName, constraintName))
			}
		} else {
			// New constraint
			result.Changes = append(result.Changes, fmt.Sprintf("Added constraint: %s.%s", tableName, constraintName))
		}
	}
}

func (c *UnifiedSchemaComparator) compareTypes(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed types
	for typeName := range prevModel.Types {
		if _, exists := currModel.Types[typeName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed type: %s", typeName))
		}
	}

	// Check for added and modified types
	for typeName, currType := range currModel.Types {
		if prevType, exists := prevModel.Types[typeName]; exists {
			// Compare existing type
			if prevType.Category != currType.Category {
				result.Changes = append(result.Changes, fmt.Sprintf("Type %s category changed: %s -> %s",
					typeName, prevType.Category, currType.Category))
			}
			// Compare definition (simplified)
			if fmt.Sprintf("%v", prevType.Definition) != fmt.Sprintf("%v", currType.Definition) {
				result.Changes = append(result.Changes, fmt.Sprintf("Type %s definition changed", typeName))
			}
		} else {
			// New type
			result.Changes = append(result.Changes, fmt.Sprintf("Added type: %s", typeName))
		}
	}
}

func (c *UnifiedSchemaComparator) compareFunctions(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed functions
	for functionName := range prevModel.Functions {
		if _, exists := currModel.Functions[functionName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed function: %s", functionName))
		}
	}

	// Check for added and modified functions
	for functionName, currFunction := range currModel.Functions {
		if prevFunction, exists := prevModel.Functions[functionName]; exists {
			// Compare existing function
			if prevFunction.Definition != currFunction.Definition {
				result.Changes = append(result.Changes, fmt.Sprintf("Function %s definition changed", functionName))
			}
			if prevFunction.Returns != currFunction.Returns {
				result.Changes = append(result.Changes, fmt.Sprintf("Function %s return type changed: %s -> %s",
					functionName, prevFunction.Returns, currFunction.Returns))
			}
		} else {
			// New function
			result.Changes = append(result.Changes, fmt.Sprintf("Added function: %s", functionName))
		}
	}
}

func (c *UnifiedSchemaComparator) compareTriggers(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed triggers
	for triggerName := range prevModel.Triggers {
		if _, exists := currModel.Triggers[triggerName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed trigger: %s", triggerName))
		}
	}

	// Check for added and modified triggers
	for triggerName, currTrigger := range currModel.Triggers {
		if prevTrigger, exists := prevModel.Triggers[triggerName]; exists {
			// Compare existing trigger
			if prevTrigger.Procedure != currTrigger.Procedure {
				result.Changes = append(result.Changes, fmt.Sprintf("Trigger %s procedure changed", triggerName))
			}
			if prevTrigger.Timing != currTrigger.Timing {
				result.Changes = append(result.Changes, fmt.Sprintf("Trigger %s timing changed: %s -> %s",
					triggerName, prevTrigger.Timing, currTrigger.Timing))
			}
			if prevTrigger.Table != currTrigger.Table {
				result.Changes = append(result.Changes, fmt.Sprintf("Trigger %s table changed: %s -> %s",
					triggerName, prevTrigger.Table, currTrigger.Table))
			}
			// Compare events slice
			if len(prevTrigger.Events) != len(currTrigger.Events) {
				result.Changes = append(result.Changes, fmt.Sprintf("Trigger %s events changed", triggerName))
			} else {
				// Check if events are different (order-sensitive comparison)
				eventsChanged := false
				for i, prevEvent := range prevTrigger.Events {
					if i >= len(currTrigger.Events) || prevEvent != currTrigger.Events[i] {
						eventsChanged = true
						break
					}
				}
				if eventsChanged {
					result.Changes = append(result.Changes, fmt.Sprintf("Trigger %s events changed", triggerName))
				}
			}
		} else {
			// New trigger
			result.Changes = append(result.Changes, fmt.Sprintf("Added trigger: %s", triggerName))
		}
	}
}

func (c *UnifiedSchemaComparator) compareSequences(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed sequences
	for sequenceName := range prevModel.Sequences {
		if _, exists := currModel.Sequences[sequenceName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed sequence: %s", sequenceName))
		}
	}

	// Check for added and modified sequences
	for sequenceName, currSequence := range currModel.Sequences {
		if prevSequence, exists := prevModel.Sequences[sequenceName]; exists {
			// Compare existing sequence
			if prevSequence.Start != currSequence.Start {
				result.Changes = append(result.Changes, fmt.Sprintf("Sequence %s start changed: %d -> %d",
					sequenceName, prevSequence.Start, currSequence.Start))
			}
			if prevSequence.Increment != currSequence.Increment {
				result.Changes = append(result.Changes, fmt.Sprintf("Sequence %s increment changed: %d -> %d",
					sequenceName, prevSequence.Increment, currSequence.Increment))
			}
			if prevSequence.Cycle != currSequence.Cycle {
				result.Changes = append(result.Changes, fmt.Sprintf("Sequence %s cycle changed: %t -> %t",
					sequenceName, prevSequence.Cycle, currSequence.Cycle))
			}
			// Compare Min and Max (pointer values)
			if (prevSequence.Min == nil) != (currSequence.Min == nil) {
				result.Changes = append(result.Changes, fmt.Sprintf("Sequence %s min value changed", sequenceName))
			} else if prevSequence.Min != nil && currSequence.Min != nil && *prevSequence.Min != *currSequence.Min {
				result.Changes = append(result.Changes, fmt.Sprintf("Sequence %s min changed: %d -> %d",
					sequenceName, *prevSequence.Min, *currSequence.Min))
			}
			if (prevSequence.Max == nil) != (currSequence.Max == nil) {
				result.Changes = append(result.Changes, fmt.Sprintf("Sequence %s max value changed", sequenceName))
			} else if prevSequence.Max != nil && currSequence.Max != nil && *prevSequence.Max != *currSequence.Max {
				result.Changes = append(result.Changes, fmt.Sprintf("Sequence %s max changed: %d -> %d",
					sequenceName, *prevSequence.Max, *currSequence.Max))
			}
			// Compare Cache (pointer value)
			if (prevSequence.Cache == nil) != (currSequence.Cache == nil) {
				result.Changes = append(result.Changes, fmt.Sprintf("Sequence %s cache value changed", sequenceName))
			} else if prevSequence.Cache != nil && currSequence.Cache != nil && *prevSequence.Cache != *currSequence.Cache {
				result.Changes = append(result.Changes, fmt.Sprintf("Sequence %s cache changed: %d -> %d",
					sequenceName, *prevSequence.Cache, *currSequence.Cache))
			}
		} else {
			// New sequence
			result.Changes = append(result.Changes, fmt.Sprintf("Added sequence: %s", sequenceName))
		}
	}
}

func (c *UnifiedSchemaComparator) compareExtensions(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed extensions
	for extensionName := range prevModel.Extensions {
		if _, exists := currModel.Extensions[extensionName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed extension: %s", extensionName))
		}
	}

	// Check for added and modified extensions
	for extensionName, currExtension := range currModel.Extensions {
		if prevExtension, exists := prevModel.Extensions[extensionName]; exists {
			// Compare existing extension
			if prevExtension.Version != currExtension.Version {
				result.Changes = append(result.Changes, fmt.Sprintf("Extension %s version changed: %s -> %s",
					extensionName, prevExtension.Version, currExtension.Version))
			}
		} else {
			// New extension
			result.Changes = append(result.Changes, fmt.Sprintf("Added extension: %s", extensionName))
		}
	}
}

// compareStringSlices compares two string slices for equality (order-sensitive)
func (c *UnifiedSchemaComparator) compareStringSlices(prev, curr []string) bool {
	if len(prev) != len(curr) {
		return false
	}
	for i, v := range prev {
		if v != curr[i] {
			return false
		}
	}
	return true
}

// Helper functions for detailed object comparisons (non-generic approach)

// Structural organization comparisons
func (c *UnifiedSchemaComparator) compareCatalogs(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed catalogs
	for catalogName := range prevModel.Catalogs {
		if _, exists := currModel.Catalogs[catalogName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed catalog: %s", catalogName))
		}
	}

	// Check for added and modified catalogs
	for catalogName, currCatalog := range currModel.Catalogs {
		if prevCatalog, exists := prevModel.Catalogs[catalogName]; exists {
			// Compare existing catalog
			if prevCatalog.Comment != currCatalog.Comment {
				result.Changes = append(result.Changes, fmt.Sprintf("Catalog %s comment changed: %s -> %s", catalogName, prevCatalog.Comment, currCatalog.Comment))
			}
		} else {
			// New catalog
			result.Changes = append(result.Changes, fmt.Sprintf("Added catalog: %s", catalogName))
		}
	}
}

func (c *UnifiedSchemaComparator) compareDatabases(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed databases
	for databaseName := range prevModel.Databases {
		if _, exists := currModel.Databases[databaseName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed database: %s", databaseName))
		}
	}

	// Check for added and modified databases
	for databaseName, currDatabase := range currModel.Databases {
		if prevDatabase, exists := prevModel.Databases[databaseName]; exists {
			// Compare existing database
			if prevDatabase.Comment != currDatabase.Comment {
				result.Changes = append(result.Changes, fmt.Sprintf("Database %s comment changed: %s -> %s", databaseName, prevDatabase.Comment, currDatabase.Comment))
			}
			if prevDatabase.Owner != currDatabase.Owner {
				result.Changes = append(result.Changes, fmt.Sprintf("Database %s owner changed: %s -> %s", databaseName, prevDatabase.Owner, currDatabase.Owner))
			}
			if prevDatabase.DefaultSchema != currDatabase.DefaultSchema {
				result.Changes = append(result.Changes, fmt.Sprintf("Database %s default schema changed: %s -> %s", databaseName, prevDatabase.DefaultSchema, currDatabase.DefaultSchema))
			}
		} else {
			// New database
			result.Changes = append(result.Changes, fmt.Sprintf("Added database: %s", databaseName))
		}
	}
}

// Primary Data Containers comparisons
func (c *UnifiedSchemaComparator) compareCollections(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed collections
	for collectionName := range prevModel.Collections {
		if _, exists := currModel.Collections[collectionName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed collection: %s", collectionName))
		}
	}

	// Check for added and modified collections
	for collectionName, currCollection := range currModel.Collections {
		if prevCollection, exists := prevModel.Collections[collectionName]; exists {
			// Compare existing collection details
			if prevCollection.Owner != currCollection.Owner {
				result.Changes = append(result.Changes, fmt.Sprintf("Collection %s owner changed: %s -> %s", collectionName, prevCollection.Owner, currCollection.Owner))
			}
			if prevCollection.Comment != currCollection.Comment {
				result.Changes = append(result.Changes, fmt.Sprintf("Collection %s comment changed: %s -> %s", collectionName, prevCollection.Comment, currCollection.Comment))
			}
			// Compare shard key (slice comparison)
			if len(prevCollection.ShardKey) != len(currCollection.ShardKey) {
				result.Changes = append(result.Changes, fmt.Sprintf("Collection %s shard key changed", collectionName))
			} else {
				for i, prevKey := range prevCollection.ShardKey {
					if i >= len(currCollection.ShardKey) || prevKey != currCollection.ShardKey[i] {
						result.Changes = append(result.Changes, fmt.Sprintf("Collection %s shard key changed", collectionName))
						break
					}
				}
			}
			// Compare fields and indexes (simplified for now - could be enhanced further)
			if len(prevCollection.Fields) != len(currCollection.Fields) {
				result.Changes = append(result.Changes, fmt.Sprintf("Collection %s fields changed", collectionName))
			}
			if len(prevCollection.Indexes) != len(currCollection.Indexes) {
				result.Changes = append(result.Changes, fmt.Sprintf("Collection %s indexes changed", collectionName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added collection: %s", collectionName))
		}
	}
}

func (c *UnifiedSchemaComparator) compareNodes(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed nodes
	for nodeName := range prevModel.Nodes {
		if _, exists := currModel.Nodes[nodeName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed node: %s", nodeName))
		}
	}

	// Check for added and modified nodes
	for nodeName, currNode := range currModel.Nodes {
		if prevNode, exists := prevModel.Nodes[nodeName]; exists {
			// Compare existing node details
			if prevNode.Label != currNode.Label {
				result.Changes = append(result.Changes, fmt.Sprintf("Node %s label changed: %s -> %s", nodeName, prevNode.Label, currNode.Label))
			}
			// Compare properties and indexes (simplified for now - could be enhanced further)
			if len(prevNode.Properties) != len(currNode.Properties) {
				result.Changes = append(result.Changes, fmt.Sprintf("Node %s properties changed", nodeName))
			}
			if len(prevNode.Indexes) != len(currNode.Indexes) {
				result.Changes = append(result.Changes, fmt.Sprintf("Node %s indexes changed", nodeName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added node: %s", nodeName))
		}
	}
}

func (c *UnifiedSchemaComparator) compareMemoryTables(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed memory tables
	for memoryTableName := range prevModel.MemoryTables {
		if _, exists := currModel.MemoryTables[memoryTableName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed memory table: %s", memoryTableName))
		}
	}

	// Check for added and modified memory tables
	for memoryTableName, currMemoryTable := range currModel.MemoryTables {
		if prevMemoryTable, exists := prevModel.MemoryTables[memoryTableName]; exists {
			// Compare existing memory table details
			// Compare columns using the existing compareColumns function
			c.compareColumns(memoryTableName, prevMemoryTable.Columns, currMemoryTable.Columns, result)
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added memory table: %s", memoryTableName))
		}
	}
}

// Temporary Data Containers comparisons
func (c *UnifiedSchemaComparator) compareTemporaryTables(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed temporary tables
	for tableName := range prevModel.TemporaryTables {
		if _, exists := currModel.TemporaryTables[tableName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed temporary table: %s", tableName))
		}
	}

	// Check for added and modified temporary tables
	for tableName, currTable := range currModel.TemporaryTables {
		if prevTable, exists := prevModel.TemporaryTables[tableName]; exists {
			// Compare existing temporary table details
			if prevTable.Name != currTable.Name {
				result.Changes = append(result.Changes, fmt.Sprintf("Temporary table %s name changed: %s -> %s", tableName, prevTable.Name, currTable.Name))
			}
			if prevTable.Scope != currTable.Scope {
				result.Changes = append(result.Changes, fmt.Sprintf("Temporary table %s scope changed: %s -> %s", tableName, prevTable.Scope, currTable.Scope))
			}
			// Compare columns within the temporary table
			c.compareColumns(tableName, prevTable.Columns, currTable.Columns, result)
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added temporary table: %s", tableName))
		}
	}
}

func (c *UnifiedSchemaComparator) compareTransientTables(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed transient tables
	for tableName := range prevModel.TransientTables {
		if _, exists := currModel.TransientTables[tableName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed transient table: %s", tableName))
		}
	}

	// Check for added and modified transient tables
	for tableName, currTable := range currModel.TransientTables {
		if prevTable, exists := prevModel.TransientTables[tableName]; exists {
			// Compare existing transient table details
			if prevTable.Name != currTable.Name {
				result.Changes = append(result.Changes, fmt.Sprintf("Transient table %s name changed: %s -> %s", tableName, prevTable.Name, currTable.Name))
			}
			// Compare columns within the transient table
			c.compareColumns(tableName, prevTable.Columns, currTable.Columns, result)
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added transient table: %s", tableName))
		}
	}
}

func (c *UnifiedSchemaComparator) compareCaches(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed caches
	for cacheName := range prevModel.Caches {
		if _, exists := currModel.Caches[cacheName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed cache: %s", cacheName))
		}
	}

	// Check for added and modified caches
	for cacheName, currCache := range currModel.Caches {
		if prevCache, exists := prevModel.Caches[cacheName]; exists {
			// Compare existing cache details
			if prevCache.Name != currCache.Name {
				result.Changes = append(result.Changes, fmt.Sprintf("Cache %s name changed: %s -> %s", cacheName, prevCache.Name, currCache.Name))
			}
			if prevCache.Scope != currCache.Scope {
				result.Changes = append(result.Changes, fmt.Sprintf("Cache %s scope changed: %s -> %s", cacheName, prevCache.Scope, currCache.Scope))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added cache: %s", cacheName))
		}
	}
}

// Virtual Data Containers comparisons
func (c *UnifiedSchemaComparator) compareViews(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed views
	for viewName := range prevModel.Views {
		if _, exists := currModel.Views[viewName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed view: %s", viewName))
		}
	}

	// Check for added and modified views
	for viewName, currView := range currModel.Views {
		if prevView, exists := prevModel.Views[viewName]; exists {
			// Compare existing view details
			if prevView.Definition != currView.Definition {
				result.Changes = append(result.Changes, fmt.Sprintf("View %s definition changed", viewName))
			}
			if prevView.Comment != currView.Comment {
				result.Changes = append(result.Changes, fmt.Sprintf("View %s comment changed: %s -> %s", viewName, prevView.Comment, currView.Comment))
			}
			// Compare columns within the view
			c.compareColumns(viewName, prevView.Columns, currView.Columns, result)
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added view: %s", viewName))
		}
	}
}

func (c *UnifiedSchemaComparator) compareLiveViews(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed live views
	for viewName := range prevModel.LiveViews {
		if _, exists := currModel.LiveViews[viewName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed live view: %s", viewName))
		}
	}

	// Check for added and modified live views
	for viewName, currView := range currModel.LiveViews {
		if prevView, exists := prevModel.LiveViews[viewName]; exists {
			// Compare existing live view details
			if prevView.Definition != currView.Definition {
				result.Changes = append(result.Changes, fmt.Sprintf("Live view %s definition changed", viewName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added live view: %s", viewName))
		}
	}
}

// Implement all remaining comparison functions with simple count-based comparisons
func (c *UnifiedSchemaComparator) compareWindowViews(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed window views
	for viewName := range prevModel.WindowViews {
		if _, exists := currModel.WindowViews[viewName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed window view: %s", viewName))
		}
	}

	// Check for added and modified window views
	for viewName, currView := range currModel.WindowViews {
		if prevView, exists := prevModel.WindowViews[viewName]; exists {
			// Compare existing window view details
			if prevView.Name != currView.Name {
				result.Changes = append(result.Changes, fmt.Sprintf("Window view %s name changed: %s -> %s", viewName, prevView.Name, currView.Name))
			}
			if prevView.Definition != currView.Definition {
				result.Changes = append(result.Changes, fmt.Sprintf("Window view %s definition changed", viewName))
			}
			if prevView.WindowSpec != currView.WindowSpec {
				result.Changes = append(result.Changes, fmt.Sprintf("Window view %s window specification changed: %s -> %s", viewName, prevView.WindowSpec, currView.WindowSpec))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added window view: %s", viewName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareMaterializedViews(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed materialized views
	for viewName := range prevModel.MaterializedViews {
		if _, exists := currModel.MaterializedViews[viewName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed materialized view: %s", viewName))
		}
	}

	// Check for added and modified materialized views
	for viewName, currView := range currModel.MaterializedViews {
		if prevView, exists := prevModel.MaterializedViews[viewName]; exists {
			// Compare existing materialized view details
			if prevView.Definition != currView.Definition {
				result.Changes = append(result.Changes, fmt.Sprintf("Materialized view %s definition changed", viewName))
			}
			// Note: MaterializedView type may not have Comment field - check type definition
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added materialized view: %s", viewName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareExternalTables(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed external tables
	for tableName := range prevModel.ExternalTables {
		if _, exists := currModel.ExternalTables[tableName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed external table: %s", tableName))
		}
	}

	// Check for added and modified external tables
	for tableName, currTable := range currModel.ExternalTables {
		if prevTable, exists := prevModel.ExternalTables[tableName]; exists {
			// Compare existing external table details
			if prevTable.Location != currTable.Location {
				result.Changes = append(result.Changes, fmt.Sprintf("External table %s location changed: %s -> %s", tableName, prevTable.Location, currTable.Location))
			}
			if prevTable.Format != currTable.Format {
				result.Changes = append(result.Changes, fmt.Sprintf("External table %s format changed: %s -> %s", tableName, prevTable.Format, currTable.Format))
			}
			// Compare columns using existing column comparison logic
			c.compareColumns(tableName, prevTable.Columns, currTable.Columns, result)
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added external table: %s", tableName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareForeignTables(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed foreign tables
	for tableName := range prevModel.ForeignTables {
		if _, exists := currModel.ForeignTables[tableName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed foreign table: %s", tableName))
		}
	}

	// Check for added and modified foreign tables
	for tableName, currTable := range currModel.ForeignTables {
		if prevTable, exists := prevModel.ForeignTables[tableName]; exists {
			// Compare existing foreign table details
			if prevTable.Server != currTable.Server {
				result.Changes = append(result.Changes, fmt.Sprintf("Foreign table %s server changed: %s -> %s", tableName, prevTable.Server, currTable.Server))
			}
			// Compare columns using existing column comparison logic
			c.compareColumns(tableName, prevTable.Columns, currTable.Columns, result)
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added foreign table: %s", tableName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareGraphs(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed graphs
	for graphName := range prevModel.Graphs {
		if _, exists := currModel.Graphs[graphName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed graph: %s", graphName))
		}
	}

	// Check for added and modified graphs
	for graphName, currGraph := range currModel.Graphs {
		if prevGraph, exists := prevModel.Graphs[graphName]; exists {
			// Compare existing graph details
			// Compare node labels (simplified - count-based for now)
			if len(prevGraph.NodeLabels) != len(currGraph.NodeLabels) {
				result.Changes = append(result.Changes, fmt.Sprintf("Graph %s node labels changed", graphName))
			}
			// Compare relationship types (simplified - count-based for now)
			if len(prevGraph.RelTypes) != len(currGraph.RelTypes) {
				result.Changes = append(result.Changes, fmt.Sprintf("Graph %s relationship types changed", graphName))
			}
			// Compare indexes using existing index comparison logic
			c.compareIndexes(graphName, prevGraph.Indexes, currGraph.Indexes, result)
			// Compare constraints using existing constraint comparison logic
			c.compareConstraints(graphName, prevGraph.Constraints, currGraph.Constraints, result)
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added graph: %s", graphName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareVectorIndexes(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed vector indexes
	for indexName := range prevModel.VectorIndexes {
		if _, exists := currModel.VectorIndexes[indexName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed vector index: %s", indexName))
		}
	}

	// Check for added and modified vector indexes
	for indexName, currIndex := range currModel.VectorIndexes {
		if prevIndex, exists := prevModel.VectorIndexes[indexName]; exists {
			// Compare existing vector index details
			if prevIndex.On != currIndex.On {
				result.Changes = append(result.Changes, fmt.Sprintf("Vector index %s target changed: %s -> %s", indexName, prevIndex.On, currIndex.On))
			}
			if prevIndex.Metric != currIndex.Metric {
				result.Changes = append(result.Changes, fmt.Sprintf("Vector index %s metric changed: %s -> %s", indexName, prevIndex.Metric, currIndex.Metric))
			}
			if prevIndex.Dimension != currIndex.Dimension {
				result.Changes = append(result.Changes, fmt.Sprintf("Vector index %s dimension changed: %d -> %d", indexName, prevIndex.Dimension, currIndex.Dimension))
			}
			// Compare fields (order-sensitive)
			if !c.compareStringSlices(prevIndex.Fields, currIndex.Fields) {
				result.Changes = append(result.Changes, fmt.Sprintf("Vector index %s fields changed", indexName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added vector index: %s", indexName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareSearchIndexes(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed search indexes
	for indexName := range prevModel.SearchIndexes {
		if _, exists := currModel.SearchIndexes[indexName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed search index: %s", indexName))
		}
	}

	// Check for added and modified search indexes
	for indexName, currIndex := range currModel.SearchIndexes {
		if prevIndex, exists := prevModel.SearchIndexes[indexName]; exists {
			// Compare existing search index details
			if prevIndex.On != currIndex.On {
				result.Changes = append(result.Changes, fmt.Sprintf("Search index %s target changed: %s -> %s", indexName, prevIndex.On, currIndex.On))
			}
			if prevIndex.Analyzer != currIndex.Analyzer {
				result.Changes = append(result.Changes, fmt.Sprintf("Search index %s analyzer changed: %s -> %s", indexName, prevIndex.Analyzer, currIndex.Analyzer))
			}
			// Compare fields (order-sensitive)
			if !c.compareStringSlices(prevIndex.Fields, currIndex.Fields) {
				result.Changes = append(result.Changes, fmt.Sprintf("Search index %s fields changed", indexName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added search index: %s", indexName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareVectors(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed vectors
	for vectorName := range prevModel.Vectors {
		if _, exists := currModel.Vectors[vectorName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed vector: %s", vectorName))
		}
	}

	// Check for added and modified vectors
	for vectorName, currVector := range currModel.Vectors {
		if prevVector, exists := prevModel.Vectors[vectorName]; exists {
			// Compare existing vector details
			if prevVector.Dimension != currVector.Dimension {
				result.Changes = append(result.Changes, fmt.Sprintf("Vector %s dimension changed: %d -> %d", vectorName, prevVector.Dimension, currVector.Dimension))
			}
			if prevVector.Metric != currVector.Metric {
				result.Changes = append(result.Changes, fmt.Sprintf("Vector %s metric changed: %s -> %s", vectorName, prevVector.Metric, currVector.Metric))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added vector: %s", vectorName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareEmbeddings(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed embeddings
	for embeddingName := range prevModel.Embeddings {
		if _, exists := currModel.Embeddings[embeddingName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed embedding: %s", embeddingName))
		}
	}

	// Check for added and modified embeddings
	for embeddingName, currEmbedding := range currModel.Embeddings {
		if prevEmbedding, exists := prevModel.Embeddings[embeddingName]; exists {
			// Compare existing embedding details
			if prevEmbedding.Model != currEmbedding.Model {
				result.Changes = append(result.Changes, fmt.Sprintf("Embedding %s model changed: %s -> %s", embeddingName, prevEmbedding.Model, currEmbedding.Model))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added embedding: %s", embeddingName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareDocuments(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed documents
	for documentKey := range prevModel.Documents {
		if _, exists := currModel.Documents[documentKey]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed document: %s", documentKey))
		}
	}

	// Check for added and modified documents
	for documentKey, currDocument := range currModel.Documents {
		if prevDocument, exists := prevModel.Documents[documentKey]; exists {
			// Compare existing document details
			// Compare fields (simplified - could be enhanced for deep field comparison)
			if len(prevDocument.Fields) != len(currDocument.Fields) {
				result.Changes = append(result.Changes, fmt.Sprintf("Document %s fields changed", documentKey))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added document: %s", documentKey))
		}
	}
}
func (c *UnifiedSchemaComparator) compareEmbeddedDocuments(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed embedded documents
	for embeddedDocName := range prevModel.EmbeddedDocuments {
		if _, exists := currModel.EmbeddedDocuments[embeddedDocName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed embedded document: %s", embeddedDocName))
		}
	}

	// Check for added and modified embedded documents
	for embeddedDocName, currEmbeddedDoc := range currModel.EmbeddedDocuments {
		if prevEmbeddedDoc, exists := prevModel.EmbeddedDocuments[embeddedDocName]; exists {
			// Compare existing embedded document details
			// Compare fields (simplified - could be enhanced for deep field comparison)
			if len(prevEmbeddedDoc.Fields) != len(currEmbeddedDoc.Fields) {
				result.Changes = append(result.Changes, fmt.Sprintf("Embedded document %s fields changed", embeddedDocName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added embedded document: %s", embeddedDocName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareRelationships(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed relationships
	for relationshipName := range prevModel.Relationships {
		if _, exists := currModel.Relationships[relationshipName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed relationship: %s", relationshipName))
		}
	}

	// Check for added and modified relationships
	for relationshipName, currRelationship := range currModel.Relationships {
		if prevRelationship, exists := prevModel.Relationships[relationshipName]; exists {
			// Compare existing relationship details
			if prevRelationship.Type != currRelationship.Type {
				result.Changes = append(result.Changes, fmt.Sprintf("Relationship %s type changed: %s -> %s", relationshipName, prevRelationship.Type, currRelationship.Type))
			}
			if prevRelationship.FromLabel != currRelationship.FromLabel {
				result.Changes = append(result.Changes, fmt.Sprintf("Relationship %s from label changed: %s -> %s", relationshipName, prevRelationship.FromLabel, currRelationship.FromLabel))
			}
			if prevRelationship.ToLabel != currRelationship.ToLabel {
				result.Changes = append(result.Changes, fmt.Sprintf("Relationship %s to label changed: %s -> %s", relationshipName, prevRelationship.ToLabel, currRelationship.ToLabel))
			}
			// Compare properties (simplified - could be enhanced for detailed property comparison)
			if len(prevRelationship.Properties) != len(currRelationship.Properties) {
				result.Changes = append(result.Changes, fmt.Sprintf("Relationship %s properties changed", relationshipName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added relationship: %s", relationshipName))
		}
	}
}
func (c *UnifiedSchemaComparator) comparePaths(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed paths
	for pathName := range prevModel.Paths {
		if _, exists := currModel.Paths[pathName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed path: %s", pathName))
		}
	}

	// Check for added and modified paths
	for pathName, currPath := range currModel.Paths {
		if prevPath, exists := prevModel.Paths[pathName]; exists {
			// Compare existing path details
			// Compare sequence (order-sensitive)
			if !c.compareStringSlices(prevPath.Sequence, currPath.Sequence) {
				result.Changes = append(result.Changes, fmt.Sprintf("Path %s sequence changed", pathName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added path: %s", pathName))
		}
	}
}
func (c *UnifiedSchemaComparator) comparePartitions(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed partitions
	for partitionName := range prevModel.Partitions {
		if _, exists := currModel.Partitions[partitionName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed partition: %s", partitionName))
		}
	}

	// Check for added and modified partitions
	for partitionName, currPartition := range currModel.Partitions {
		if prevPartition, exists := prevModel.Partitions[partitionName]; exists {
			// Compare existing partition details
			if prevPartition.Type != currPartition.Type {
				result.Changes = append(result.Changes, fmt.Sprintf("Partition %s type changed: %s -> %s", partitionName, prevPartition.Type, currPartition.Type))
			}
			// Compare key (order-sensitive)
			if !c.compareStringSlices(prevPartition.Key, currPartition.Key) {
				result.Changes = append(result.Changes, fmt.Sprintf("Partition %s key changed", partitionName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added partition: %s", partitionName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareSubPartitions(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed sub-partitions
	for subPartitionName := range prevModel.SubPartitions {
		if _, exists := currModel.SubPartitions[subPartitionName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed sub-partition: %s", subPartitionName))
		}
	}

	// Check for added and modified sub-partitions
	for subPartitionName, currSubPartition := range currModel.SubPartitions {
		if prevSubPartition, exists := prevModel.SubPartitions[subPartitionName]; exists {
			// Compare existing sub-partition details
			if prevSubPartition.Type != currSubPartition.Type {
				result.Changes = append(result.Changes, fmt.Sprintf("Sub-partition %s type changed: %s -> %s", subPartitionName, prevSubPartition.Type, currSubPartition.Type))
			}
			// Compare key (order-sensitive)
			if !c.compareStringSlices(prevSubPartition.Key, currSubPartition.Key) {
				result.Changes = append(result.Changes, fmt.Sprintf("Sub-partition %s key changed", subPartitionName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added sub-partition: %s", subPartitionName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareShards(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed shards
	for shardName := range prevModel.Shards {
		if _, exists := currModel.Shards[shardName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed shard: %s", shardName))
		}
	}

	// Check for added and modified shards
	for shardName, currShard := range currModel.Shards {
		if prevShard, exists := prevModel.Shards[shardName]; exists {
			// Compare existing shard details
			if prevShard.Strategy != currShard.Strategy {
				result.Changes = append(result.Changes, fmt.Sprintf("Shard %s strategy changed: %s -> %s", shardName, prevShard.Strategy, currShard.Strategy))
			}
			// Compare key (order-sensitive)
			if !c.compareStringSlices(prevShard.Key, currShard.Key) {
				result.Changes = append(result.Changes, fmt.Sprintf("Shard %s key changed", shardName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added shard: %s", shardName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareKeyspaces(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed keyspaces
	for keyspaceName := range prevModel.Keyspaces {
		if _, exists := currModel.Keyspaces[keyspaceName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed keyspace: %s", keyspaceName))
		}
	}

	// Check for added and modified keyspaces
	for keyspaceName, currKeyspace := range currModel.Keyspaces {
		if prevKeyspace, exists := prevModel.Keyspaces[keyspaceName]; exists {
			// Compare existing keyspace details
			if prevKeyspace.ReplicationStrategy != currKeyspace.ReplicationStrategy {
				result.Changes = append(result.Changes, fmt.Sprintf("Keyspace %s replication strategy changed: %s -> %s", keyspaceName, prevKeyspace.ReplicationStrategy, currKeyspace.ReplicationStrategy))
			}
			if prevKeyspace.DurableWrites != currKeyspace.DurableWrites {
				result.Changes = append(result.Changes, fmt.Sprintf("Keyspace %s durable writes changed: %t -> %t", keyspaceName, prevKeyspace.DurableWrites, currKeyspace.DurableWrites))
			}
			// Compare replication options (simplified - could be enhanced for detailed comparison)
			if len(prevKeyspace.ReplicationOptions) != len(currKeyspace.ReplicationOptions) {
				result.Changes = append(result.Changes, fmt.Sprintf("Keyspace %s replication options changed", keyspaceName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added keyspace: %s", keyspaceName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareNamespaces(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed namespaces
	for namespaceName := range prevModel.Namespaces {
		if _, exists := currModel.Namespaces[namespaceName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed namespace: %s", namespaceName))
		}
	}

	// Check for added and modified namespaces
	for namespaceName, currNamespace := range currModel.Namespaces {
		if prevNamespace, exists := prevModel.Namespaces[namespaceName]; exists {
			// Compare existing namespace details
			// Compare labels (simplified - could be enhanced for detailed comparison)
			if len(prevNamespace.Labels) != len(currNamespace.Labels) {
				result.Changes = append(result.Changes, fmt.Sprintf("Namespace %s labels changed", namespaceName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added namespace: %s", namespaceName))
		}
	}
}
func (c *UnifiedSchemaComparator) comparePropertyKeys(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed property keys
	for propertyKeyName := range prevModel.PropertyKeys {
		if _, exists := currModel.PropertyKeys[propertyKeyName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed property key: %s", propertyKeyName))
		}
	}

	// Check for added and modified property keys
	for propertyKeyName, currPropertyKey := range currModel.PropertyKeys {
		if prevPropertyKey, exists := prevModel.PropertyKeys[propertyKeyName]; exists {
			// Compare existing property key details
			if prevPropertyKey.Type != currPropertyKey.Type {
				result.Changes = append(result.Changes, fmt.Sprintf("Property key %s type changed: %s -> %s", propertyKeyName, prevPropertyKey.Type, currPropertyKey.Type))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added property key: %s", propertyKeyName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareIdentities(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed identities
	for identityName := range prevModel.Identities {
		if _, exists := currModel.Identities[identityName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed identity: %s", identityName))
		}
	}

	// Check for added and modified identities
	for identityName, currIdentity := range currModel.Identities {
		if prevIdentity, exists := prevModel.Identities[identityName]; exists {
			// Compare existing identity details
			if prevIdentity.Table != currIdentity.Table {
				result.Changes = append(result.Changes, fmt.Sprintf("Identity %s table changed: %s -> %s", identityName, prevIdentity.Table, currIdentity.Table))
			}
			if prevIdentity.Column != currIdentity.Column {
				result.Changes = append(result.Changes, fmt.Sprintf("Identity %s column changed: %s -> %s", identityName, prevIdentity.Column, currIdentity.Column))
			}
			if prevIdentity.Strategy != currIdentity.Strategy {
				result.Changes = append(result.Changes, fmt.Sprintf("Identity %s strategy changed: %s -> %s", identityName, prevIdentity.Strategy, currIdentity.Strategy))
			}
			if prevIdentity.Start != currIdentity.Start {
				result.Changes = append(result.Changes, fmt.Sprintf("Identity %s start changed: %d -> %d", identityName, prevIdentity.Start, currIdentity.Start))
			}
			if prevIdentity.Increment != currIdentity.Increment {
				result.Changes = append(result.Changes, fmt.Sprintf("Identity %s increment changed: %d -> %d", identityName, prevIdentity.Increment, currIdentity.Increment))
			}
			if prevIdentity.Cycle != currIdentity.Cycle {
				result.Changes = append(result.Changes, fmt.Sprintf("Identity %s cycle changed: %t -> %t", identityName, prevIdentity.Cycle, currIdentity.Cycle))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added identity: %s", identityName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareUUIDGenerators(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed UUID generators
	for generatorName := range prevModel.UUIDGenerators {
		if _, exists := currModel.UUIDGenerators[generatorName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed UUID generator: %s", generatorName))
		}
	}

	// Check for added and modified UUID generators
	for generatorName, currGenerator := range currModel.UUIDGenerators {
		if prevGenerator, exists := prevModel.UUIDGenerators[generatorName]; exists {
			// Compare existing UUID generator details
			if prevGenerator.Version != currGenerator.Version {
				result.Changes = append(result.Changes, fmt.Sprintf("UUID generator %s version changed: %s -> %s", generatorName, prevGenerator.Version, currGenerator.Version))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added UUID generator: %s", generatorName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareProcedures(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed procedures
	for procedureName := range prevModel.Procedures {
		if _, exists := currModel.Procedures[procedureName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed procedure: %s", procedureName))
		}
	}

	// Check for added and modified procedures
	for procedureName, currProcedure := range currModel.Procedures {
		if prevProcedure, exists := prevModel.Procedures[procedureName]; exists {
			// Compare existing procedure details
			if prevProcedure.Language != currProcedure.Language {
				result.Changes = append(result.Changes, fmt.Sprintf("Procedure %s language changed: %s -> %s", procedureName, prevProcedure.Language, currProcedure.Language))
			}
			if prevProcedure.Definition != currProcedure.Definition {
				result.Changes = append(result.Changes, fmt.Sprintf("Procedure %s definition changed", procedureName))
			}
			// Compare arguments (simplified - could be enhanced for detailed argument comparison)
			if len(prevProcedure.Arguments) != len(currProcedure.Arguments) {
				result.Changes = append(result.Changes, fmt.Sprintf("Procedure %s arguments changed", procedureName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added procedure: %s", procedureName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareMethods(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed methods
	for methodName := range prevModel.Methods {
		if _, exists := currModel.Methods[methodName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed method: %s", methodName))
		}
	}

	// Check for added and modified methods
	for methodName, currMethod := range currModel.Methods {
		if prevMethod, exists := prevModel.Methods[methodName]; exists {
			// Compare existing method details
			if prevMethod.OfType != currMethod.OfType {
				result.Changes = append(result.Changes, fmt.Sprintf("Method %s object type changed: %s -> %s", methodName, prevMethod.OfType, currMethod.OfType))
			}
			if prevMethod.Language != currMethod.Language {
				result.Changes = append(result.Changes, fmt.Sprintf("Method %s language changed: %s -> %s", methodName, prevMethod.Language, currMethod.Language))
			}
			if prevMethod.Definition != currMethod.Definition {
				result.Changes = append(result.Changes, fmt.Sprintf("Method %s definition changed", methodName))
			}
			// Compare arguments (simplified - could be enhanced for detailed argument comparison)
			if len(prevMethod.Arguments) != len(currMethod.Arguments) {
				result.Changes = append(result.Changes, fmt.Sprintf("Method %s arguments changed", methodName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added method: %s", methodName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareEventTriggers(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed event triggers
	for eventTriggerName := range prevModel.EventTriggers {
		if _, exists := currModel.EventTriggers[eventTriggerName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed event trigger: %s", eventTriggerName))
		}
	}

	// Check for added and modified event triggers
	for eventTriggerName, currEventTrigger := range currModel.EventTriggers {
		if prevEventTrigger, exists := prevModel.EventTriggers[eventTriggerName]; exists {
			// Compare existing event trigger details
			if prevEventTrigger.Scope != currEventTrigger.Scope {
				result.Changes = append(result.Changes, fmt.Sprintf("Event trigger %s scope changed: %s -> %s", eventTriggerName, prevEventTrigger.Scope, currEventTrigger.Scope))
			}
			if prevEventTrigger.Procedure != currEventTrigger.Procedure {
				result.Changes = append(result.Changes, fmt.Sprintf("Event trigger %s procedure changed: %s -> %s", eventTriggerName, prevEventTrigger.Procedure, currEventTrigger.Procedure))
			}
			// Compare events (order-sensitive)
			if !c.compareStringSlices(prevEventTrigger.Events, currEventTrigger.Events) {
				result.Changes = append(result.Changes, fmt.Sprintf("Event trigger %s events changed", eventTriggerName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added event trigger: %s", eventTriggerName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareAggregates(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed aggregates
	for aggregateName := range prevModel.Aggregates {
		if _, exists := currModel.Aggregates[aggregateName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed aggregate: %s", aggregateName))
		}
	}

	// Check for added and modified aggregates
	for aggregateName, currAggregate := range currModel.Aggregates {
		if prevAggregate, exists := prevModel.Aggregates[aggregateName]; exists {
			// Compare existing aggregate details
			if prevAggregate.StateType != currAggregate.StateType {
				result.Changes = append(result.Changes, fmt.Sprintf("Aggregate %s state type changed: %s -> %s", aggregateName, prevAggregate.StateType, currAggregate.StateType))
			}
			if prevAggregate.FinalType != currAggregate.FinalType {
				result.Changes = append(result.Changes, fmt.Sprintf("Aggregate %s final type changed: %s -> %s", aggregateName, prevAggregate.FinalType, currAggregate.FinalType))
			}
			// Compare input types (order-sensitive)
			if !c.compareStringSlices(prevAggregate.InputTypes, currAggregate.InputTypes) {
				result.Changes = append(result.Changes, fmt.Sprintf("Aggregate %s input types changed", aggregateName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added aggregate: %s", aggregateName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareOperators(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed operators
	for operatorName := range prevModel.Operators {
		if _, exists := currModel.Operators[operatorName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed operator: %s", operatorName))
		}
	}

	// Check for added and modified operators
	for operatorName, currOperator := range currModel.Operators {
		if prevOperator, exists := prevModel.Operators[operatorName]; exists {
			// Compare existing operator details
			if prevOperator.LeftType != currOperator.LeftType {
				result.Changes = append(result.Changes, fmt.Sprintf("Operator %s left type changed: %s -> %s", operatorName, prevOperator.LeftType, currOperator.LeftType))
			}
			if prevOperator.RightType != currOperator.RightType {
				result.Changes = append(result.Changes, fmt.Sprintf("Operator %s right type changed: %s -> %s", operatorName, prevOperator.RightType, currOperator.RightType))
			}
			if prevOperator.Returns != currOperator.Returns {
				result.Changes = append(result.Changes, fmt.Sprintf("Operator %s return type changed: %s -> %s", operatorName, prevOperator.Returns, currOperator.Returns))
			}
			if prevOperator.Definition != currOperator.Definition {
				result.Changes = append(result.Changes, fmt.Sprintf("Operator %s definition changed", operatorName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added operator: %s", operatorName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareModules(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed modules
	for moduleName := range prevModel.Modules {
		if _, exists := currModel.Modules[moduleName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed module: %s", moduleName))
		}
	}

	// Check for added and modified modules
	for moduleName, currModule := range currModel.Modules {
		if prevModule, exists := prevModel.Modules[moduleName]; exists {
			// Compare existing module details
			if prevModule.Comment != currModule.Comment {
				result.Changes = append(result.Changes, fmt.Sprintf("Module %s comment changed", moduleName))
			}
			if prevModule.Language != currModule.Language {
				result.Changes = append(result.Changes, fmt.Sprintf("Module %s language changed: %s -> %s", moduleName, prevModule.Language, currModule.Language))
			}
			if prevModule.Code != currModule.Code {
				result.Changes = append(result.Changes, fmt.Sprintf("Module %s code changed", moduleName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added module: %s", moduleName))
		}
	}
}
func (c *UnifiedSchemaComparator) comparePackages(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed packages
	for packageName := range prevModel.Packages {
		if _, exists := currModel.Packages[packageName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed package: %s", packageName))
		}
	}

	// Check for added and modified packages
	for packageName, currPackage := range currModel.Packages {
		if prevPackage, exists := prevModel.Packages[packageName]; exists {
			// Compare existing package details
			if prevPackage.Spec != currPackage.Spec {
				result.Changes = append(result.Changes, fmt.Sprintf("Package %s specification changed", packageName))
			}
			if prevPackage.Body != currPackage.Body {
				result.Changes = append(result.Changes, fmt.Sprintf("Package %s body changed", packageName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added package: %s", packageName))
		}
	}
}
func (c *UnifiedSchemaComparator) comparePackageBodies(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed package bodies
	for packageBodyName := range prevModel.PackageBodies {
		if _, exists := currModel.PackageBodies[packageBodyName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed package body: %s", packageBodyName))
		}
	}

	// Check for added and modified package bodies
	for packageBodyName, currPackageBody := range currModel.PackageBodies {
		if prevPackageBody, exists := prevModel.PackageBodies[packageBodyName]; exists {
			// Compare existing package body details
			if prevPackageBody.Body != currPackageBody.Body {
				result.Changes = append(result.Changes, fmt.Sprintf("Package body %s implementation changed", packageBodyName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added package body: %s", packageBodyName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareMacros(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed macros
	for macroName := range prevModel.Macros {
		if _, exists := currModel.Macros[macroName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed macro: %s", macroName))
		}
	}

	// Check for added and modified macros
	for macroName, currMacro := range currModel.Macros {
		if prevMacro, exists := prevModel.Macros[macroName]; exists {
			// Compare existing macro details
			if prevMacro.Definition != currMacro.Definition {
				result.Changes = append(result.Changes, fmt.Sprintf("Macro %s definition changed", macroName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added macro: %s", macroName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareRules(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed rules
	for ruleName := range prevModel.Rules {
		if _, exists := currModel.Rules[ruleName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed rule: %s", ruleName))
		}
	}

	// Check for added and modified rules
	for ruleName, currRule := range currModel.Rules {
		if prevRule, exists := prevModel.Rules[ruleName]; exists {
			// Compare existing rule details
			if prevRule.Target != currRule.Target {
				result.Changes = append(result.Changes, fmt.Sprintf("Rule %s target changed: %s -> %s", ruleName, prevRule.Target, currRule.Target))
			}
			if prevRule.Definition != currRule.Definition {
				result.Changes = append(result.Changes, fmt.Sprintf("Rule %s definition changed", ruleName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added rule: %s", ruleName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareWindowFuncs(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed window functions
	for windowFuncName := range prevModel.WindowFuncs {
		if _, exists := currModel.WindowFuncs[windowFuncName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed window function: %s", windowFuncName))
		}
	}

	// Check for added and modified window functions
	for windowFuncName, currWindowFunc := range currModel.WindowFuncs {
		if prevWindowFunc, exists := prevModel.WindowFuncs[windowFuncName]; exists {
			// Compare existing window function details
			if prevWindowFunc.Definition != currWindowFunc.Definition {
				result.Changes = append(result.Changes, fmt.Sprintf("Window function %s definition changed", windowFuncName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added window function: %s", windowFuncName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareUsers(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed users
	for userName := range prevModel.Users {
		if _, exists := currModel.Users[userName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed user: %s", userName))
		}
	}

	// Check for added and modified users
	for userName, currUser := range currModel.Users {
		if prevUser, exists := prevModel.Users[userName]; exists {
			// Compare existing user details
			// Compare roles (order-sensitive)
			if !c.compareStringSlices(prevUser.Roles, currUser.Roles) {
				result.Changes = append(result.Changes, fmt.Sprintf("User %s roles changed", userName))
			}
			// Compare labels (simplified - could be enhanced for detailed comparison)
			if len(prevUser.Labels) != len(currUser.Labels) {
				result.Changes = append(result.Changes, fmt.Sprintf("User %s labels changed", userName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added user: %s", userName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareRoles(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed roles
	for roleName := range prevModel.Roles {
		if _, exists := currModel.Roles[roleName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed role: %s", roleName))
		}
	}

	// Check for added and modified roles
	for roleName, currRole := range currModel.Roles {
		if prevRole, exists := prevModel.Roles[roleName]; exists {
			// Compare existing role details
			// Compare members (order-sensitive)
			if !c.compareStringSlices(prevRole.Members, currRole.Members) {
				result.Changes = append(result.Changes, fmt.Sprintf("Role %s members changed", roleName))
			}
			// Compare parent roles (order-sensitive)
			if !c.compareStringSlices(prevRole.ParentRoles, currRole.ParentRoles) {
				result.Changes = append(result.Changes, fmt.Sprintf("Role %s parent roles changed", roleName))
			}
			// Compare labels (simplified - could be enhanced for detailed comparison)
			if len(prevRole.Labels) != len(currRole.Labels) {
				result.Changes = append(result.Changes, fmt.Sprintf("Role %s labels changed", roleName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added role: %s", roleName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareGrants(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed grants
	for grantName := range prevModel.Grants {
		if _, exists := currModel.Grants[grantName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed grant: %s", grantName))
		}
	}

	// Check for added and modified grants
	for grantName, currGrant := range currModel.Grants {
		if prevGrant, exists := prevModel.Grants[grantName]; exists {
			// Compare existing grant details
			if prevGrant.Principal != currGrant.Principal {
				result.Changes = append(result.Changes, fmt.Sprintf("Grant %s principal changed: %s -> %s", grantName, prevGrant.Principal, currGrant.Principal))
			}
			if prevGrant.Privilege != currGrant.Privilege {
				result.Changes = append(result.Changes, fmt.Sprintf("Grant %s privilege changed: %s -> %s", grantName, prevGrant.Privilege, currGrant.Privilege))
			}
			if prevGrant.Scope != currGrant.Scope {
				result.Changes = append(result.Changes, fmt.Sprintf("Grant %s scope changed: %s -> %s", grantName, prevGrant.Scope, currGrant.Scope))
			}
			if prevGrant.Object != currGrant.Object {
				result.Changes = append(result.Changes, fmt.Sprintf("Grant %s object changed: %s -> %s", grantName, prevGrant.Object, currGrant.Object))
			}
			// Compare columns (order-sensitive)
			if !c.compareStringSlices(prevGrant.Columns, currGrant.Columns) {
				result.Changes = append(result.Changes, fmt.Sprintf("Grant %s columns changed", grantName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added grant: %s", grantName))
		}
	}
}
func (c *UnifiedSchemaComparator) comparePolicies(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed policies
	for policyName := range prevModel.Policies {
		if _, exists := currModel.Policies[policyName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed policy: %s", policyName))
		}
	}

	// Check for added and modified policies
	for policyName, currPolicy := range currModel.Policies {
		if prevPolicy, exists := prevModel.Policies[policyName]; exists {
			// Compare existing policy details
			if prevPolicy.Type != currPolicy.Type {
				result.Changes = append(result.Changes, fmt.Sprintf("Policy %s type changed: %s -> %s", policyName, prevPolicy.Type, currPolicy.Type))
			}
			if prevPolicy.Scope != currPolicy.Scope {
				result.Changes = append(result.Changes, fmt.Sprintf("Policy %s scope changed: %s -> %s", policyName, prevPolicy.Scope, currPolicy.Scope))
			}
			if prevPolicy.Object != currPolicy.Object {
				result.Changes = append(result.Changes, fmt.Sprintf("Policy %s object changed: %s -> %s", policyName, prevPolicy.Object, currPolicy.Object))
			}
			if prevPolicy.Definition != currPolicy.Definition {
				result.Changes = append(result.Changes, fmt.Sprintf("Policy %s definition changed", policyName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added policy: %s", policyName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareTablespaces(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed tablespaces
	for tablespaceName := range prevModel.Tablespaces {
		if _, exists := currModel.Tablespaces[tablespaceName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed tablespace: %s", tablespaceName))
		}
	}

	// Check for added tablespaces
	for tablespaceName := range currModel.Tablespaces {
		if _, exists := prevModel.Tablespaces[tablespaceName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Added tablespace: %s", tablespaceName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareSegments(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed segments
	for segmentName := range prevModel.Segments {
		if _, exists := currModel.Segments[segmentName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed segment: %s", segmentName))
		}
	}

	// Check for added segments
	for segmentName := range currModel.Segments {
		if _, exists := prevModel.Segments[segmentName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Added segment: %s", segmentName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareExtents(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed extents
	for extentName := range prevModel.Extents {
		if _, exists := currModel.Extents[extentName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed extent: %s", extentName))
		}
	}

	// Check for added and modified extents
	for extentName, currExtent := range currModel.Extents {
		if prevExtent, exists := prevModel.Extents[extentName]; exists {
			// Compare existing extent details
			if prevExtent.Size != currExtent.Size {
				result.Changes = append(result.Changes, fmt.Sprintf("Extent %s size changed: %d -> %d bytes", extentName, prevExtent.Size, currExtent.Size))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added extent: %s", extentName))
		}
	}
}
func (c *UnifiedSchemaComparator) comparePages(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed pages
	for pageName := range prevModel.Pages {
		if _, exists := currModel.Pages[pageName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed page: %s", pageName))
		}
	}

	// Check for added and modified pages
	for pageName, currPage := range currModel.Pages {
		if prevPage, exists := prevModel.Pages[pageName]; exists {
			// Compare existing page details
			if prevPage.Number != currPage.Number {
				result.Changes = append(result.Changes, fmt.Sprintf("Page %s number changed: %d -> %d", pageName, prevPage.Number, currPage.Number))
			}
			if prevPage.Size != currPage.Size {
				result.Changes = append(result.Changes, fmt.Sprintf("Page %s size changed: %d -> %d bytes", pageName, prevPage.Size, currPage.Size))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added page: %s", pageName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareFilegroups(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed filegroups
	for filegroupName := range prevModel.Filegroups {
		if _, exists := currModel.Filegroups[filegroupName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed filegroup: %s", filegroupName))
		}
	}

	// Check for added filegroups
	for filegroupName := range currModel.Filegroups {
		if _, exists := prevModel.Filegroups[filegroupName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Added filegroup: %s", filegroupName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareDatafiles(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed datafiles
	for datafileName := range prevModel.Datafiles {
		if _, exists := currModel.Datafiles[datafileName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed datafile: %s", datafileName))
		}
	}

	// Check for added and modified datafiles
	for datafileName, currDatafile := range currModel.Datafiles {
		if prevDatafile, exists := prevModel.Datafiles[datafileName]; exists {
			// Compare existing datafile details
			if prevDatafile.Path != currDatafile.Path {
				result.Changes = append(result.Changes, fmt.Sprintf("Datafile %s path changed: %s -> %s", datafileName, prevDatafile.Path, currDatafile.Path))
			}
			if prevDatafile.Size != currDatafile.Size {
				result.Changes = append(result.Changes, fmt.Sprintf("Datafile %s size changed: %d -> %d bytes", datafileName, prevDatafile.Size, currDatafile.Size))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added datafile: %s", datafileName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareServers(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed servers
	for serverName := range prevModel.Servers {
		if _, exists := currModel.Servers[serverName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed server: %s", serverName))
		}
	}

	// Check for added and modified servers
	for serverName, currServer := range currModel.Servers {
		if prevServer, exists := prevModel.Servers[serverName]; exists {
			// Compare existing server details
			if prevServer.Type != currServer.Type {
				result.Changes = append(result.Changes, fmt.Sprintf("Server %s type changed: %s -> %s", serverName, prevServer.Type, currServer.Type))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added server: %s", serverName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareConnections(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed connections
	for connectionName := range prevModel.Connections {
		if _, exists := currModel.Connections[connectionName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed connection: %s", connectionName))
		}
	}

	// Check for added and modified connections
	for connectionName, currConnection := range currModel.Connections {
		if prevConnection, exists := prevModel.Connections[connectionName]; exists {
			// Compare existing connection details
			if prevConnection.Driver != currConnection.Driver {
				result.Changes = append(result.Changes, fmt.Sprintf("Connection %s driver changed: %s -> %s", connectionName, prevConnection.Driver, currConnection.Driver))
			}
			if prevConnection.DSN != currConnection.DSN {
				result.Changes = append(result.Changes, fmt.Sprintf("Connection %s DSN changed", connectionName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added connection: %s", connectionName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareEndpoints(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed endpoints
	for endpointName := range prevModel.Endpoints {
		if _, exists := currModel.Endpoints[endpointName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed endpoint: %s", endpointName))
		}
	}

	// Check for added and modified endpoints
	for endpointName, currEndpoint := range currModel.Endpoints {
		if prevEndpoint, exists := prevModel.Endpoints[endpointName]; exists {
			// Compare existing endpoint details
			if prevEndpoint.Scheme != currEndpoint.Scheme {
				result.Changes = append(result.Changes, fmt.Sprintf("Endpoint %s scheme changed: %s -> %s", endpointName, prevEndpoint.Scheme, currEndpoint.Scheme))
			}
			if prevEndpoint.Host != currEndpoint.Host {
				result.Changes = append(result.Changes, fmt.Sprintf("Endpoint %s host changed: %s -> %s", endpointName, prevEndpoint.Host, currEndpoint.Host))
			}
			if prevEndpoint.Port != currEndpoint.Port {
				result.Changes = append(result.Changes, fmt.Sprintf("Endpoint %s port changed: %d -> %d", endpointName, prevEndpoint.Port, currEndpoint.Port))
			}
			if prevEndpoint.Path != currEndpoint.Path {
				result.Changes = append(result.Changes, fmt.Sprintf("Endpoint %s path changed: %s -> %s", endpointName, prevEndpoint.Path, currEndpoint.Path))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added endpoint: %s", endpointName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareForeignDataWrappers(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed foreign data wrappers
	for fdwName := range prevModel.ForeignDataWrappers {
		if _, exists := currModel.ForeignDataWrappers[fdwName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed foreign data wrapper: %s", fdwName))
		}
	}

	// Check for added and modified foreign data wrappers
	for fdwName, currFDW := range currModel.ForeignDataWrappers {
		if prevFDW, exists := prevModel.ForeignDataWrappers[fdwName]; exists {
			// Compare existing foreign data wrapper details
			if prevFDW.Handler != currFDW.Handler {
				result.Changes = append(result.Changes, fmt.Sprintf("Foreign data wrapper %s handler changed: %s -> %s", fdwName, prevFDW.Handler, currFDW.Handler))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added foreign data wrapper: %s", fdwName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareUserMappings(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed user mappings
	for mappingName := range prevModel.UserMappings {
		if _, exists := currModel.UserMappings[mappingName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed user mapping: %s", mappingName))
		}
	}

	// Check for added and modified user mappings
	for mappingName, currMapping := range currModel.UserMappings {
		if prevMapping, exists := prevModel.UserMappings[mappingName]; exists {
			// Compare existing user mapping details
			if prevMapping.User != currMapping.User {
				result.Changes = append(result.Changes, fmt.Sprintf("User mapping %s user changed: %s -> %s", mappingName, prevMapping.User, currMapping.User))
			}
			if prevMapping.Server != currMapping.Server {
				result.Changes = append(result.Changes, fmt.Sprintf("User mapping %s server changed: %s -> %s", mappingName, prevMapping.Server, currMapping.Server))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added user mapping: %s", mappingName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareFederations(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed federations
	for federationName := range prevModel.Federations {
		if _, exists := currModel.Federations[federationName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed federation: %s", federationName))
		}
	}

	// Check for added and modified federations
	for federationName, currFederation := range currModel.Federations {
		if prevFederation, exists := prevModel.Federations[federationName]; exists {
			// Compare existing federation details
			// Compare members (order-sensitive)
			if !c.compareStringSlices(prevFederation.Members, currFederation.Members) {
				result.Changes = append(result.Changes, fmt.Sprintf("Federation %s members changed", federationName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added federation: %s", federationName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareReplicas(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed replicas
	for replicaName := range prevModel.Replicas {
		if _, exists := currModel.Replicas[replicaName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed replica: %s", replicaName))
		}
	}

	// Check for added and modified replicas
	for replicaName, currReplica := range currModel.Replicas {
		if prevReplica, exists := prevModel.Replicas[replicaName]; exists {
			// Compare existing replica details
			if prevReplica.Mode != currReplica.Mode {
				result.Changes = append(result.Changes, fmt.Sprintf("Replica %s mode changed: %s -> %s", replicaName, prevReplica.Mode, currReplica.Mode))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added replica: %s", replicaName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareClusters(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed clusters
	for clusterName := range prevModel.Clusters {
		if _, exists := currModel.Clusters[clusterName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed cluster: %s", clusterName))
		}
	}

	// Check for added and modified clusters
	for clusterName, currCluster := range currModel.Clusters {
		if prevCluster, exists := prevModel.Clusters[clusterName]; exists {
			// Compare existing cluster details
			// Compare nodes (order-sensitive)
			if !c.compareStringSlices(prevCluster.Nodes, currCluster.Nodes) {
				result.Changes = append(result.Changes, fmt.Sprintf("Cluster %s nodes changed", clusterName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added cluster: %s", clusterName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareTasks(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed tasks
	for taskName := range prevModel.Tasks {
		if _, exists := currModel.Tasks[taskName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed task: %s", taskName))
		}
	}

	// Check for added and modified tasks
	for taskName, currTask := range currModel.Tasks {
		if prevTask, exists := prevModel.Tasks[taskName]; exists {
			// Compare existing task details
			if prevTask.Definition != currTask.Definition {
				result.Changes = append(result.Changes, fmt.Sprintf("Task %s definition changed", taskName))
			}
			if prevTask.Schedule != currTask.Schedule {
				result.Changes = append(result.Changes, fmt.Sprintf("Task %s schedule changed: %s -> %s", taskName, prevTask.Schedule, currTask.Schedule))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added task: %s", taskName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareJobs(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed jobs
	for jobName := range prevModel.Jobs {
		if _, exists := currModel.Jobs[jobName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed job: %s", jobName))
		}
	}

	// Check for added and modified jobs
	for jobName, currJob := range currModel.Jobs {
		if prevJob, exists := prevModel.Jobs[jobName]; exists {
			// Compare existing job details
			if prevJob.Type != currJob.Type {
				result.Changes = append(result.Changes, fmt.Sprintf("Job %s type changed: %s -> %s", jobName, prevJob.Type, currJob.Type))
			}
			if prevJob.Schedule != currJob.Schedule {
				result.Changes = append(result.Changes, fmt.Sprintf("Job %s schedule changed: %s -> %s", jobName, prevJob.Schedule, currJob.Schedule))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added job: %s", jobName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareSchedules(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed schedules
	for scheduleName := range prevModel.Schedules {
		if _, exists := currModel.Schedules[scheduleName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed schedule: %s", scheduleName))
		}
	}

	// Check for added and modified schedules
	for scheduleName, currSchedule := range currModel.Schedules {
		if prevSchedule, exists := prevModel.Schedules[scheduleName]; exists {
			// Compare existing schedule details
			if prevSchedule.Cron != currSchedule.Cron {
				result.Changes = append(result.Changes, fmt.Sprintf("Schedule %s cron changed: %s -> %s", scheduleName, prevSchedule.Cron, currSchedule.Cron))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added schedule: %s", scheduleName))
		}
	}
}
func (c *UnifiedSchemaComparator) comparePipelines(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed pipelines
	for pipelineName := range prevModel.Pipelines {
		if _, exists := currModel.Pipelines[pipelineName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed pipeline: %s", pipelineName))
		}
	}

	// Check for added and modified pipelines
	for pipelineName, currPipeline := range currModel.Pipelines {
		if prevPipeline, exists := prevModel.Pipelines[pipelineName]; exists {
			// Compare existing pipeline details
			// Compare steps (order-sensitive)
			if !c.compareStringSlices(prevPipeline.Steps, currPipeline.Steps) {
				result.Changes = append(result.Changes, fmt.Sprintf("Pipeline %s steps changed", pipelineName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added pipeline: %s", pipelineName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareStreams(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed streams
	for streamName := range prevModel.Streams {
		if _, exists := currModel.Streams[streamName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed stream: %s", streamName))
		}
	}

	// Check for added and modified streams
	for streamName, currStream := range currModel.Streams {
		if prevStream, exists := prevModel.Streams[streamName]; exists {
			// Compare existing stream details
			if prevStream.On != currStream.On {
				result.Changes = append(result.Changes, fmt.Sprintf("Stream %s source changed: %s -> %s", streamName, prevStream.On, currStream.On))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added stream: %s", streamName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareEvents(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed events
	for eventName := range prevModel.Events {
		if _, exists := currModel.Events[eventName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed event: %s", eventName))
		}
	}

	// Check for added and modified events
	for eventName, currEvent := range currModel.Events {
		if prevEvent, exists := prevModel.Events[eventName]; exists {
			// Compare existing event details
			if prevEvent.Source != currEvent.Source {
				result.Changes = append(result.Changes, fmt.Sprintf("Event %s source changed: %s -> %s", eventName, prevEvent.Source, currEvent.Source))
			}
			// Compare payload (simplified - could be enhanced for detailed comparison)
			if len(prevEvent.Payload) != len(currEvent.Payload) {
				result.Changes = append(result.Changes, fmt.Sprintf("Event %s payload changed", eventName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added event: %s", eventName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareNotifications(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed notifications
	for notificationName := range prevModel.Notifications {
		if _, exists := currModel.Notifications[notificationName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed notification: %s", notificationName))
		}
	}

	// Check for added and modified notifications
	for notificationName, currNotification := range currModel.Notifications {
		if prevNotification, exists := prevModel.Notifications[notificationName]; exists {
			// Compare existing notification details
			if prevNotification.Channel != currNotification.Channel {
				result.Changes = append(result.Changes, fmt.Sprintf("Notification %s channel changed: %s -> %s", notificationName, prevNotification.Channel, currNotification.Channel))
			}
			if prevNotification.Message != currNotification.Message {
				result.Changes = append(result.Changes, fmt.Sprintf("Notification %s message changed", notificationName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added notification: %s", notificationName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareAlerts(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed alerts
	for alertName := range prevModel.Alerts {
		if _, exists := currModel.Alerts[alertName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed alert: %s", alertName))
		}
	}

	// Check for added and modified alerts
	for alertName, currAlert := range currModel.Alerts {
		if prevAlert, exists := prevModel.Alerts[alertName]; exists {
			// Compare existing alert details
			if prevAlert.Condition != currAlert.Condition {
				result.Changes = append(result.Changes, fmt.Sprintf("Alert %s condition changed", alertName))
			}
			if prevAlert.Severity != currAlert.Severity {
				result.Changes = append(result.Changes, fmt.Sprintf("Alert %s severity changed: %s -> %s", alertName, prevAlert.Severity, currAlert.Severity))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added alert: %s", alertName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareStatistics(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed statistics
	for statisticName := range prevModel.Statistics {
		if _, exists := currModel.Statistics[statisticName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed statistic: %s", statisticName))
		}
	}

	// Check for added and modified statistics
	for statisticName, currStatistic := range currModel.Statistics {
		if prevStatistic, exists := prevModel.Statistics[statisticName]; exists {
			// Compare existing statistic details
			if prevStatistic.Value != currStatistic.Value {
				result.Changes = append(result.Changes, fmt.Sprintf("Statistic %s value changed", statisticName))
			}
			// Compare labels (simplified - could be enhanced for detailed comparison)
			if len(prevStatistic.Labels) != len(currStatistic.Labels) {
				result.Changes = append(result.Changes, fmt.Sprintf("Statistic %s labels changed", statisticName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added statistic: %s", statisticName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareHistograms(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed histograms
	for histogramName := range prevModel.Histograms {
		if _, exists := currModel.Histograms[histogramName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed histogram: %s", histogramName))
		}
	}

	// Check for added and modified histograms
	for histogramName, currHistogram := range currModel.Histograms {
		if prevHistogram, exists := prevModel.Histograms[histogramName]; exists {
			// Compare existing histogram details
			// Compare buckets (simplified - could be enhanced for detailed comparison)
			if len(prevHistogram.Buckets) != len(currHistogram.Buckets) {
				result.Changes = append(result.Changes, fmt.Sprintf("Histogram %s buckets changed", histogramName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added histogram: %s", histogramName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareMonitors(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed monitors
	for monitorName := range prevModel.Monitors {
		if _, exists := currModel.Monitors[monitorName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed monitor: %s", monitorName))
		}
	}

	// Check for added and modified monitors
	for monitorName, currMonitor := range currModel.Monitors {
		if prevMonitor, exists := prevModel.Monitors[monitorName]; exists {
			// Compare existing monitor details
			if prevMonitor.Scope != currMonitor.Scope {
				result.Changes = append(result.Changes, fmt.Sprintf("Monitor %s scope changed: %s -> %s", monitorName, prevMonitor.Scope, currMonitor.Scope))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added monitor: %s", monitorName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareMonitorMetrics(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed monitor metrics
	for metricName := range prevModel.MonitorMetrics {
		if _, exists := currModel.MonitorMetrics[metricName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed monitor metric: %s", metricName))
		}
	}

	// Check for added and modified monitor metrics
	for metricName, currMetric := range currModel.MonitorMetrics {
		if prevMetric, exists := prevModel.MonitorMetrics[metricName]; exists {
			// Compare existing monitor metric details
			if prevMetric.Unit != currMetric.Unit {
				result.Changes = append(result.Changes, fmt.Sprintf("Monitor metric %s unit changed: %s -> %s", metricName, prevMetric.Unit, currMetric.Unit))
			}
			// Compare labels (simplified - could be enhanced for detailed comparison)
			if len(prevMetric.Labels) != len(currMetric.Labels) {
				result.Changes = append(result.Changes, fmt.Sprintf("Monitor metric %s labels changed", metricName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added monitor metric: %s", metricName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareThresholds(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed thresholds
	for thresholdName := range prevModel.Thresholds {
		if _, exists := currModel.Thresholds[thresholdName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed threshold: %s", thresholdName))
		}
	}

	// Check for added and modified thresholds
	for thresholdName, currThreshold := range currModel.Thresholds {
		if prevThreshold, exists := prevModel.Thresholds[thresholdName]; exists {
			// Compare existing threshold details
			if prevThreshold.Metric != currThreshold.Metric {
				result.Changes = append(result.Changes, fmt.Sprintf("Threshold %s metric changed: %s -> %s", thresholdName, prevThreshold.Metric, currThreshold.Metric))
			}
			if prevThreshold.Operator != currThreshold.Operator {
				result.Changes = append(result.Changes, fmt.Sprintf("Threshold %s operator changed: %s -> %s", thresholdName, prevThreshold.Operator, currThreshold.Operator))
			}
			if prevThreshold.Value != currThreshold.Value {
				result.Changes = append(result.Changes, fmt.Sprintf("Threshold %s value changed", thresholdName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added threshold: %s", thresholdName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareTextSearchComponents(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed text search components
	for componentName := range prevModel.TextSearchComponents {
		if _, exists := currModel.TextSearchComponents[componentName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed text search component: %s", componentName))
		}
	}

	// Check for added and modified text search components
	for componentName, currComponent := range currModel.TextSearchComponents {
		if prevComponent, exists := prevModel.TextSearchComponents[componentName]; exists {
			// Compare existing component details
			if prevComponent.Type != currComponent.Type {
				result.Changes = append(result.Changes, fmt.Sprintf("Text search component %s type changed: %s -> %s", componentName, prevComponent.Type, currComponent.Type))
			}
			if prevComponent.Parser != currComponent.Parser {
				result.Changes = append(result.Changes, fmt.Sprintf("Text search component %s parser changed: %s -> %s", componentName, prevComponent.Parser, currComponent.Parser))
			}
			// Compare dictionaries (order-sensitive)
			if !c.compareStringSlices(prevComponent.Dictionaries, currComponent.Dictionaries) {
				result.Changes = append(result.Changes, fmt.Sprintf("Text search component %s dictionaries changed", componentName))
			}
			// Compare chain (order-sensitive)
			if !c.compareStringSlices(prevComponent.Chain, currComponent.Chain) {
				result.Changes = append(result.Changes, fmt.Sprintf("Text search component %s chain changed", componentName))
			}
			if prevComponent.Comment != currComponent.Comment {
				result.Changes = append(result.Changes, fmt.Sprintf("Text search component %s comment changed", componentName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added text search component: %s", componentName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareComments(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed comments
	for commentName := range prevModel.Comments {
		if _, exists := currModel.Comments[commentName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed comment: %s", commentName))
		}
	}

	// Check for added and modified comments
	for commentName, currComment := range currModel.Comments {
		if prevComment, exists := prevModel.Comments[commentName]; exists {
			// Compare existing comment details
			if prevComment.On != currComment.On {
				result.Changes = append(result.Changes, fmt.Sprintf("Comment %s target changed: %s -> %s", commentName, prevComment.On, currComment.On))
			}
			if prevComment.Comment != currComment.Comment {
				result.Changes = append(result.Changes, fmt.Sprintf("Comment %s text changed", commentName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added comment: %s", commentName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareAnnotations(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed annotations
	for annotationName := range prevModel.Annotations {
		if _, exists := currModel.Annotations[annotationName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed annotation: %s", annotationName))
		}
	}

	// Check for added and modified annotations
	for annotationName, currAnnotation := range currModel.Annotations {
		if prevAnnotation, exists := prevModel.Annotations[annotationName]; exists {
			// Compare existing annotation details
			if prevAnnotation.On != currAnnotation.On {
				result.Changes = append(result.Changes, fmt.Sprintf("Annotation %s target changed: %s -> %s", annotationName, prevAnnotation.On, currAnnotation.On))
			}
			if prevAnnotation.Key != currAnnotation.Key {
				result.Changes = append(result.Changes, fmt.Sprintf("Annotation %s key changed: %s -> %s", annotationName, prevAnnotation.Key, currAnnotation.Key))
			}
			if prevAnnotation.Value != currAnnotation.Value {
				result.Changes = append(result.Changes, fmt.Sprintf("Annotation %s value changed", annotationName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added annotation: %s", annotationName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareTags(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed tags
	for tagName := range prevModel.Tags {
		if _, exists := currModel.Tags[tagName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed tag: %s", tagName))
		}
	}

	// Check for added and modified tags
	for tagName, currTag := range currModel.Tags {
		if prevTag, exists := prevModel.Tags[tagName]; exists {
			// Compare existing tag details
			if prevTag.On != currTag.On {
				result.Changes = append(result.Changes, fmt.Sprintf("Tag %s target changed: %s -> %s", tagName, prevTag.On, currTag.On))
			}
			if prevTag.Name != currTag.Name {
				result.Changes = append(result.Changes, fmt.Sprintf("Tag %s name changed: %s -> %s", tagName, prevTag.Name, currTag.Name))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added tag: %s", tagName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareAliases(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed aliases
	for aliasName := range prevModel.Aliases {
		if _, exists := currModel.Aliases[aliasName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed alias: %s", aliasName))
		}
	}

	// Check for added and modified aliases
	for aliasName, currAlias := range currModel.Aliases {
		if prevAlias, exists := prevModel.Aliases[aliasName]; exists {
			// Compare existing alias details
			if prevAlias.On != currAlias.On {
				result.Changes = append(result.Changes, fmt.Sprintf("Alias %s target changed: %s -> %s", aliasName, prevAlias.On, currAlias.On))
			}
			if prevAlias.Alias != currAlias.Alias {
				result.Changes = append(result.Changes, fmt.Sprintf("Alias %s value changed: %s -> %s", aliasName, prevAlias.Alias, currAlias.Alias))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added alias: %s", aliasName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareSynonyms(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed synonyms
	for synonymName := range prevModel.Synonyms {
		if _, exists := currModel.Synonyms[synonymName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed synonym: %s", synonymName))
		}
	}

	// Check for added and modified synonyms
	for synonymName, currSynonym := range currModel.Synonyms {
		if prevSynonym, exists := prevModel.Synonyms[synonymName]; exists {
			// Compare existing synonym details
			if prevSynonym.On != currSynonym.On {
				result.Changes = append(result.Changes, fmt.Sprintf("Synonym %s target changed: %s -> %s", synonymName, prevSynonym.On, currSynonym.On))
			}
			if prevSynonym.Name != currSynonym.Name {
				result.Changes = append(result.Changes, fmt.Sprintf("Synonym %s name changed: %s -> %s", synonymName, prevSynonym.Name, currSynonym.Name))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added synonym: %s", synonymName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareLabels(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed labels
	for labelName := range prevModel.Labels {
		if _, exists := currModel.Labels[labelName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed label: %s", labelName))
		}
	}

	// Check for added and modified labels
	for labelName, currLabel := range currModel.Labels {
		if prevLabel, exists := prevModel.Labels[labelName]; exists {
			// Compare existing label details
			if prevLabel.On != currLabel.On {
				result.Changes = append(result.Changes, fmt.Sprintf("Label %s target changed: %s -> %s", labelName, prevLabel.On, currLabel.On))
			}
			if prevLabel.Name != currLabel.Name {
				result.Changes = append(result.Changes, fmt.Sprintf("Label %s name changed: %s -> %s", labelName, prevLabel.Name, currLabel.Name))
			}
			// Compare props (simplified - could be enhanced for detailed comparison)
			if len(prevLabel.Props) != len(currLabel.Props) {
				result.Changes = append(result.Changes, fmt.Sprintf("Label %s properties changed", labelName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added label: %s", labelName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareSnapshots(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed snapshots
	for snapshotName := range prevModel.Snapshots {
		if _, exists := currModel.Snapshots[snapshotName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed snapshot: %s", snapshotName))
		}
	}

	// Check for added and modified snapshots
	for snapshotName, currSnapshot := range currModel.Snapshots {
		if prevSnapshot, exists := prevModel.Snapshots[snapshotName]; exists {
			// Compare existing snapshot details
			if prevSnapshot.Scope != currSnapshot.Scope {
				result.Changes = append(result.Changes, fmt.Sprintf("Snapshot %s scope changed: %s -> %s", snapshotName, prevSnapshot.Scope, currSnapshot.Scope))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added snapshot: %s", snapshotName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareBackups(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed backups
	for backupName := range prevModel.Backups {
		if _, exists := currModel.Backups[backupName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed backup: %s", backupName))
		}
	}

	// Check for added and modified backups
	for backupName, currBackup := range currModel.Backups {
		if prevBackup, exists := prevModel.Backups[backupName]; exists {
			// Compare existing backup details
			if prevBackup.Method != currBackup.Method {
				result.Changes = append(result.Changes, fmt.Sprintf("Backup %s method changed: %s -> %s", backupName, prevBackup.Method, currBackup.Method))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added backup: %s", backupName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareArchives(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed archives
	for archiveName := range prevModel.Archives {
		if _, exists := currModel.Archives[archiveName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed archive: %s", archiveName))
		}
	}

	// Check for added and modified archives
	for archiveName, currArchive := range currModel.Archives {
		if prevArchive, exists := prevModel.Archives[archiveName]; exists {
			// Compare existing archive details
			if prevArchive.Format != currArchive.Format {
				result.Changes = append(result.Changes, fmt.Sprintf("Archive %s format changed: %s -> %s", archiveName, prevArchive.Format, currArchive.Format))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added archive: %s", archiveName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareRecoveryPoints(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed recovery points
	for recoveryPointName := range prevModel.RecoveryPoints {
		if _, exists := currModel.RecoveryPoints[recoveryPointName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed recovery point: %s", recoveryPointName))
		}
	}

	// Check for added and modified recovery points
	for recoveryPointName, currRecoveryPoint := range currModel.RecoveryPoints {
		if prevRecoveryPoint, exists := prevModel.RecoveryPoints[recoveryPointName]; exists {
			// Compare existing recovery point details
			if prevRecoveryPoint.Point != currRecoveryPoint.Point {
				result.Changes = append(result.Changes, fmt.Sprintf("Recovery point %s location changed: %s -> %s", recoveryPointName, prevRecoveryPoint.Point, currRecoveryPoint.Point))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added recovery point: %s", recoveryPointName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareVersions(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed versions
	for versionName := range prevModel.Versions {
		if _, exists := currModel.Versions[versionName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed version: %s", versionName))
		}
	}

	// Check for added and modified versions
	for versionName, currVersion := range currModel.Versions {
		if prevVersion, exists := prevModel.Versions[versionName]; exists {
			// Compare existing version details
			if prevVersion.ID != currVersion.ID {
				result.Changes = append(result.Changes, fmt.Sprintf("Version %s ID changed: %s -> %s", versionName, prevVersion.ID, currVersion.ID))
			}
			// Compare parents (order-sensitive)
			if !c.compareStringSlices(prevVersion.Parents, currVersion.Parents) {
				result.Changes = append(result.Changes, fmt.Sprintf("Version %s parents changed", versionName))
			}
			if prevVersion.Message != currVersion.Message {
				result.Changes = append(result.Changes, fmt.Sprintf("Version %s message changed", versionName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added version: %s", versionName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareMigrations(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed migrations
	for migrationName := range prevModel.Migrations {
		if _, exists := currModel.Migrations[migrationName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed migration: %s", migrationName))
		}
	}

	// Check for added and modified migrations
	for migrationName, currMigration := range currModel.Migrations {
		if prevMigration, exists := prevModel.Migrations[migrationName]; exists {
			// Compare existing migration details
			if prevMigration.ID != currMigration.ID {
				result.Changes = append(result.Changes, fmt.Sprintf("Migration %s ID changed: %s -> %s", migrationName, prevMigration.ID, currMigration.ID))
			}
			if prevMigration.Description != currMigration.Description {
				result.Changes = append(result.Changes, fmt.Sprintf("Migration %s description changed", migrationName))
			}
			if prevMigration.Script != currMigration.Script {
				result.Changes = append(result.Changes, fmt.Sprintf("Migration %s script changed", migrationName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added migration: %s", migrationName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareBranches(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed branches
	for branchName := range prevModel.Branches {
		if _, exists := currModel.Branches[branchName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed branch: %s", branchName))
		}
	}

	// Check for added and modified branches
	for branchName, currBranch := range currModel.Branches {
		if prevBranch, exists := prevModel.Branches[branchName]; exists {
			// Compare existing branch details
			if prevBranch.Name != currBranch.Name {
				result.Changes = append(result.Changes, fmt.Sprintf("Branch %s name changed: %s -> %s", branchName, prevBranch.Name, currBranch.Name))
			}
			if prevBranch.From != currBranch.From {
				result.Changes = append(result.Changes, fmt.Sprintf("Branch %s source changed: %s -> %s", branchName, prevBranch.From, currBranch.From))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added branch: %s", branchName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareTimeTravel(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed time travel configurations
	for timeTravelName := range prevModel.TimeTravel {
		if _, exists := currModel.TimeTravel[timeTravelName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed time travel configuration: %s", timeTravelName))
		}
	}

	// Check for added and modified time travel configurations
	for timeTravelName, currTimeTravel := range currModel.TimeTravel {
		if prevTimeTravel, exists := prevModel.TimeTravel[timeTravelName]; exists {
			// Compare existing time travel details
			if prevTimeTravel.Object != currTimeTravel.Object {
				result.Changes = append(result.Changes, fmt.Sprintf("Time travel %s object changed: %s -> %s", timeTravelName, prevTimeTravel.Object, currTimeTravel.Object))
			}
			if prevTimeTravel.AsOf != currTimeTravel.AsOf {
				result.Changes = append(result.Changes, fmt.Sprintf("Time travel %s timestamp changed: %s -> %s", timeTravelName, prevTimeTravel.AsOf, currTimeTravel.AsOf))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added time travel configuration: %s", timeTravelName))
		}
	}
}
func (c *UnifiedSchemaComparator) comparePlugins(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed plugins
	for pluginName := range prevModel.Plugins {
		if _, exists := currModel.Plugins[pluginName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed plugin: %s", pluginName))
		}
	}

	// Check for added and modified plugins
	for pluginName, currPlugin := range currModel.Plugins {
		if prevPlugin, exists := prevModel.Plugins[pluginName]; exists {
			// Compare existing plugin details
			if prevPlugin.Name != currPlugin.Name {
				result.Changes = append(result.Changes, fmt.Sprintf("Plugin %s name changed: %s -> %s", pluginName, prevPlugin.Name, currPlugin.Name))
			}
			if prevPlugin.Version != currPlugin.Version {
				result.Changes = append(result.Changes, fmt.Sprintf("Plugin %s version changed: %s -> %s", pluginName, prevPlugin.Version, currPlugin.Version))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added plugin: %s", pluginName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareModuleExtensions(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed module extensions
	for moduleExtensionName := range prevModel.ModuleExtensions {
		if _, exists := currModel.ModuleExtensions[moduleExtensionName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed module extension: %s", moduleExtensionName))
		}
	}

	// Check for added and modified module extensions
	for moduleExtensionName, currModuleExtension := range currModel.ModuleExtensions {
		if prevModuleExtension, exists := prevModel.ModuleExtensions[moduleExtensionName]; exists {
			// Compare existing module extension details
			if prevModuleExtension.Name != currModuleExtension.Name {
				result.Changes = append(result.Changes, fmt.Sprintf("Module extension %s name changed: %s -> %s", moduleExtensionName, prevModuleExtension.Name, currModuleExtension.Name))
			}
			if prevModuleExtension.Module != currModuleExtension.Module {
				result.Changes = append(result.Changes, fmt.Sprintf("Module extension %s module changed: %s -> %s", moduleExtensionName, prevModuleExtension.Module, currModuleExtension.Module))
			}
			if prevModuleExtension.Version != currModuleExtension.Version {
				result.Changes = append(result.Changes, fmt.Sprintf("Module extension %s version changed: %s -> %s", moduleExtensionName, prevModuleExtension.Version, currModuleExtension.Version))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added module extension: %s", moduleExtensionName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareTTLSettings(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed TTL settings
	for ttlSettingName := range prevModel.TTLSettings {
		if _, exists := currModel.TTLSettings[ttlSettingName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed TTL setting: %s", ttlSettingName))
		}
	}

	// Check for added and modified TTL settings
	for ttlSettingName, currTTLSetting := range currModel.TTLSettings {
		if prevTTLSetting, exists := prevModel.TTLSettings[ttlSettingName]; exists {
			// Compare existing TTL setting details
			if prevTTLSetting.Name != currTTLSetting.Name {
				result.Changes = append(result.Changes, fmt.Sprintf("TTL setting %s name changed: %s -> %s", ttlSettingName, prevTTLSetting.Name, currTTLSetting.Name))
			}
			if prevTTLSetting.Scope != currTTLSetting.Scope {
				result.Changes = append(result.Changes, fmt.Sprintf("TTL setting %s scope changed: %s -> %s", ttlSettingName, prevTTLSetting.Scope, currTTLSetting.Scope))
			}
			if prevTTLSetting.Policy != currTTLSetting.Policy {
				result.Changes = append(result.Changes, fmt.Sprintf("TTL setting %s policy changed: %s -> %s", ttlSettingName, prevTTLSetting.Policy, currTTLSetting.Policy))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added TTL setting: %s", ttlSettingName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareDimensions(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed dimensions
	for dimensionName := range prevModel.Dimensions {
		if _, exists := currModel.Dimensions[dimensionName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed dimension: %s", dimensionName))
		}
	}

	// Check for added and modified dimensions
	for dimensionName, currDimension := range currModel.Dimensions {
		if prevDimension, exists := prevModel.Dimensions[dimensionName]; exists {
			// Compare existing dimension details
			if prevDimension.Name != currDimension.Name {
				result.Changes = append(result.Changes, fmt.Sprintf("Dimension %s name changed: %s -> %s", dimensionName, prevDimension.Name, currDimension.Name))
			}
			if prevDimension.Size != currDimension.Size {
				result.Changes = append(result.Changes, fmt.Sprintf("Dimension %s size changed: %d -> %d", dimensionName, prevDimension.Size, currDimension.Size))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added dimension: %s", dimensionName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareDistanceMetrics(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed distance metrics
	for distanceMetricName := range prevModel.DistanceMetrics {
		if _, exists := currModel.DistanceMetrics[distanceMetricName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed distance metric: %s", distanceMetricName))
		}
	}

	// Check for added and modified distance metrics
	for distanceMetricName, currDistanceMetric := range currModel.DistanceMetrics {
		if prevDistanceMetric, exists := prevModel.DistanceMetrics[distanceMetricName]; exists {
			// Compare existing distance metric details
			if prevDistanceMetric.Name != currDistanceMetric.Name {
				result.Changes = append(result.Changes, fmt.Sprintf("Distance metric %s name changed: %s -> %s", distanceMetricName, prevDistanceMetric.Name, currDistanceMetric.Name))
			}
			if prevDistanceMetric.Method != currDistanceMetric.Method {
				result.Changes = append(result.Changes, fmt.Sprintf("Distance metric %s method changed: %s -> %s", distanceMetricName, prevDistanceMetric.Method, currDistanceMetric.Method))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added distance metric: %s", distanceMetricName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareProjections(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed projections
	for projectionName := range prevModel.Projections {
		if _, exists := currModel.Projections[projectionName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed projection: %s", projectionName))
		}
	}

	// Check for added and modified projections
	for projectionName, currProjection := range currModel.Projections {
		if prevProjection, exists := prevModel.Projections[projectionName]; exists {
			// Compare existing projection details
			if prevProjection.Name != currProjection.Name {
				result.Changes = append(result.Changes, fmt.Sprintf("Projection %s name changed: %s -> %s", projectionName, prevProjection.Name, currProjection.Name))
			}
			if prevProjection.Definition != currProjection.Definition {
				result.Changes = append(result.Changes, fmt.Sprintf("Projection %s definition changed", projectionName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added projection: %s", projectionName))
		}
	}
}
func (c *UnifiedSchemaComparator) compareAnalyticsAggs(prevModel, currModel *unifiedmodel.UnifiedModel, result *UnifiedCompareResult) {
	// Check for removed analytics aggregations
	for analyticsAggName := range prevModel.AnalyticsAggs {
		if _, exists := currModel.AnalyticsAggs[analyticsAggName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed analytics aggregation: %s", analyticsAggName))
		}
	}

	// Check for added and modified analytics aggregations
	for analyticsAggName, currAnalyticsAgg := range currModel.AnalyticsAggs {
		if prevAnalyticsAgg, exists := prevModel.AnalyticsAggs[analyticsAggName]; exists {
			// Compare existing analytics aggregation details
			if prevAnalyticsAgg.Name != currAnalyticsAgg.Name {
				result.Changes = append(result.Changes, fmt.Sprintf("Analytics aggregation %s name changed: %s -> %s", analyticsAggName, prevAnalyticsAgg.Name, currAnalyticsAgg.Name))
			}
			if prevAnalyticsAgg.Definition != currAnalyticsAgg.Definition {
				result.Changes = append(result.Changes, fmt.Sprintf("Analytics aggregation %s definition changed", analyticsAggName))
			}
		} else {
			result.Changes = append(result.Changes, fmt.Sprintf("Added analytics aggregation: %s", analyticsAggName))
		}
	}
}
