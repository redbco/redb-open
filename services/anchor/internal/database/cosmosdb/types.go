package cosmosdb

import (
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// CreateCosmosDBUnifiedModel creates a UnifiedModel for CosmosDB with database details
func CreateCosmosDBUnifiedModel(uniqueIdentifier, version string, databaseSize int64) *unifiedmodel.UnifiedModel {
	um := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.CosmosDB,
		Collections:  make(map[string]unifiedmodel.Collection),
		Databases:    make(map[string]unifiedmodel.Database),
		Functions:    make(map[string]unifiedmodel.Function),
		Triggers:     make(map[string]unifiedmodel.Trigger),
		Procedures:   make(map[string]unifiedmodel.Procedure),
		Indexes:      make(map[string]unifiedmodel.Index),
	}
	return um
}

// ConvertCosmosDBContainer converts common.TableInfo to unifiedmodel.Collection for CosmosDB
func ConvertCosmosDBContainer(containerInfo common.TableInfo) unifiedmodel.Collection {
	collection := unifiedmodel.Collection{
		Name:    containerInfo.Name,
		Owner:   containerInfo.Schema, // Store database name in owner
		Fields:  make(map[string]unifiedmodel.Field),
		Indexes: make(map[string]unifiedmodel.Index),
	}

	// Convert columns to fields (CosmosDB uses flexible schema)
	for _, col := range containerInfo.Columns {
		collection.Fields[col.Name] = unifiedmodel.Field{
			Name: col.Name,
			Type: col.DataType,
		}
	}

	// Convert indexes
	for _, idx := range containerInfo.Indexes {
		collection.Indexes[idx.Name] = unifiedmodel.Index{
			Name:    idx.Name,
			Columns: idx.Columns,
			Unique:  idx.IsUnique,
		}
	}

	return collection
}

// ConvertCosmosDBDatabase converts common.DatabaseSchemaInfo to unifiedmodel.Database
func ConvertCosmosDBDatabase(dbInfo common.DatabaseSchemaInfo) unifiedmodel.Database {
	return unifiedmodel.Database{
		Name:    dbInfo.Name,
		Comment: dbInfo.Description,
	}
}

// CosmosDBReplicationSourceDetails contains information about CosmosDB change feed (limited support)
type CosmosDBReplicationSourceDetails struct {
	ContainerName string `json:"container_name"`
	DatabaseID    string `json:"database_id"`
	DatabaseName  string `json:"database_name"`
}

// CosmosDBReplicationChange represents a change in CosmosDB
type CosmosDBReplicationChange struct {
	Operation string
	Data      map[string]interface{}
	OldData   map[string]interface{}
}

// CosmosDBContainerDetails contains detailed information about a CosmosDB container
type CosmosDBContainerDetails struct {
	ContainerName      string                    `json:"containerName"`
	PartitionKeyPath   string                    `json:"partitionKeyPath"`
	ThroughputSettings CosmosDBThroughputInfo    `json:"throughputSettings"`
	IndexingPolicy     CosmosDBIndexingPolicy    `json:"indexingPolicy"`
	UniqueKeyPolicy    CosmosDBUniqueKeyPolicy   `json:"uniqueKeyPolicy"`
	ConflictResolution CosmosDBConflictPolicy    `json:"conflictResolution"`
	AnalyticalStorage  CosmosDBAnalyticalStorage `json:"analyticalStorage"`
}

// CosmosDBThroughputInfo contains throughput information
type CosmosDBThroughputInfo struct {
	ThroughputType        string `json:"throughputType"` // "manual" or "autoscale"
	ProvisionedThroughput int32  `json:"provisionedThroughput"`
	MaxThroughput         int32  `json:"maxThroughput"`
	MinThroughput         int32  `json:"minThroughput"`
}

// CosmosDBIndexingPolicy contains indexing policy information
type CosmosDBIndexingPolicy struct {
	IndexingMode     string                 `json:"indexingMode"`
	Automatic        bool                   `json:"automatic"`
	IncludedPaths    []CosmosDBIndexPath    `json:"includedPaths"`
	ExcludedPaths    []CosmosDBIndexPath    `json:"excludedPaths"`
	CompositeIndexes [][]CosmosDBIndexPath  `json:"compositeIndexes"`
	SpatialIndexes   []CosmosDBSpatialIndex `json:"spatialIndexes"`
}

// CosmosDBIndexPath represents an index path
type CosmosDBIndexPath struct {
	Path    string                `json:"path"`
	Indexes []CosmosDBIndexConfig `json:"indexes"`
}

// CosmosDBIndexConfig represents index configuration
type CosmosDBIndexConfig struct {
	Kind      string `json:"kind"`     // "Hash", "Range", "Spatial"
	DataType  string `json:"dataType"` // "String", "Number", "Point", "Polygon", "LineString"
	Precision int32  `json:"precision"`
}

// CosmosDBSpatialIndex represents spatial index configuration
type CosmosDBSpatialIndex struct {
	Path  string   `json:"path"`
	Types []string `json:"types"` // "Point", "LineString", "Polygon", "MultiPolygon"
}

// CosmosDBUniqueKeyPolicy represents unique key policy
type CosmosDBUniqueKeyPolicy struct {
	UniqueKeys []CosmosDBUniqueKey `json:"uniqueKeys"`
}

// CosmosDBUniqueKey represents a unique key constraint
type CosmosDBUniqueKey struct {
	Paths []string `json:"paths"`
}

// CosmosDBConflictPolicy represents conflict resolution policy
type CosmosDBConflictPolicy struct {
	Mode                        string `json:"mode"` // "LastWriterWins", "Custom"
	ConflictResolutionPath      string `json:"conflictResolutionPath"`
	ConflictResolutionProcedure string `json:"conflictResolutionProcedure"`
}

// CosmosDBAnalyticalStorage represents analytical storage configuration
type CosmosDBAnalyticalStorage struct {
	TimeToLiveInSeconds int32 `json:"timeToLiveInSeconds"`
}
