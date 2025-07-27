package cosmosdb

import (
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// CosmosDBDetails contains information about a CosmosDB database
type CosmosDBDetails struct {
	UniqueIdentifier string `json:"uniqueIdentifier"`
	DatabaseType     string `json:"databaseType"`
	DatabaseEdition  string `json:"databaseEdition"`
	Version          string `json:"version"`
	DatabaseSize     int64  `json:"databaseSize"`
	AccountName      string `json:"accountName"`
	Region           string `json:"region"`
	ConsistencyLevel string `json:"consistencyLevel"`
	API              string `json:"api"`
}

// DatabaseSchema represents the schema of a CosmosDB database
type DatabaseSchema struct {
	Containers []common.TableInfo          `json:"containers"` // CosmosDB containers are similar to tables
	EnumTypes  []common.EnumInfo           `json:"enumTypes"`
	Schemas    []common.DatabaseSchemaInfo `json:"schemas"`
	Functions  []common.FunctionInfo       `json:"functions"`
	Triggers   []common.TriggerInfo        `json:"triggers"`
	Procedures []common.ProcedureInfo      `json:"procedures"`
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
