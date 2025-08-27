package iceberg

import (
	"fmt"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// CreateIcebergUnifiedModel creates a UnifiedModel for Iceberg with database details
func CreateIcebergUnifiedModel(uniqueIdentifier, version string, databaseSize int64) *unifiedmodel.UnifiedModel {
	um := &unifiedmodel.UnifiedModel{
		DatabaseType:   dbcapabilities.Iceberg,
		Tables:         make(map[string]unifiedmodel.Table),
		Namespaces:     make(map[string]unifiedmodel.Namespace),
		Views:          make(map[string]unifiedmodel.View),
		ExternalTables: make(map[string]unifiedmodel.ExternalTable),
		Snapshots:      make(map[string]unifiedmodel.Snapshot),
	}
	return um
}

// ConvertIcebergTable converts IcebergTableInfo to unifiedmodel.Table for Iceberg
func ConvertIcebergTable(tableInfo IcebergTableInfo) unifiedmodel.Table {
	table := unifiedmodel.Table{
		Name:        tableInfo.Name,
		Comment:     tableInfo.Location, // Store location as comment for reference
		Columns:     make(map[string]unifiedmodel.Column),
		Indexes:     make(map[string]unifiedmodel.Index),
		Constraints: make(map[string]unifiedmodel.Constraint),
	}

	// Convert columns
	for _, col := range tableInfo.Columns {
		var defaultValue string
		if col.ColumnDefault != nil {
			defaultValue = *col.ColumnDefault
		}
		table.Columns[col.Name] = unifiedmodel.Column{
			Name:         col.Name,
			DataType:     col.DataType,
			Nullable:     col.IsNullable,
			Default:      defaultValue,
			IsPrimaryKey: col.IsPrimaryKey,
		}
	}

	return table
}

// ConvertIcebergNamespace converts IcebergNamespaceInfo to unifiedmodel.Namespace for Iceberg
func ConvertIcebergNamespace(namespaceInfo IcebergNamespaceInfo) unifiedmodel.Namespace {
	return unifiedmodel.Namespace{
		Name: namespaceInfo.Name,
	}
}

// IcebergNamespaceInfo represents an Iceberg namespace (similar to schema)
type IcebergNamespaceInfo struct {
	Name        string            `json:"name"`
	Properties  map[string]string `json:"properties,omitempty"`
	Description string            `json:"description,omitempty"`
}

// IcebergTableInfo extends common.TableInfo with Iceberg-specific metadata
type IcebergTableInfo struct {
	common.TableInfo
	TableUUID         string                  `json:"tableUUID"`
	Location          string                  `json:"location"`
	CurrentSchemaID   int                     `json:"currentSchemaId"`
	CurrentSnapshotID *int64                  `json:"currentSnapshotId,omitempty"`
	Snapshots         []IcebergSnapshotInfo   `json:"snapshots,omitempty"`
	PartitionSpec     []IcebergPartitionField `json:"partitionSpec,omitempty"`
	SortOrder         []IcebergSortField      `json:"sortOrder,omitempty"`
	Properties        map[string]string       `json:"properties,omitempty"`
	Metadata          IcebergTableMetadata    `json:"metadata"`
}

// IcebergSnapshotInfo represents an Iceberg table snapshot
type IcebergSnapshotInfo struct {
	SnapshotID       int64             `json:"snapshotId"`
	ParentSnapshotID *int64            `json:"parentSnapshotId,omitempty"`
	SequenceNumber   int64             `json:"sequenceNumber"`
	TimestampMs      int64             `json:"timestampMs"`
	ManifestList     string            `json:"manifestList"`
	SchemaID         int               `json:"schemaId"`
	Summary          map[string]string `json:"summary,omitempty"`
}

// IcebergPartitionField represents a partition field in Iceberg
type IcebergPartitionField struct {
	SourceID  int    `json:"sourceId"`
	FieldID   int    `json:"fieldId"`
	Transform string `json:"transform"`
	Name      string `json:"name"`
}

// IcebergSortField represents a sort field in Iceberg
type IcebergSortField struct {
	SourceID  int    `json:"sourceId"`
	Transform string `json:"transform"`
	Direction string `json:"direction"` // asc, desc
	NullOrder string `json:"nullOrder"` // nulls-first, nulls-last
}

// IcebergTableMetadata represents the complete metadata of an Iceberg table
type IcebergTableMetadata struct {
	FormatVersion      int                       `json:"formatVersion"`
	TableUUID          string                    `json:"tableUuid"`
	Location           string                    `json:"location"`
	LastSequenceNumber int64                     `json:"lastSequenceNumber"`
	LastUpdatedMs      int64                     `json:"lastUpdatedMs"`
	LastColumnID       int                       `json:"lastColumnId"`
	Schemas            []IcebergSchemaDefinition `json:"schemas"`
	CurrentSchemaID    int                       `json:"currentSchemaId"`
	PartitionSpecs     []IcebergPartitionSpec    `json:"partitionSpecs"`
	DefaultSpecID      int                       `json:"defaultSpecId"`
	LastPartitionID    int                       `json:"lastPartitionId"`
	Properties         map[string]string         `json:"properties"`
	CurrentSnapshotID  *int64                    `json:"currentSnapshotId,omitempty"`
	Refs               map[string]IcebergRef     `json:"refs,omitempty"`
	SnapshotLog        []IcebergSnapshotLogEntry `json:"snapshotLog,omitempty"`
	MetadataLog        []IcebergMetadataLogEntry `json:"metadataLog,omitempty"`
	SortOrders         []IcebergSortOrder        `json:"sortOrders"`
	DefaultSortOrderID int                       `json:"defaultSortOrderId"`
}

// IcebergSchemaDefinition represents an Iceberg schema definition
type IcebergSchemaDefinition struct {
	SchemaID           int                      `json:"schemaId"`
	IdentifierFieldIDs []int                    `json:"identifierFieldIds,omitempty"`
	Fields             []IcebergFieldDefinition `json:"fields"`
}

// IcebergFieldDefinition represents an Iceberg field definition
type IcebergFieldDefinition struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Required bool   `json:"required"`
	Type     string `json:"type"` // Iceberg type string representation
}

// IcebergPartitionSpec represents an Iceberg partition specification
type IcebergPartitionSpec struct {
	SpecID int                     `json:"specId"`
	Fields []IcebergPartitionField `json:"fields"`
}

// IcebergRef represents an Iceberg table reference (branch/tag)
type IcebergRef struct {
	SnapshotID         int64  `json:"snapshotId"`
	Type               string `json:"type"` // branch, tag
	MinSnapshotsToKeep *int   `json:"minSnapshotsToKeep,omitempty"`
	MaxSnapshotAgeMs   *int64 `json:"maxSnapshotAgeMs,omitempty"`
	MaxRefAgeMs        *int64 `json:"maxRefAgeMs,omitempty"`
}

// IcebergSnapshotLogEntry represents an entry in the snapshot log
type IcebergSnapshotLogEntry struct {
	SnapshotID  int64 `json:"snapshotId"`
	TimestampMs int64 `json:"timestampMs"`
}

// IcebergMetadataLogEntry represents an entry in the metadata log
type IcebergMetadataLogEntry struct {
	MetadataFile string `json:"metadataFile"`
	TimestampMs  int64  `json:"timestampMs"`
}

// IcebergSortOrder represents an Iceberg sort order
type IcebergSortOrder struct {
	OrderID int                `json:"orderId"`
	Fields  []IcebergSortField `json:"fields"`
}

// IcebergReplicationSourceDetails represents replication details for Iceberg
// Note: Iceberg doesn't support traditional CDC, so this is mostly a stub
type IcebergReplicationSourceDetails struct {
	DatabaseID   string                       `json:"database_id"`
	CatalogName  string                       `json:"catalog_name"`
	TableNames   map[string]struct{}          `json:"table_names"`
	isActive     bool                         `json:"-"`
	EventHandler func(map[string]interface{}) `json:"-"`
}

// Implement ReplicationSourceInterface for Iceberg (stub implementation)
func (i *IcebergReplicationSourceDetails) GetSourceID() string {
	return fmt.Sprintf("iceberg-%s", i.CatalogName)
}

func (i *IcebergReplicationSourceDetails) GetDatabaseID() string {
	return i.DatabaseID
}

func (i *IcebergReplicationSourceDetails) GetStatus() map[string]interface{} {
	return map[string]interface{}{
		"catalog_name":  i.CatalogName,
		"table_names":   i.GetTables(),
		"database_id":   i.DatabaseID,
		"is_active":     i.isActive,
		"cdc_supported": false, // Iceberg doesn't support traditional CDC
	}
}

func (i *IcebergReplicationSourceDetails) Start() error {
	// Iceberg doesn't support traditional CDC
	return fmt.Errorf("apache iceberg doesn't support traditional Change Data Capture (CDC)")
}

func (i *IcebergReplicationSourceDetails) Stop() error {
	i.isActive = false
	return nil
}

func (i *IcebergReplicationSourceDetails) IsActive() bool {
	return i.isActive
}

func (i *IcebergReplicationSourceDetails) GetMetadata() map[string]interface{} {
	return map[string]interface{}{
		"catalog_name":  i.CatalogName,
		"table_names":   i.GetTables(),
		"database_id":   i.DatabaseID,
		"cdc_supported": false,
	}
}

func (i *IcebergReplicationSourceDetails) Close() error {
	return i.Stop()
}

// AddTable adds a table to the replication source
func (i *IcebergReplicationSourceDetails) AddTable(table string) {
	if i.TableNames == nil {
		i.TableNames = make(map[string]struct{})
	}
	i.TableNames[table] = struct{}{}
}

// RemoveTable removes a table from the replication source
func (i *IcebergReplicationSourceDetails) RemoveTable(table string) {
	if i.TableNames != nil {
		delete(i.TableNames, table)
	}
}

// HasTable checks if the replication source is replicating a given table
func (i *IcebergReplicationSourceDetails) HasTable(table string) bool {
	_, ok := i.TableNames[table]
	return ok
}

// GetTables returns a slice of all tables being replicated
func (i *IcebergReplicationSourceDetails) GetTables() []string {
	tables := make([]string, 0, len(i.TableNames))
	for t := range i.TableNames {
		tables = append(tables, t)
	}
	return tables
}

// IcebergClient represents a connection to an Iceberg catalog
type IcebergClient struct {
	CatalogName   string
	CatalogType   string // REST, Hive, Hadoop, etc.
	BaseURL       string // For REST catalog
	WarehousePath string
	Properties    map[string]string
	HTTPClient    interface{} // HTTP client for REST catalog
	HiveClient    interface{} // Hive metastore client
	HadoopConfig  interface{} // Hadoop configuration
}

// IcebergRestCatalogConfig represents configuration for REST catalog
type IcebergRestCatalogConfig struct {
	URI        string            `json:"uri"`
	Credential string            `json:"credential,omitempty"`
	Token      string            `json:"token,omitempty"`
	Headers    map[string]string `json:"headers,omitempty"`
	Properties map[string]string `json:"properties,omitempty"`
}

// IcebergHiveCatalogConfig represents configuration for Hive catalog
type IcebergHiveCatalogConfig struct {
	URI        string            `json:"uri"`
	Warehouse  string            `json:"warehouse"`
	Properties map[string]string `json:"properties,omitempty"`
}

// IcebergHadoopCatalogConfig represents configuration for Hadoop catalog
type IcebergHadoopCatalogConfig struct {
	Warehouse  string            `json:"warehouse"`
	Properties map[string]string `json:"properties,omitempty"`
}
