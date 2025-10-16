package adapter

import (
	"context"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// UnsupportedSchemaOperator is a nil object pattern for databases that don't support schema operations.
type UnsupportedSchemaOperator struct {
	dbType dbcapabilities.DatabaseType
}

func (u *UnsupportedSchemaOperator) DiscoverSchema(ctx context.Context) (*unifiedmodel.UnifiedModel, error) {
	return nil, NewUnsupportedOperationError(u.dbType, "schema discovery", "")
}

func (u *UnsupportedSchemaOperator) CreateStructure(ctx context.Context, model *unifiedmodel.UnifiedModel) error {
	return NewUnsupportedOperationError(u.dbType, "schema creation", "")
}

func (u *UnsupportedSchemaOperator) ListTables(ctx context.Context) ([]string, error) {
	return nil, NewUnsupportedOperationError(u.dbType, "list tables", "")
}

func (u *UnsupportedSchemaOperator) GetTableSchema(ctx context.Context, tableName string) (*unifiedmodel.Table, error) {
	return nil, NewUnsupportedOperationError(u.dbType, "get table schema", "")
}

// NewUnsupportedSchemaOperator creates a new unsupported schema operator.
func NewUnsupportedSchemaOperator(dbType dbcapabilities.DatabaseType) SchemaOperator {
	return &UnsupportedSchemaOperator{dbType: dbType}
}

// UnsupportedReplicationOperator is a nil object pattern for databases that don't support replication.
type UnsupportedReplicationOperator struct {
	dbType dbcapabilities.DatabaseType
}

func (u *UnsupportedReplicationOperator) IsSupported() bool {
	return false
}

func (u *UnsupportedReplicationOperator) GetSupportedMechanisms() []string {
	return nil
}

func (u *UnsupportedReplicationOperator) CheckPrerequisites(ctx context.Context) error {
	return NewUnsupportedOperationError(u.dbType, "replication", "this database does not support CDC/replication")
}

func (u *UnsupportedReplicationOperator) Connect(ctx context.Context, config ReplicationConfig) (ReplicationSource, error) {
	return nil, NewUnsupportedOperationError(u.dbType, "replication", "this database does not support CDC/replication")
}

func (u *UnsupportedReplicationOperator) GetStatus(ctx context.Context) (map[string]interface{}, error) {
	return nil, NewUnsupportedOperationError(u.dbType, "replication status", "")
}

func (u *UnsupportedReplicationOperator) GetLag(ctx context.Context) (map[string]interface{}, error) {
	return nil, NewUnsupportedOperationError(u.dbType, "replication lag", "")
}

func (u *UnsupportedReplicationOperator) ListSlots(ctx context.Context) ([]map[string]interface{}, error) {
	return nil, NewUnsupportedOperationError(u.dbType, "replication slots", "")
}

func (u *UnsupportedReplicationOperator) DropSlot(ctx context.Context, slotName string) error {
	return NewUnsupportedOperationError(u.dbType, "drop replication slot", "")
}

func (u *UnsupportedReplicationOperator) ListPublications(ctx context.Context) ([]map[string]interface{}, error) {
	return nil, NewUnsupportedOperationError(u.dbType, "replication publications", "")
}

func (u *UnsupportedReplicationOperator) DropPublication(ctx context.Context, publicationName string) error {
	return NewUnsupportedOperationError(u.dbType, "drop publication", "")
}

func (u *UnsupportedReplicationOperator) ParseEvent(ctx context.Context, rawEvent map[string]interface{}) (*CDCEvent, error) {
	return nil, NewUnsupportedOperationError(u.dbType, "parse CDC event", "this database does not support CDC/replication")
}

func (u *UnsupportedReplicationOperator) ApplyCDCEvent(ctx context.Context, event *CDCEvent) error {
	return NewUnsupportedOperationError(u.dbType, "apply CDC event", "this database does not support CDC/replication")
}

func (u *UnsupportedReplicationOperator) TransformData(ctx context.Context, data map[string]interface{}, rules []TransformationRule, transformationServiceEndpoint string) (map[string]interface{}, error) {
	return nil, NewUnsupportedOperationError(u.dbType, "transform data", "this database does not support CDC/replication")
}

// NewUnsupportedReplicationOperator creates a new unsupported replication operator.
func NewUnsupportedReplicationOperator(dbType dbcapabilities.DatabaseType) ReplicationOperator {
	return &UnsupportedReplicationOperator{dbType: dbType}
}

// IsUnsupportedOperator checks if an operator is an unsupported operator.
// This can be used to detect when an operation is not available.
func IsUnsupportedOperator(op interface{}) bool {
	switch op.(type) {
	case *UnsupportedSchemaOperator, *UnsupportedReplicationOperator:
		return true
	default:
		return false
	}
}
