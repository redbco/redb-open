package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// CreatePostgresUnifiedModel creates a UnifiedModel for PostgreSQL with database details
func CreatePostgresUnifiedModel(uniqueIdentifier, version string, databaseSize int64) *unifiedmodel.UnifiedModel {
	um := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.PostgreSQL,
		Tables:       make(map[string]unifiedmodel.Table),
		Schemas:      make(map[string]unifiedmodel.Schema),
		Types:        make(map[string]unifiedmodel.Type),
		Functions:    make(map[string]unifiedmodel.Function),
		Triggers:     make(map[string]unifiedmodel.Trigger),
		Sequences:    make(map[string]unifiedmodel.Sequence),
		Extensions:   make(map[string]unifiedmodel.Extension),
	}
	return um
}

// ConvertCommonTableToUnified converts common.TableInfo to unifiedmodel.Table
func ConvertCommonTableToUnified(tableInfo common.TableInfo) unifiedmodel.Table {
	table := unifiedmodel.Table{
		Name:        tableInfo.Name,
		Columns:     make(map[string]unifiedmodel.Column),
		Indexes:     make(map[string]unifiedmodel.Index),
		Constraints: make(map[string]unifiedmodel.Constraint),
	}

	// Convert columns
	for _, col := range tableInfo.Columns {
		column := unifiedmodel.Column{
			Name:          col.Name,
			DataType:      col.DataType,
			Nullable:      col.IsNullable,
			IsPrimaryKey:  col.IsPrimaryKey,
			AutoIncrement: col.IsAutoIncrement,
		}
		if col.ColumnDefault != nil {
			column.Default = *col.ColumnDefault
		}
		if col.GenerationExpression != nil {
			column.GeneratedExpression = *col.GenerationExpression
		}
		table.Columns[col.Name] = column
	}

	// Convert indexes
	for _, idx := range tableInfo.Indexes {
		index := unifiedmodel.Index{
			Name:    idx.Name,
			Columns: idx.Columns,
			Unique:  idx.IsUnique,
		}
		table.Indexes[idx.Name] = index
	}

	// Convert constraints
	for _, constraint := range tableInfo.Constraints {
		constraintType := unifiedmodel.ConstraintTypePrimaryKey
		switch constraint.Type {
		case "PRIMARY KEY":
			constraintType = unifiedmodel.ConstraintTypePrimaryKey
		case "FOREIGN KEY":
			constraintType = unifiedmodel.ConstraintTypeForeignKey
		case "UNIQUE":
			constraintType = unifiedmodel.ConstraintTypeUnique
		case "CHECK":
			constraintType = unifiedmodel.ConstraintTypeCheck
		case "NOT NULL":
			constraintType = unifiedmodel.ConstraintTypeNotNull
		}

		unifiedConstraint := unifiedmodel.Constraint{
			Name:    constraint.Name,
			Type:    constraintType,
			Columns: []string{constraint.Column},
		}

		if constraint.ForeignTable != "" {
			unifiedConstraint.Reference = unifiedmodel.Reference{
				Table:    constraint.ForeignTable,
				Columns:  []string{constraint.ForeignColumn},
				OnUpdate: constraint.OnUpdate,
				OnDelete: constraint.OnDelete,
			}
		}

		table.Constraints[constraint.Name] = unifiedConstraint
	}

	return table
}

type PostgresReplicationSourceDetails struct {
	SlotName        string                       `json:"slot_name"`
	PublicationName string                       `json:"publication_name"`
	DatabaseID      string                       `json:"database_id"`
	ReplicationConn *pgconn.PgConn               `json:"-"`
	StopChan        chan struct{}                `json:"-"`
	isActive        bool                         `json:"-"`
	EventHandler    func(map[string]interface{}) `json:"-"`
	TableNames      map[string]struct{}          `json:"table_names"` // Set of tables being replicated
}

// AddTable adds a table to the replication source
func (p *PostgresReplicationSourceDetails) AddTable(table string) {
	if p.TableNames == nil {
		p.TableNames = make(map[string]struct{})
	}
	p.TableNames[table] = struct{}{}
}

// RemoveTable removes a table from the replication source
func (p *PostgresReplicationSourceDetails) RemoveTable(table string) {
	if p.TableNames != nil {
		delete(p.TableNames, table)
	}
}

// HasTable checks if the replication source is replicating a given table
func (p *PostgresReplicationSourceDetails) HasTable(table string) bool {
	_, ok := p.TableNames[table]
	return ok
}

// GetTables returns a slice of all tables being replicated
func (p *PostgresReplicationSourceDetails) GetTables() []string {
	tables := make([]string, 0, len(p.TableNames))
	for t := range p.TableNames {
		tables = append(tables, t)
	}
	return tables
}

// Implement ReplicationSourceInterface
func (p *PostgresReplicationSourceDetails) GetSourceID() string {
	return p.SlotName
}

func (p *PostgresReplicationSourceDetails) GetDatabaseID() string {
	return p.DatabaseID
}

func (p *PostgresReplicationSourceDetails) GetStatus() map[string]interface{} {
	return map[string]interface{}{
		"slot_name":        p.SlotName,
		"publication_name": p.PublicationName,
		"table_names":      p.GetTables(),
		"database_id":      p.DatabaseID,
		"is_active":        p.isActive,
		"has_connection":   p.ReplicationConn != nil,
	}
}

func (p *PostgresReplicationSourceDetails) Start() error {
	if p.ReplicationConn == nil {
		return fmt.Errorf("no replication connection available")
	}

	if p.isActive {
		return nil // Already active
	}

	// Start the replication streaming for all tables
	go streamReplicationEvents(p.ReplicationConn, p, p.EventHandler, nil)
	p.isActive = true
	return nil
}

func (p *PostgresReplicationSourceDetails) Stop() error {
	if !p.isActive {
		return nil // Already stopped
	}

	if p.StopChan != nil {
		close(p.StopChan)
		p.StopChan = make(chan struct{}) // Create new channel for future use
	}

	p.isActive = false
	return nil
}

func (p *PostgresReplicationSourceDetails) IsActive() bool {
	return p.isActive
}

func (p *PostgresReplicationSourceDetails) GetMetadata() map[string]interface{} {
	return map[string]interface{}{
		"slot_name":        p.SlotName,
		"publication_name": p.PublicationName,
		"table_names":      p.GetTables(),
		"database_id":      p.DatabaseID,
	}
}

func (p *PostgresReplicationSourceDetails) Close() error {
	// Stop replication first
	if err := p.Stop(); err != nil {
		return fmt.Errorf("failed to stop replication: %w", err)
	}

	// Close the replication connection
	if p.ReplicationConn != nil {
		p.ReplicationConn.Close(context.Background())
		p.ReplicationConn = nil
	}

	return nil
}

type PostgresReplicationChange struct {
	Operation string                 `json:"operation"`
	TableName string                 `json:"table_name"`
	Data      map[string]interface{} `json:"data"`
	OldData   map[string]interface{} `json:"old_data,omitempty"`
}
