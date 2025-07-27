package models

// MariaDBModel represents the MariaDB-specific schema model
type MariaDBModel struct {
	SchemaType string
	Schemas    []Schema
	Tables     []MariaDBTable
	Enums      []Enum
	Functions  []Function
	Triggers   []Trigger
	Extensions []Extension
	Indexes    []Index
}

// MariaDBTable extends the base Table type with MariaDB-specific features
type MariaDBTable struct {
	TableType         string
	Name              string
	Schema            string
	Columns           []MariaDBColumn
	Indexes           []Index
	Constraints       []Constraint
	ParentTable       string
	PartitionValue    string
	PartitionStrategy string
	PartitionKeys     []string
	Partitions        []string
	ViewDefinition    string
	Owner             string
	Engine            string // Storage engine (InnoDB, MyISAM, etc.)
	AutoIncrement     int64  // Next auto-increment value
	RowFormat         string // Row format (COMPACT, DYNAMIC, etc.)
	CharacterSet      string // Table character set
	Collation         string // Table collation
	Comment           string // Table comment
	PartitionInfo     *MariaDBPartitionInfo
}

// MariaDBPartitionInfo represents MariaDB-specific partitioning information
type MariaDBPartitionInfo struct {
	Type             string   // Partition type (RANGE, LIST, HASH, KEY)
	Expression       string   // Partition expression
	Partitions       []string // Partition names
	PartitionKeys    []string // Partition key columns
	SubPartitionBy   string   // Sub-partition type
	SubPartitionExpr string   // Sub-partition expression
	SubPartitions    int      // Number of sub-partitions
}

// MariaDBColumn extends the base Column type with MariaDB-specific features
type MariaDBColumn struct {
	Name                 string
	DataType             DataType
	IsNullable           bool
	IsPrimaryKey         bool
	IsUnique             bool
	IsAutoIncrement      bool
	IsGenerated          bool
	DefaultIsFunction    bool
	DefaultValueFunction string
	DefaultValue         *string
	Constraints          []Constraint
	Collation            string
	CharacterSet         string // Column character set
	GenerationExpr       string // Generated column expression
	IsStored             bool   // Whether the generated column is stored
	IsVirtual            bool   // Whether the generated column is virtual
	Comment              string // Column comment
	OnUpdate             string // ON UPDATE expression for TIMESTAMP columns
}

// GetPrimaryKey returns the primary key constraint for the table
func (t *MariaDBTable) GetPrimaryKey() *Constraint {
	for _, constraint := range t.Constraints {
		if constraint.Type == "PRIMARY KEY" {
			return &constraint
		}
	}
	return nil
}

// ToUnifiedModel converts a MariaDBModel to a UnifiedModel
func (m *MariaDBModel) ToUnifiedModel() *UnifiedModel {
	unified := &UnifiedModel{
		SchemaType: "unified",
		Schemas:    m.Schemas,
		Enums:      m.Enums,
		Functions:  m.Functions,
		Triggers:   m.Triggers,
		Extensions: m.Extensions,
		Indexes:    m.Indexes,
	}

	// Convert MariaDB tables to unified tables
	for _, mt := range m.Tables {
		t := Table{
			TableType:         mt.TableType,
			Name:              mt.Name,
			Schema:            mt.Schema,
			Columns:           make([]Column, len(mt.Columns)),
			Indexes:           mt.Indexes,
			Constraints:       mt.Constraints,
			ParentTable:       mt.ParentTable,
			PartitionValue:    mt.PartitionValue,
			PartitionStrategy: mt.PartitionStrategy,
			PartitionKeys:     mt.PartitionKeys,
			Partitions:        mt.Partitions,
			ViewDefinition:    mt.ViewDefinition,
			Owner:             mt.Owner,
		}

		// Convert MariaDB columns to unified columns
		for i, mc := range mt.Columns {
			t.Columns[i] = Column{
				Name:                 mc.Name,
				DataType:             mc.DataType,
				IsNullable:           mc.IsNullable,
				IsPrimaryKey:         mc.IsPrimaryKey,
				IsUnique:             mc.IsUnique,
				IsAutoIncrement:      mc.IsAutoIncrement,
				IsGenerated:          mc.IsGenerated,
				DefaultIsFunction:    mc.DefaultIsFunction,
				DefaultValueFunction: mc.DefaultValueFunction,
				DefaultValue:         mc.DefaultValue,
				Constraints:          mc.Constraints,
				Collation:            mc.Collation,
			}
		}

		unified.Tables = append(unified.Tables, t)
	}

	return unified
}

// ToMariaDBTable converts a Table to a MariaDBTable
func (t *Table) ToMariaDBTable() *MariaDBTable {
	mariadbTable := &MariaDBTable{
		TableType:         t.TableType,
		Name:              t.Name,
		Schema:            t.Schema,
		Columns:           make([]MariaDBColumn, len(t.Columns)),
		Indexes:           t.Indexes,
		Constraints:       t.Constraints,
		ParentTable:       t.ParentTable,
		PartitionValue:    t.PartitionValue,
		PartitionStrategy: t.PartitionStrategy,
		PartitionKeys:     t.PartitionKeys,
		Partitions:        t.Partitions,
		ViewDefinition:    t.ViewDefinition,
		Owner:             t.Owner,
	}

	// Convert columns
	for i, c := range t.Columns {
		mariadbTable.Columns[i] = MariaDBColumn{
			Name:                 c.Name,
			DataType:             c.DataType,
			IsNullable:           c.IsNullable,
			IsPrimaryKey:         c.IsPrimaryKey,
			IsUnique:             c.IsUnique,
			IsAutoIncrement:      c.IsAutoIncrement,
			IsGenerated:          c.IsGenerated,
			DefaultIsFunction:    c.DefaultIsFunction,
			DefaultValueFunction: c.DefaultValueFunction,
			DefaultValue:         c.DefaultValue,
			Constraints:          c.Constraints,
			Collation:            c.Collation,
		}
	}

	return mariadbTable
}
