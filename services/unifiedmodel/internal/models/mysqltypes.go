package models

// MySQLModel represents the MySQL-specific schema model
type MySQLModel struct {
	SchemaType string
	Schemas    []Schema
	Tables     []MySQLTable
	Enums      []Enum
	Functions  []Function
	Triggers   []Trigger
	Extensions []Extension
	Indexes    []Index
}

// MySQLTable extends the base Table type with MySQL-specific features
type MySQLTable struct {
	TableType         string
	Name              string
	Schema            string
	Columns           []MySQLColumn
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
	PartitionInfo     *MySQLPartitionInfo
}

// MySQLPartitionInfo represents MySQL-specific partitioning information
type MySQLPartitionInfo struct {
	Type           string   // Partition type (RANGE, LIST, HASH, KEY)
	Expression     string   // Partition expression
	Partitions     []string // Partition names
	PartitionKeys  []string // Partition key columns
	SubPartitionBy string   // Sub-partition type
	SubPartitions  int      // Number of sub-partitions
}

// MySQLColumn extends the base Column type with MySQL-specific features
type MySQLColumn struct {
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

// ToUnifiedModel converts a MySQLModel to a UnifiedModel
func (m *MySQLModel) ToUnifiedModel() *UnifiedModel {
	unified := &UnifiedModel{
		SchemaType: "unified",
		Schemas:    m.Schemas,
		Enums:      m.Enums,
		Functions:  m.Functions,
		Triggers:   m.Triggers,
		Extensions: m.Extensions,
		Indexes:    m.Indexes,
	}

	// Convert MySQL tables to unified tables
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

		// Convert MySQL columns to unified columns
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

// ToMySQLTable converts a Table to a MySQLTable
func (t *Table) ToMySQLTable() *MySQLTable {
	mysqlTable := &MySQLTable{
		TableType:         t.TableType,
		Name:              t.Name,
		Schema:            t.Schema,
		Columns:           make([]MySQLColumn, len(t.Columns)),
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
		mysqlTable.Columns[i] = MySQLColumn{
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

	return mysqlTable
}
