package models

// UnifiedModel represents the neutral schema model
type UnifiedModel struct {
	SchemaType     string // "unified" for the unified model
	Schemas        []Schema
	Tables         []Table // Tables also include views and materialized views
	Enums          []Enum
	CompositeTypes []CompositeType
	Domains        []Domain
	Ranges         []Range
	Functions      []Function
	Triggers       []Trigger
	Sequences      []Sequence
	Extensions     []Extension
	Indexes        []Index
}

type Schema struct {
	Name         string
	CharacterSet string
	Collation    string
	Comment      string
}

type Index struct {
	Name           string        // Name is the identifier for the index (e.g. "idx_users_email").
	Schema         string        // Schema is the schema or namespace in which the index will be created. Common in PostgreSQL or other databases that support named schemas.
	Table          string        // Table is the name of the table (or indexed object) on which the index is defined.
	Columns        []IndexColumn // Columns contains the list of columns or expressions used in the index.
	IncludeColumns []string      // IncludeColumns is a list of columns that are not part of the key but are included in the index. Supported by some databases like SQL Server (INCLUDE clause).
	IsUnique       bool          // IsUnique indicates whether this index enforces uniqueness.
	IndexMethod    string        // IndexMethod specifies the indexing method or access method used (e.g. "BTREE", "HASH", "GIN", "CLUSTERED").
	WhereClause    string        // WhereClause allows partial indexing, limiting which rows get indexed. Primarily used in PostgreSQL.
	Concurrency    bool          // Concurrency configures whether an index build is performed concurrently (e.g. PostgreSQL's CONCURRENTLY).
	FillFactor     int           // FillFactor (or equivalent) controls how much the index pages are filled to reduce page splits, if supported by the database.
	Tablespace     string        // Tablespace is where the index should be physically stored, if supported.
	Collation      string        // Collation is used if you want to override the default collation for text columns in the index, if supported by the database.
	Comment        string        // Comment or remark about this index, if supported by the database.
	Owner          string        // Owner is the user or role that owns this index, if the DB allows explicitly setting index ownership.
}

type IndexColumn struct {
	ColumnName   string // ColumnName is the name of the column to index. If using an expression, leave ColumnName empty and set Expression instead.
	Order        int    // Order can be "ASC" or "DESC". Some databases allow specifying this per column.
	Expression   string // Expression is the expression to index (e.g. "lower(email)"). Used if this index is built on a function or expression instead of a raw column.
	NullPosition int    // NullPosition can be "FIRST", "LAST", or empty. Some databases (like PostgreSQL) allow controlling how NULL values are sorted.
	Length       int    // Length is the length of the column to index.
}

type Table struct {
	TableType         string       // Type of table ("standard", "indexed", "partitioned", "partition", "view", "materialized", "temporary").
	Name              string       // Name of the table
	Schema            string       // Schema of the table
	Columns           []Column     // Columns in the table
	Indexes           []Index      // Indexes on the table
	Constraints       []Constraint // Constraints on the table
	ParentTable       string       // For partitioned tables
	PartitionValue    string       // For partitioned tables
	PartitionStrategy string       // For partitioned tables
	PartitionKeys     []string     // For partitioned tables
	Partitions        []string     // For partitioned tables
	ViewDefinition    string       // For views
	Owner             string       // Owner of the table
	Comment           string       // Comment on the table
}

type Column struct {
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
	Comment              string
}

type DataType struct {
	Name            string           // Name of the data type
	TypeCategory    string           // "basic", "array", "enum", "domain", "composite", "range", "extension"
	BaseType        string           // Basic type name (int, varchar, etc.)
	IsArray         bool             // Whether this is an array type
	ArrayDimensions int              // Number of array dimensions (1 for single array, 2 for array of arrays, etc.)
	IsEnum          bool             // Whether this is an enum type
	EnumValues      []string         // Values if this is an enum
	IsDomain        bool             // Whether this is a domain type
	IsUnsigned      bool             // Whether this is an unsigned type
	DomainBase      *DataType        // Base type if this is a domain type
	IsComposite     bool             // Whether this is a composite type
	CompositeFields []CompositeField // Fields if this is a composite type
	IsRange         bool             // Whether this is a range type
	RangeSubtype    *DataType        // Subtype if this is a range type
	IsExtension     bool             // Whether this is an extension-defined type
	ExtensionName   string           // Name of the extension if IsExtension is true
	CustomTypeName  string           // Name of the custom type (for USER-DEFINED types)
	Length          int              // Length for string types
	Precision       int              // Precision for numeric types
	Scale           int              // Scale for numeric types
	Modifiers       []string         // Additional type modifiers
	Schema          string           // Schema where the type is defined (for user-defined types)
}

type Constraint struct {
	Type              string   // The constraint type (e.g. "PRIMARY KEY", "FOREIGN KEY", "UNIQUE", "CHECK", "NOT NULL").
	Name              string   // Name of the constraint (e.g. "pk_orders_id", "fk_orders_customers").
	Table             string   // The table name where the constraint is defined.
	Columns           []string // The list of columns involved in this constraint.
	Deferrable        bool     // Whether the constraint is DEFERRABLE (useful in databases like PostgreSQL/Oracle). If true, the constraint check can be deferred until the end of a transaction.
	InitiallyDeferred string   // The initial enforcement mode if Deferrable is true (valid values might include "DEFERRED", "IMMEDIATE").
	Enabled           bool     // Whether the constraint is currently enabled.
	Validated         bool     // Whether the constraint has been validated. In some systems, a constraint can be created or enabled without validation on existing rows.
	CheckExpression   string   // An expression that must evaluate to true for CHECK constraints. Used only if Type == "CHECK".
	ReferencedTable   string   // The name of the referenced table for a FOREIGN KEY constraint. Used only if Type == "FOREIGN KEY".
	ReferencedColumns []string // The columns in the referenced table for a FOREIGN KEY constraint. Used only if Type == "FOREIGN KEY".
	OnUpdate          string   // The action to take if a referenced row is updated (e.g. "CASCADE", "SET NULL", "RESTRICT", "NO ACTION"). Used only if Type == "FOREIGN KEY".
	OnDelete          string   // The action to take if a referenced row is deleted (e.g. "CASCADE", "SET NULL", "RESTRICT", "NO ACTION"). Used only if Type == "FOREIGN KEY".
	UsingIndex        string   // Name or settings for a supporting index, if applicable (e.g., for Oracle or PostgreSQL when creating a unique constraint with a specific index).
}

type Enum struct {
	Name         string   // The name of the ENUM type (e.g. "mood_enum").
	Values       []string // The allowed values in the ENUM type (e.g. ["sad", "ok", "happy"]).
	Schema       string   // The schema or namespace in which this ENUM type is created. (Commonly used in PostgreSQL or other systems supporting schemas.)
	IsExtensible bool     // Whether the ENUM can be modified (e.g., adding or renaming values) after initial creation. Some databases (like PostgreSQL) support certain ALTER TYPE operations, while others do not.
	Collation    string   // The collation used by the ENUM values, if supported by the database engine.
	Comment      string   // An optional human-readable comment or description for the ENUM type.
	Owner        string   // The owner of the ENUM type, if the database supports specifying or transferring ownership.
}

type CompositeType struct {
	Name    string
	Fields  []CompositeField
	Schema  string
	Comment string
	Owner   string
}

type CompositeField struct {
	Name         string
	DataType     DataType
	Collation    string
	IsNullable   bool
	DefaultValue string
}

type Domain struct {
	Name            string
	BaseType        string
	Schema          string
	Collation       string
	IsNullable      bool
	ColumnDefault   string
	CheckConstraint string
	Comment         string
	Owner           string
}

type Range struct {
	Name                string
	Schema              string
	Subtype             string
	CanonicalFunction   string
	SubtypeDiffFunction string
	MultirangeType      string
	Comment             string
	Owner               string
}

type FunctionParameter struct {
	Name     string
	DataType string
}

type Function struct {
	Name            string
	Schema          string
	Arguments       []FunctionParameter
	ReturnType      string
	IsDeterministic bool
	Definition      string
}

type Trigger struct {
	Name       string
	Schema     string
	Table      string
	Event      string
	Definition string
	Timing     string
}

type Extension struct {
	Name        string
	Schema      string
	Version     string
	Description string
}

type Sequence struct {
	Name      string
	Schema    string
	DataType  string
	Start     int64
	Increment int64
	MaxValue  int64
	MinValue  int64
	CacheSize int64
	Cycle     bool
}
