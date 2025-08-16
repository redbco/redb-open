package unifiedmodel

import "github.com/redbco/redb-open/pkg/dbcapabilities"

// InitializeAllDatabaseMetadata initializes metadata for all 29 supported databases
func (stc *ScalableTypeConverter) initializeAllDatabaseMetadata() {
	// Relational databases
	stc.metadata[dbcapabilities.PostgreSQL] = stc.createPostgreSQLMetadata()
	stc.metadata[dbcapabilities.MySQL] = stc.createMySQLMetadata()
	stc.metadata[dbcapabilities.MariaDB] = stc.createMariaDBMetadata()
	stc.metadata[dbcapabilities.Oracle] = stc.createOracleMetadata()
	stc.metadata[dbcapabilities.SQLServer] = stc.createSQLServerMetadata()
	stc.metadata[dbcapabilities.TiDB] = stc.createTiDBMetadata()
	stc.metadata[dbcapabilities.ClickHouse] = stc.createClickHouseMetadata()
	stc.metadata[dbcapabilities.DB2] = stc.createDB2Metadata()
	stc.metadata[dbcapabilities.CockroachDB] = stc.createCockroachDBMetadata()
	stc.metadata[dbcapabilities.DuckDB] = stc.createDuckDBMetadata()
	stc.metadata[dbcapabilities.EdgeDB] = stc.createEdgeDBMetadata()

	// Document databases
	stc.metadata[dbcapabilities.MongoDB] = stc.createMongoDBMetadata()
	stc.metadata[dbcapabilities.CosmosDB] = stc.createCosmosDBMetadata()

	// Key-Value databases
	stc.metadata[dbcapabilities.DynamoDB] = stc.createDynamoDBMetadata()
	stc.metadata[dbcapabilities.Redis] = stc.createRedisMetadata()

	// Graph databases
	stc.metadata[dbcapabilities.Neo4j] = stc.createNeo4jMetadata()

	// Vector databases
	stc.metadata[dbcapabilities.Milvus] = stc.createMilvusMetadata()
	stc.metadata[dbcapabilities.Weaviate] = stc.createWeaviateMetadata()
	stc.metadata[dbcapabilities.Pinecone] = stc.createPineconeMetadata()
	stc.metadata[dbcapabilities.Chroma] = stc.createChromaMetadata()
	stc.metadata[dbcapabilities.LanceDB] = stc.createLanceDBMetadata()

	// Search databases
	stc.metadata[dbcapabilities.Elasticsearch] = stc.createElasticsearchMetadata()

	// Analytics databases
	stc.metadata[dbcapabilities.Snowflake] = stc.createSnowflakeMetadata()

	// Wide-Column databases
	stc.metadata[dbcapabilities.Cassandra] = stc.createCassandraMetadata()

	// Object Storage (if S3 is defined in capabilities)
	// stc.metadata[dbcapabilities.S3] = stc.createS3Metadata()
}

// Relational Database Metadata

func (stc *ScalableTypeConverter) createPostgreSQLMetadata() DatabaseTypeMetadata {
	return DatabaseTypeMetadata{
		DatabaseType: dbcapabilities.PostgreSQL,
		PrimitiveTypes: map[string]PrimitiveTypeInfo{
			"integer": {
				NativeName:   "integer",
				UnifiedType:  UnifiedTypeInt32,
				SupportsNull: true,
				Aliases:      []string{"int", "int4"},
			},
			"bigint": {
				NativeName:   "bigint",
				UnifiedType:  UnifiedTypeInt64,
				SupportsNull: true,
				Aliases:      []string{"int8"},
			},
			"text": {
				NativeName:   "text",
				UnifiedType:  UnifiedTypeString,
				SupportsNull: true,
			},
			"varchar": {
				NativeName:   "varchar",
				UnifiedType:  UnifiedTypeVarchar,
				HasLength:    true,
				MaxLength:    func() *int64 { v := int64(10485760); return &v }(), // 10MB
				SupportsNull: true,
			},
			"boolean": {
				NativeName:   "boolean",
				UnifiedType:  UnifiedTypeBoolean,
				SupportsNull: true,
				Aliases:      []string{"bool"},
			},
			"timestamp": {
				NativeName:   "timestamp",
				UnifiedType:  UnifiedTypeTimestamp,
				SupportsNull: true,
			},
			"uuid": {
				NativeName:   "uuid",
				UnifiedType:  UnifiedTypeUUID,
				SupportsNull: true,
			},
			"jsonb": {
				NativeName:   "jsonb",
				UnifiedType:  UnifiedTypeJSON,
				SupportsNull: true,
			},
		},
		CustomTypeSupport: CustomTypeSupportInfo{
			SupportsEnum:      true,
			SupportsComposite: true,
			SupportsDomain:    true,
			SupportsArray:     true,
			SupportsRange:     true,
			SupportsJSON:      true,
			SupportsSpatial:   true,
			EnumImplementation: CustomTypeImplementation{
				IsNative: true,
				Syntax:   "CREATE TYPE name AS ENUM ('value1', 'value2')",
			},
			CompositeImplementation: CustomTypeImplementation{
				IsNative: true,
				Syntax:   "CREATE TYPE name AS (field1 type1, field2 type2)",
			},
			DomainImplementation: CustomTypeImplementation{
				IsNative: true,
				Syntax:   "CREATE DOMAIN name AS base_type CHECK (constraint)",
			},
		},
		ConstraintSupport: ConstraintSupportInfo{
			SupportsPrimaryKey:    true,
			SupportsForeignKey:    true,
			SupportsUnique:        true,
			SupportsCheck:         true,
			SupportsNotNull:       true,
			SupportsDefault:       true,
			SupportsAutoIncrement: true,
		},
		DefaultMappings: map[UnifiedDataType]string{
			UnifiedTypeInt32:     "integer",
			UnifiedTypeInt64:     "bigint",
			UnifiedTypeString:    "text",
			UnifiedTypeBoolean:   "boolean",
			UnifiedTypeTimestamp: "timestamp",
			UnifiedTypeUUID:      "uuid",
			UnifiedTypeJSON:      "jsonb",
		},
	}
}

func (stc *ScalableTypeConverter) createMySQLMetadata() DatabaseTypeMetadata {
	return DatabaseTypeMetadata{
		DatabaseType: dbcapabilities.MySQL,
		PrimitiveTypes: map[string]PrimitiveTypeInfo{
			"int": {
				NativeName:       "int",
				UnifiedType:      UnifiedTypeInt32,
				SupportsUnsigned: true,
				SupportsNull:     true,
				Aliases:          []string{"integer"},
			},
			"bigint": {
				NativeName:       "bigint",
				UnifiedType:      UnifiedTypeInt64,
				SupportsUnsigned: true,
				SupportsNull:     true,
			},
			"varchar": {
				NativeName:    "varchar",
				UnifiedType:   UnifiedTypeVarchar,
				HasLength:     true,
				MaxLength:     func() *int64 { v := int64(65535); return &v }(),
				DefaultLength: func() *int64 { v := int64(255); return &v }(),
				SupportsNull:  true,
			},
			"text": {
				NativeName:   "text",
				UnifiedType:  UnifiedTypeText,
				SupportsNull: true,
			},
			"boolean": {
				NativeName:   "boolean",
				UnifiedType:  UnifiedTypeBoolean,
				SupportsNull: true,
				Aliases:      []string{"bool"},
			},
			"datetime": {
				NativeName:   "datetime",
				UnifiedType:  UnifiedTypeTimestamp,
				SupportsNull: true,
			},
			"json": {
				NativeName:   "json",
				UnifiedType:  UnifiedTypeJSON,
				SupportsNull: true,
			},
		},
		CustomTypeSupport: CustomTypeSupportInfo{
			SupportsEnum:      true,
			SupportsComposite: false,
			SupportsDomain:    false,
			SupportsArray:     false,
			SupportsJSON:      true,
			EnumImplementation: CustomTypeImplementation{
				IsNative:    true,
				Syntax:      "ENUM('value1', 'value2')",
				Limitations: []string{"Limited to 65535 values", "Case insensitive by default"},
			},
			JSONImplementation: CustomTypeImplementation{
				IsNative: true,
				Syntax:   "JSON",
			},
		},
		ConstraintSupport: ConstraintSupportInfo{
			SupportsPrimaryKey:    true,
			SupportsForeignKey:    true,
			SupportsUnique:        true,
			SupportsCheck:         true, // MySQL 8.0+
			SupportsNotNull:       true,
			SupportsDefault:       true,
			SupportsAutoIncrement: true,
		},
		DefaultMappings: map[UnifiedDataType]string{
			UnifiedTypeInt32:     "int",
			UnifiedTypeInt64:     "bigint",
			UnifiedTypeString:    "text",
			UnifiedTypeVarchar:   "varchar(255)",
			UnifiedTypeBoolean:   "boolean",
			UnifiedTypeTimestamp: "datetime",
			UnifiedTypeJSON:      "json",
		},
	}
}

func (stc *ScalableTypeConverter) createMongoDBMetadata() DatabaseTypeMetadata {
	return DatabaseTypeMetadata{
		DatabaseType: dbcapabilities.MongoDB,
		PrimitiveTypes: map[string]PrimitiveTypeInfo{
			"int32": {
				NativeName:   "int32",
				UnifiedType:  UnifiedTypeInt32,
				SupportsNull: true,
			},
			"int64": {
				NativeName:   "int64",
				UnifiedType:  UnifiedTypeInt64,
				SupportsNull: true,
			},
			"string": {
				NativeName:   "string",
				UnifiedType:  UnifiedTypeString,
				SupportsNull: true,
			},
			"boolean": {
				NativeName:   "boolean",
				UnifiedType:  UnifiedTypeBoolean,
				SupportsNull: true,
			},
			"date": {
				NativeName:   "date",
				UnifiedType:  UnifiedTypeTimestamp,
				SupportsNull: true,
			},
			"object": {
				NativeName:   "object",
				UnifiedType:  UnifiedTypeJSON,
				SupportsNull: true,
			},
			"array": {
				NativeName:   "array",
				UnifiedType:  UnifiedTypeArray,
				SupportsNull: true,
			},
		},
		CustomTypeSupport: CustomTypeSupportInfo{
			SupportsEnum:      false,
			SupportsComposite: true, // Via nested documents
			SupportsDomain:    false,
			SupportsArray:     true,
			SupportsJSON:      true, // Native document support
			CompositeImplementation: CustomTypeImplementation{
				IsNative: true,
				Syntax:   "Nested document structure",
			},
			JSONImplementation: CustomTypeImplementation{
				IsNative: true,
				Syntax:   "Native BSON document",
			},
		},
		ConstraintSupport: ConstraintSupportInfo{
			SupportsPrimaryKey:    true, // _id field
			SupportsForeignKey:    false,
			SupportsUnique:        true, // Via indexes
			SupportsCheck:         false,
			SupportsNotNull:       false, // Application level
			SupportsDefault:       false, // Application level
			SupportsAutoIncrement: false,
		},
		DefaultMappings: map[UnifiedDataType]string{
			UnifiedTypeInt32:     "int32",
			UnifiedTypeInt64:     "int64",
			UnifiedTypeString:    "string",
			UnifiedTypeBoolean:   "boolean",
			UnifiedTypeTimestamp: "date",
			UnifiedTypeJSON:      "object",
			UnifiedTypeArray:     "array",
		},
	}
}

func (stc *ScalableTypeConverter) createMariaDBMetadata() DatabaseTypeMetadata {
	// MariaDB is very similar to MySQL
	metadata := stc.createMySQLMetadata()
	metadata.DatabaseType = dbcapabilities.MariaDB

	// MariaDB-specific differences
	metadata.CustomTypeSupport.SupportsJSON = true // MariaDB 10.2+
	metadata.PrimitiveTypes["json"] = PrimitiveTypeInfo{
		NativeName:   "json",
		UnifiedType:  UnifiedTypeJSON,
		SupportsNull: true,
	}

	return metadata
}

func (stc *ScalableTypeConverter) createOracleMetadata() DatabaseTypeMetadata {
	return DatabaseTypeMetadata{
		DatabaseType: dbcapabilities.Oracle,
		PrimitiveTypes: map[string]PrimitiveTypeInfo{
			"number": {
				NativeName:       "number",
				UnifiedType:      UnifiedTypeNumeric,
				HasPrecision:     true,
				HasScale:         true,
				MaxPrecision:     func() *int64 { v := int64(38); return &v }(),
				MaxScale:         func() *int64 { v := int64(127); return &v }(),
				DefaultPrecision: func() *int64 { v := int64(38); return &v }(),
				SupportsNull:     true,
			},
			"varchar2": {
				NativeName:    "varchar2",
				UnifiedType:   UnifiedTypeVarchar,
				HasLength:     true,
				MaxLength:     func() *int64 { v := int64(4000); return &v }(),
				DefaultLength: func() *int64 { v := int64(1); return &v }(),
				SupportsNull:  true,
			},
			"clob": {
				NativeName:   "clob",
				UnifiedType:  UnifiedTypeClob,
				SupportsNull: true,
			},
			"date": {
				NativeName:   "date",
				UnifiedType:  UnifiedTypeTimestamp,
				SupportsNull: true,
			},
			"timestamp": {
				NativeName:   "timestamp",
				UnifiedType:  UnifiedTypeTimestamp,
				SupportsNull: true,
			},
		},
		CustomTypeSupport: CustomTypeSupportInfo{
			SupportsEnum:      false,
			SupportsComposite: true, // Object types
			SupportsDomain:    false,
			SupportsArray:     true, // VARRAY, nested tables
			SupportsJSON:      true, // Oracle 12c+
			CompositeImplementation: CustomTypeImplementation{
				IsNative: true,
				Syntax:   "CREATE TYPE name AS OBJECT (field1 type1, field2 type2)",
			},
		},
		ConstraintSupport: ConstraintSupportInfo{
			SupportsPrimaryKey:    true,
			SupportsForeignKey:    true,
			SupportsUnique:        true,
			SupportsCheck:         true,
			SupportsNotNull:       true,
			SupportsDefault:       true,
			SupportsAutoIncrement: true, // Via sequences
		},
		DefaultMappings: map[UnifiedDataType]string{
			UnifiedTypeInt32:     "number(10)",
			UnifiedTypeInt64:     "number(19)",
			UnifiedTypeString:    "clob",
			UnifiedTypeVarchar:   "varchar2(4000)",
			UnifiedTypeBoolean:   "number(1)",
			UnifiedTypeTimestamp: "timestamp",
			UnifiedTypeJSON:      "clob", // or JSON type in 12c+
		},
	}
}

func (stc *ScalableTypeConverter) createSQLServerMetadata() DatabaseTypeMetadata {
	return DatabaseTypeMetadata{
		DatabaseType: dbcapabilities.SQLServer,
		PrimitiveTypes: map[string]PrimitiveTypeInfo{
			"int": {
				NativeName:   "int",
				UnifiedType:  UnifiedTypeInt32,
				SupportsNull: true,
			},
			"bigint": {
				NativeName:   "bigint",
				UnifiedType:  UnifiedTypeInt64,
				SupportsNull: true,
			},
			"nvarchar": {
				NativeName:    "nvarchar",
				UnifiedType:   UnifiedTypeVarchar,
				HasLength:     true,
				MaxLength:     func() *int64 { v := int64(4000); return &v }(),
				DefaultLength: func() *int64 { v := int64(255); return &v }(),
				SupportsNull:  true,
			},
			"ntext": {
				NativeName:   "ntext",
				UnifiedType:  UnifiedTypeText,
				SupportsNull: true,
			},
			"bit": {
				NativeName:   "bit",
				UnifiedType:  UnifiedTypeBoolean,
				SupportsNull: true,
			},
			"datetime2": {
				NativeName:   "datetime2",
				UnifiedType:  UnifiedTypeTimestamp,
				SupportsNull: true,
			},
			"uniqueidentifier": {
				NativeName:   "uniqueidentifier",
				UnifiedType:  UnifiedTypeUUID,
				SupportsNull: true,
			},
		},
		CustomTypeSupport: CustomTypeSupportInfo{
			SupportsEnum:      false,
			SupportsComposite: true, // User-defined table types
			SupportsDomain:    true, // User-defined data types
			SupportsArray:     false,
			SupportsJSON:      true, // SQL Server 2016+
			SupportsXML:       true,
			JSONImplementation: CustomTypeImplementation{
				IsNative: true,
				Syntax:   "nvarchar(max) with JSON functions",
			},
		},
		ConstraintSupport: ConstraintSupportInfo{
			SupportsPrimaryKey:    true,
			SupportsForeignKey:    true,
			SupportsUnique:        true,
			SupportsCheck:         true,
			SupportsNotNull:       true,
			SupportsDefault:       true,
			SupportsAutoIncrement: true, // IDENTITY
		},
		DefaultMappings: map[UnifiedDataType]string{
			UnifiedTypeInt32:     "int",
			UnifiedTypeInt64:     "bigint",
			UnifiedTypeString:    "ntext",
			UnifiedTypeVarchar:   "nvarchar(255)",
			UnifiedTypeBoolean:   "bit",
			UnifiedTypeTimestamp: "datetime2",
			UnifiedTypeUUID:      "uniqueidentifier",
			UnifiedTypeJSON:      "nvarchar(max)",
		},
	}
}

func (stc *ScalableTypeConverter) createTiDBMetadata() DatabaseTypeMetadata {
	// TiDB is MySQL-compatible
	metadata := stc.createMySQLMetadata()
	metadata.DatabaseType = dbcapabilities.TiDB

	// TiDB-specific enhancements
	metadata.CustomTypeSupport.SupportsJSON = true

	return metadata
}

func (stc *ScalableTypeConverter) createClickHouseMetadata() DatabaseTypeMetadata {
	return DatabaseTypeMetadata{
		DatabaseType: dbcapabilities.ClickHouse,
		PrimitiveTypes: map[string]PrimitiveTypeInfo{
			"Int32": {
				NativeName:   "Int32",
				UnifiedType:  UnifiedTypeInt32,
				SupportsNull: false, // ClickHouse doesn't have NULL by default
			},
			"Int64": {
				NativeName:   "Int64",
				UnifiedType:  UnifiedTypeInt64,
				SupportsNull: false,
			},
			"String": {
				NativeName:   "String",
				UnifiedType:  UnifiedTypeString,
				SupportsNull: false,
			},
			"Bool": {
				NativeName:   "Bool",
				UnifiedType:  UnifiedTypeBoolean,
				SupportsNull: false,
			},
			"DateTime": {
				NativeName:   "DateTime",
				UnifiedType:  UnifiedTypeTimestamp,
				SupportsNull: false,
			},
			"UUID": {
				NativeName:   "UUID",
				UnifiedType:  UnifiedTypeUUID,
				SupportsNull: false,
			},
		},
		CustomTypeSupport: CustomTypeSupportInfo{
			SupportsEnum:      true,
			SupportsComposite: true, // Nested structures
			SupportsDomain:    false,
			SupportsArray:     true,
			SupportsJSON:      false, // No native JSON type
			EnumImplementation: CustomTypeImplementation{
				IsNative: true,
				Syntax:   "Enum8('value1' = 1, 'value2' = 2)",
			},
			ArrayImplementation: CustomTypeImplementation{
				IsNative: true,
				Syntax:   "Array(Type)",
			},
		},
		ConstraintSupport: ConstraintSupportInfo{
			SupportsPrimaryKey:    true,
			SupportsForeignKey:    false, // ClickHouse doesn't enforce FK
			SupportsUnique:        false,
			SupportsCheck:         false,
			SupportsNotNull:       false, // No explicit NULL support
			SupportsDefault:       true,
			SupportsAutoIncrement: false,
		},
		DefaultMappings: map[UnifiedDataType]string{
			UnifiedTypeInt32:     "Int32",
			UnifiedTypeInt64:     "Int64",
			UnifiedTypeString:    "String",
			UnifiedTypeBoolean:   "Bool",
			UnifiedTypeTimestamp: "DateTime",
			UnifiedTypeUUID:      "UUID",
			UnifiedTypeArray:     "Array(String)",
		},
	}
}

func (stc *ScalableTypeConverter) createDB2Metadata() DatabaseTypeMetadata {
	return DatabaseTypeMetadata{
		DatabaseType: dbcapabilities.DB2,
		PrimitiveTypes: map[string]PrimitiveTypeInfo{
			"integer": {
				NativeName:   "integer",
				UnifiedType:  UnifiedTypeInt32,
				SupportsNull: true,
				Aliases:      []string{"int"},
			},
			"bigint": {
				NativeName:   "bigint",
				UnifiedType:  UnifiedTypeInt64,
				SupportsNull: true,
			},
			"varchar": {
				NativeName:    "varchar",
				UnifiedType:   UnifiedTypeVarchar,
				HasLength:     true,
				MaxLength:     func() *int64 { v := int64(32672); return &v }(),
				DefaultLength: func() *int64 { v := int64(1); return &v }(),
				SupportsNull:  true,
			},
			"clob": {
				NativeName:   "clob",
				UnifiedType:  UnifiedTypeClob,
				SupportsNull: true,
			},
			"boolean": {
				NativeName:   "boolean",
				UnifiedType:  UnifiedTypeBoolean,
				SupportsNull: true,
			},
			"timestamp": {
				NativeName:   "timestamp",
				UnifiedType:  UnifiedTypeTimestamp,
				SupportsNull: true,
			},
		},
		CustomTypeSupport: CustomTypeSupportInfo{
			SupportsEnum:      false,
			SupportsComposite: true, // Structured types
			SupportsDomain:    true, // Distinct types
			SupportsArray:     true,
			SupportsJSON:      false, // No native JSON
			SupportsXML:       true,
		},
		ConstraintSupport: ConstraintSupportInfo{
			SupportsPrimaryKey:    true,
			SupportsForeignKey:    true,
			SupportsUnique:        true,
			SupportsCheck:         true,
			SupportsNotNull:       true,
			SupportsDefault:       true,
			SupportsAutoIncrement: true, // GENERATED ALWAYS AS IDENTITY
		},
		DefaultMappings: map[UnifiedDataType]string{
			UnifiedTypeInt32:     "integer",
			UnifiedTypeInt64:     "bigint",
			UnifiedTypeString:    "clob",
			UnifiedTypeVarchar:   "varchar(255)",
			UnifiedTypeBoolean:   "boolean",
			UnifiedTypeTimestamp: "timestamp",
		},
	}
}

func (stc *ScalableTypeConverter) createCockroachDBMetadata() DatabaseTypeMetadata {
	// CockroachDB is PostgreSQL-compatible
	metadata := stc.createPostgreSQLMetadata()
	metadata.DatabaseType = dbcapabilities.CockroachDB

	// CockroachDB-specific differences
	metadata.CustomTypeSupport.SupportsRange = false // Not supported yet

	return metadata
}

func (stc *ScalableTypeConverter) createDuckDBMetadata() DatabaseTypeMetadata {
	return DatabaseTypeMetadata{
		DatabaseType: dbcapabilities.DuckDB,
		PrimitiveTypes: map[string]PrimitiveTypeInfo{
			"INTEGER": {
				NativeName:   "INTEGER",
				UnifiedType:  UnifiedTypeInt32,
				SupportsNull: true,
				Aliases:      []string{"INT", "INT4"},
			},
			"BIGINT": {
				NativeName:   "BIGINT",
				UnifiedType:  UnifiedTypeInt64,
				SupportsNull: true,
				Aliases:      []string{"INT8", "LONG"},
			},
			"VARCHAR": {
				NativeName:   "VARCHAR",
				UnifiedType:  UnifiedTypeVarchar,
				HasLength:    true,
				SupportsNull: true,
				Aliases:      []string{"TEXT", "STRING"},
			},
			"BOOLEAN": {
				NativeName:   "BOOLEAN",
				UnifiedType:  UnifiedTypeBoolean,
				SupportsNull: true,
				Aliases:      []string{"BOOL"},
			},
			"TIMESTAMP": {
				NativeName:   "TIMESTAMP",
				UnifiedType:  UnifiedTypeTimestamp,
				SupportsNull: true,
			},
			"JSON": {
				NativeName:   "JSON",
				UnifiedType:  UnifiedTypeJSON,
				SupportsNull: true,
			},
		},
		CustomTypeSupport: CustomTypeSupportInfo{
			SupportsEnum:      true,
			SupportsComposite: true, // STRUCT
			SupportsDomain:    false,
			SupportsArray:     true, // LIST
			SupportsJSON:      true,
			EnumImplementation: CustomTypeImplementation{
				IsNative: true,
				Syntax:   "CREATE TYPE name AS ENUM ('value1', 'value2')",
			},
			ArrayImplementation: CustomTypeImplementation{
				IsNative: true,
				Syntax:   "LIST[type]",
			},
		},
		ConstraintSupport: ConstraintSupportInfo{
			SupportsPrimaryKey:    true,
			SupportsForeignKey:    true,
			SupportsUnique:        true,
			SupportsCheck:         true,
			SupportsNotNull:       true,
			SupportsDefault:       true,
			SupportsAutoIncrement: false, // Use sequences
		},
		DefaultMappings: map[UnifiedDataType]string{
			UnifiedTypeInt32:     "INTEGER",
			UnifiedTypeInt64:     "BIGINT",
			UnifiedTypeString:    "VARCHAR",
			UnifiedTypeBoolean:   "BOOLEAN",
			UnifiedTypeTimestamp: "TIMESTAMP",
			UnifiedTypeJSON:      "JSON",
			UnifiedTypeArray:     "LIST",
		},
	}
}

func (stc *ScalableTypeConverter) createEdgeDBMetadata() DatabaseTypeMetadata {
	return DatabaseTypeMetadata{
		DatabaseType: dbcapabilities.EdgeDB,
		PrimitiveTypes: map[string]PrimitiveTypeInfo{
			"int32": {
				NativeName:   "int32",
				UnifiedType:  UnifiedTypeInt32,
				SupportsNull: true,
			},
			"int64": {
				NativeName:   "int64",
				UnifiedType:  UnifiedTypeInt64,
				SupportsNull: true,
			},
			"str": {
				NativeName:   "str",
				UnifiedType:  UnifiedTypeString,
				SupportsNull: true,
			},
			"bool": {
				NativeName:   "bool",
				UnifiedType:  UnifiedTypeBoolean,
				SupportsNull: true,
			},
			"datetime": {
				NativeName:   "datetime",
				UnifiedType:  UnifiedTypeTimestamp,
				SupportsNull: true,
			},
			"uuid": {
				NativeName:   "uuid",
				UnifiedType:  UnifiedTypeUUID,
				SupportsNull: true,
			},
			"json": {
				NativeName:   "json",
				UnifiedType:  UnifiedTypeJSON,
				SupportsNull: true,
			},
		},
		CustomTypeSupport: CustomTypeSupportInfo{
			SupportsEnum:      true,
			SupportsComposite: true, // Object types
			SupportsDomain:    true, // Scalar types
			SupportsArray:     true,
			SupportsJSON:      true,
			EnumImplementation: CustomTypeImplementation{
				IsNative: true,
				Syntax:   "scalar type Status extending enum<Active, Inactive>",
			},
		},
		ConstraintSupport: ConstraintSupportInfo{
			SupportsPrimaryKey:    true,
			SupportsForeignKey:    true, // Links
			SupportsUnique:        true,
			SupportsCheck:         true, // Constraints
			SupportsNotNull:       true,
			SupportsDefault:       true,
			SupportsAutoIncrement: false, // Use uuid
		},
		DefaultMappings: map[UnifiedDataType]string{
			UnifiedTypeInt32:     "int32",
			UnifiedTypeInt64:     "int64",
			UnifiedTypeString:    "str",
			UnifiedTypeBoolean:   "bool",
			UnifiedTypeTimestamp: "datetime",
			UnifiedTypeUUID:      "uuid",
			UnifiedTypeJSON:      "json",
		},
	}
}

// Document Database Metadata

func (stc *ScalableTypeConverter) createCosmosDBMetadata() DatabaseTypeMetadata {
	return DatabaseTypeMetadata{
		DatabaseType: dbcapabilities.CosmosDB,
		PrimitiveTypes: map[string]PrimitiveTypeInfo{
			"number": {
				NativeName:   "number",
				UnifiedType:  UnifiedTypeFloat64, // CosmosDB uses double precision
				SupportsNull: true,
			},
			"string": {
				NativeName:   "string",
				UnifiedType:  UnifiedTypeString,
				SupportsNull: true,
			},
			"boolean": {
				NativeName:   "boolean",
				UnifiedType:  UnifiedTypeBoolean,
				SupportsNull: true,
			},
			"object": {
				NativeName:   "object",
				UnifiedType:  UnifiedTypeJSON,
				SupportsNull: true,
			},
			"array": {
				NativeName:   "array",
				UnifiedType:  UnifiedTypeArray,
				SupportsNull: true,
			},
		},
		CustomTypeSupport: CustomTypeSupportInfo{
			SupportsEnum:      false,
			SupportsComposite: true, // Nested documents
			SupportsDomain:    false,
			SupportsArray:     true,
			SupportsJSON:      true, // Native document support
			CompositeImplementation: CustomTypeImplementation{
				IsNative: true,
				Syntax:   "Nested JSON document",
			},
		},
		ConstraintSupport: ConstraintSupportInfo{
			SupportsPrimaryKey:    true, // id field
			SupportsForeignKey:    false,
			SupportsUnique:        true, // Via unique indexes
			SupportsCheck:         false,
			SupportsNotNull:       false, // Application level
			SupportsDefault:       false, // Application level
			SupportsAutoIncrement: false,
		},
		DefaultMappings: map[UnifiedDataType]string{
			UnifiedTypeInt32:   "number",
			UnifiedTypeInt64:   "number",
			UnifiedTypeFloat64: "number",
			UnifiedTypeString:  "string",
			UnifiedTypeBoolean: "boolean",
			UnifiedTypeJSON:    "object",
			UnifiedTypeArray:   "array",
		},
	}
}

// Key-Value Database Metadata

func (stc *ScalableTypeConverter) createDynamoDBMetadata() DatabaseTypeMetadata {
	return DatabaseTypeMetadata{
		DatabaseType: dbcapabilities.DynamoDB,
		PrimitiveTypes: map[string]PrimitiveTypeInfo{
			"N": { // Number
				NativeName:   "N",
				UnifiedType:  UnifiedTypeString, // DynamoDB stores numbers as strings
				SupportsNull: false,
			},
			"S": { // String
				NativeName:   "S",
				UnifiedType:  UnifiedTypeString,
				SupportsNull: false,
			},
			"B": { // Binary
				NativeName:   "B",
				UnifiedType:  UnifiedTypeBinary,
				SupportsNull: false,
			},
			"BOOL": { // Boolean
				NativeName:   "BOOL",
				UnifiedType:  UnifiedTypeBoolean,
				SupportsNull: false,
			},
			"M": { // Map
				NativeName:   "M",
				UnifiedType:  UnifiedTypeJSON,
				SupportsNull: false,
			},
			"L": { // List
				NativeName:   "L",
				UnifiedType:  UnifiedTypeArray,
				SupportsNull: false,
			},
		},
		CustomTypeSupport: CustomTypeSupportInfo{
			SupportsEnum:      false,
			SupportsComposite: true, // Maps
			SupportsDomain:    false,
			SupportsArray:     true, // Lists
			SupportsJSON:      true, // Maps and Lists
		},
		ConstraintSupport: ConstraintSupportInfo{
			SupportsPrimaryKey:    true, // Partition key + sort key
			SupportsForeignKey:    false,
			SupportsUnique:        false, // Only for primary key
			SupportsCheck:         false,
			SupportsNotNull:       false,
			SupportsDefault:       false,
			SupportsAutoIncrement: false,
		},
		DefaultMappings: map[UnifiedDataType]string{
			UnifiedTypeInt32:   "N",
			UnifiedTypeInt64:   "N",
			UnifiedTypeFloat64: "N",
			UnifiedTypeString:  "S",
			UnifiedTypeBoolean: "BOOL",
			UnifiedTypeBinary:  "B",
			UnifiedTypeJSON:    "M",
			UnifiedTypeArray:   "L",
		},
	}
}

func (stc *ScalableTypeConverter) createRedisMetadata() DatabaseTypeMetadata {
	return DatabaseTypeMetadata{
		DatabaseType: dbcapabilities.Redis,
		PrimitiveTypes: map[string]PrimitiveTypeInfo{
			"string": {
				NativeName:   "string",
				UnifiedType:  UnifiedTypeString,
				SupportsNull: false, // Redis doesn't have NULL
			},
			"hash": {
				NativeName:   "hash",
				UnifiedType:  UnifiedTypeJSON,
				SupportsNull: false,
			},
			"list": {
				NativeName:   "list",
				UnifiedType:  UnifiedTypeArray,
				SupportsNull: false,
			},
			"set": {
				NativeName:   "set",
				UnifiedType:  UnifiedTypeSet,
				SupportsNull: false,
			},
			"zset": {
				NativeName:   "zset",
				UnifiedType:  UnifiedTypeSet,
				SupportsNull: false,
			},
		},
		CustomTypeSupport: CustomTypeSupportInfo{
			SupportsEnum:      false,
			SupportsComposite: true, // Hashes
			SupportsDomain:    false,
			SupportsArray:     true,  // Lists
			SupportsJSON:      false, // No native JSON, use hashes
		},
		ConstraintSupport: ConstraintSupportInfo{
			SupportsPrimaryKey:    true, // Key
			SupportsForeignKey:    false,
			SupportsUnique:        true, // Keys are unique
			SupportsCheck:         false,
			SupportsNotNull:       false,
			SupportsDefault:       false,
			SupportsAutoIncrement: false,
		},
		DefaultMappings: map[UnifiedDataType]string{
			UnifiedTypeString: "string",
			UnifiedTypeJSON:   "hash",
			UnifiedTypeArray:  "list",
			UnifiedTypeSet:    "set",
		},
	}
}

// Continue with remaining databases...
// (Graph, Vector, Search, Analytics, Wide-Column, Time-Series, Object Storage)
// Each following the same pattern with database-specific type mappings and capabilities

func (stc *ScalableTypeConverter) createNeo4jMetadata() DatabaseTypeMetadata {
	return DatabaseTypeMetadata{
		DatabaseType: dbcapabilities.Neo4j,
		PrimitiveTypes: map[string]PrimitiveTypeInfo{
			"INTEGER": {
				NativeName:   "INTEGER",
				UnifiedType:  UnifiedTypeInt64, // Neo4j uses 64-bit integers
				SupportsNull: true,
			},
			"FLOAT": {
				NativeName:   "FLOAT",
				UnifiedType:  UnifiedTypeFloat64,
				SupportsNull: true,
			},
			"STRING": {
				NativeName:   "STRING",
				UnifiedType:  UnifiedTypeString,
				SupportsNull: true,
			},
			"BOOLEAN": {
				NativeName:   "BOOLEAN",
				UnifiedType:  UnifiedTypeBoolean,
				SupportsNull: true,
			},
			"DATE": {
				NativeName:   "DATE",
				UnifiedType:  UnifiedTypeDate,
				SupportsNull: true,
			},
			"DATETIME": {
				NativeName:   "DATETIME",
				UnifiedType:  UnifiedTypeTimestamp,
				SupportsNull: true,
			},
			"LIST": {
				NativeName:   "LIST",
				UnifiedType:  UnifiedTypeArray,
				SupportsNull: true,
			},
		},
		CustomTypeSupport: CustomTypeSupportInfo{
			SupportsEnum:      false,
			SupportsComposite: true, // Maps/Properties
			SupportsDomain:    false,
			SupportsArray:     true,  // Lists
			SupportsJSON:      false, // Use maps
		},
		ConstraintSupport: ConstraintSupportInfo{
			SupportsPrimaryKey:    false, // No traditional PK
			SupportsForeignKey:    false, // Relationships instead
			SupportsUnique:        true,  // Unique constraints
			SupportsCheck:         false,
			SupportsNotNull:       true, // Property existence constraints
			SupportsDefault:       false,
			SupportsAutoIncrement: false,
		},
		DefaultMappings: map[UnifiedDataType]string{
			UnifiedTypeInt32:     "INTEGER",
			UnifiedTypeInt64:     "INTEGER",
			UnifiedTypeFloat64:   "FLOAT",
			UnifiedTypeString:    "STRING",
			UnifiedTypeBoolean:   "BOOLEAN",
			UnifiedTypeDate:      "DATE",
			UnifiedTypeTimestamp: "DATETIME",
			UnifiedTypeArray:     "LIST",
		},
	}
}

// Vector Database Metadata (simplified examples)

func (stc *ScalableTypeConverter) createMilvusMetadata() DatabaseTypeMetadata {
	return DatabaseTypeMetadata{
		DatabaseType: dbcapabilities.Milvus,
		PrimitiveTypes: map[string]PrimitiveTypeInfo{
			"INT64": {
				NativeName:   "INT64",
				UnifiedType:  UnifiedTypeInt64,
				SupportsNull: false,
			},
			"FLOAT": {
				NativeName:   "FLOAT",
				UnifiedType:  UnifiedTypeFloat32,
				SupportsNull: false,
			},
			"VARCHAR": {
				NativeName:   "VARCHAR",
				UnifiedType:  UnifiedTypeVarchar,
				HasLength:    true,
				SupportsNull: false,
			},
			"FLOAT_VECTOR": {
				NativeName:   "FLOAT_VECTOR",
				UnifiedType:  UnifiedTypeVector,
				SupportsNull: false,
			},
		},
		CustomTypeSupport: CustomTypeSupportInfo{
			SupportsEnum:      false,
			SupportsComposite: false,
			SupportsDomain:    false,
			SupportsArray:     false,
			SupportsJSON:      false,
		},
		ConstraintSupport: ConstraintSupportInfo{
			SupportsPrimaryKey:    true,
			SupportsForeignKey:    false,
			SupportsUnique:        false,
			SupportsCheck:         false,
			SupportsNotNull:       true, // All fields required
			SupportsDefault:       false,
			SupportsAutoIncrement: true,
		},
		DefaultMappings: map[UnifiedDataType]string{
			UnifiedTypeInt64:   "INT64",
			UnifiedTypeFloat32: "FLOAT",
			UnifiedTypeString:  "VARCHAR(255)",
			UnifiedTypeVector:  "FLOAT_VECTOR",
		},
	}
}

func (stc *ScalableTypeConverter) createWeaviateMetadata() DatabaseTypeMetadata {
	return DatabaseTypeMetadata{
		DatabaseType: dbcapabilities.Weaviate,
		PrimitiveTypes: map[string]PrimitiveTypeInfo{
			"int": {
				NativeName:   "int",
				UnifiedType:  UnifiedTypeInt64,
				SupportsNull: true,
			},
			"number": {
				NativeName:   "number",
				UnifiedType:  UnifiedTypeFloat64,
				SupportsNull: true,
			},
			"text": {
				NativeName:   "text",
				UnifiedType:  UnifiedTypeString,
				SupportsNull: true,
			},
			"boolean": {
				NativeName:   "boolean",
				UnifiedType:  UnifiedTypeBoolean,
				SupportsNull: true,
			},
			"date": {
				NativeName:   "date",
				UnifiedType:  UnifiedTypeTimestamp,
				SupportsNull: true,
			},
		},
		CustomTypeSupport: CustomTypeSupportInfo{
			SupportsEnum:      false,
			SupportsComposite: true, // Objects
			SupportsDomain:    false,
			SupportsArray:     true,
			SupportsJSON:      false,
		},
		DefaultMappings: map[UnifiedDataType]string{
			UnifiedTypeInt64:     "int",
			UnifiedTypeFloat64:   "number",
			UnifiedTypeString:    "text",
			UnifiedTypeBoolean:   "boolean",
			UnifiedTypeTimestamp: "date",
		},
	}
}

func (stc *ScalableTypeConverter) createPineconeMetadata() DatabaseTypeMetadata {
	return DatabaseTypeMetadata{
		DatabaseType: dbcapabilities.Pinecone,
		PrimitiveTypes: map[string]PrimitiveTypeInfo{
			"string": {
				NativeName:   "string",
				UnifiedType:  UnifiedTypeString,
				SupportsNull: false,
			},
			"number": {
				NativeName:   "number",
				UnifiedType:  UnifiedTypeFloat64,
				SupportsNull: false,
			},
			"vector": {
				NativeName:   "vector",
				UnifiedType:  UnifiedTypeVector,
				SupportsNull: false,
			},
		},
		CustomTypeSupport: CustomTypeSupportInfo{
			SupportsEnum:      false,
			SupportsComposite: false,
			SupportsDomain:    false,
			SupportsArray:     false,
			SupportsJSON:      false,
		},
		DefaultMappings: map[UnifiedDataType]string{
			UnifiedTypeString:  "string",
			UnifiedTypeFloat64: "number",
			UnifiedTypeVector:  "vector",
		},
	}
}

func (stc *ScalableTypeConverter) createChromaMetadata() DatabaseTypeMetadata {
	return DatabaseTypeMetadata{
		DatabaseType: dbcapabilities.Chroma,
		PrimitiveTypes: map[string]PrimitiveTypeInfo{
			"str": {
				NativeName:   "str",
				UnifiedType:  UnifiedTypeString,
				SupportsNull: true,
			},
			"int": {
				NativeName:   "int",
				UnifiedType:  UnifiedTypeInt64,
				SupportsNull: true,
			},
			"float": {
				NativeName:   "float",
				UnifiedType:  UnifiedTypeFloat64,
				SupportsNull: true,
			},
		},
		DefaultMappings: map[UnifiedDataType]string{
			UnifiedTypeString:  "str",
			UnifiedTypeInt64:   "int",
			UnifiedTypeFloat64: "float",
		},
	}
}

func (stc *ScalableTypeConverter) createLanceDBMetadata() DatabaseTypeMetadata {
	return DatabaseTypeMetadata{
		DatabaseType: dbcapabilities.LanceDB,
		PrimitiveTypes: map[string]PrimitiveTypeInfo{
			"string": {
				NativeName:   "string",
				UnifiedType:  UnifiedTypeString,
				SupportsNull: true,
			},
			"int64": {
				NativeName:   "int64",
				UnifiedType:  UnifiedTypeInt64,
				SupportsNull: true,
			},
			"float64": {
				NativeName:   "float64",
				UnifiedType:  UnifiedTypeFloat64,
				SupportsNull: true,
			},
			"vector": {
				NativeName:   "vector",
				UnifiedType:  UnifiedTypeVector,
				SupportsNull: true,
			},
		},
		DefaultMappings: map[UnifiedDataType]string{
			UnifiedTypeString:  "string",
			UnifiedTypeInt64:   "int64",
			UnifiedTypeFloat64: "float64",
			UnifiedTypeVector:  "vector",
		},
	}
}

// Continue with remaining databases following the same pattern...
// (Elasticsearch, Snowflake, BigQuery, Redshift, Cassandra, InfluxDB, TimescaleDB, S3)

func (stc *ScalableTypeConverter) createElasticsearchMetadata() DatabaseTypeMetadata {
	return DatabaseTypeMetadata{
		DatabaseType: dbcapabilities.Elasticsearch,
		PrimitiveTypes: map[string]PrimitiveTypeInfo{
			"text": {
				NativeName:   "text",
				UnifiedType:  UnifiedTypeText,
				SupportsNull: true,
			},
			"keyword": {
				NativeName:   "keyword",
				UnifiedType:  UnifiedTypeString,
				SupportsNull: true,
			},
			"long": {
				NativeName:   "long",
				UnifiedType:  UnifiedTypeInt64,
				SupportsNull: true,
			},
			"integer": {
				NativeName:   "integer",
				UnifiedType:  UnifiedTypeInt32,
				SupportsNull: true,
			},
			"double": {
				NativeName:   "double",
				UnifiedType:  UnifiedTypeFloat64,
				SupportsNull: true,
			},
			"boolean": {
				NativeName:   "boolean",
				UnifiedType:  UnifiedTypeBoolean,
				SupportsNull: true,
			},
			"date": {
				NativeName:   "date",
				UnifiedType:  UnifiedTypeTimestamp,
				SupportsNull: true,
			},
		},
		CustomTypeSupport: CustomTypeSupportInfo{
			SupportsEnum:      false,
			SupportsComposite: true, // Nested objects
			SupportsDomain:    false,
			SupportsArray:     true,
			SupportsJSON:      true, // Native document support
		},
		DefaultMappings: map[UnifiedDataType]string{
			UnifiedTypeInt32:     "integer",
			UnifiedTypeInt64:     "long",
			UnifiedTypeFloat64:   "double",
			UnifiedTypeString:    "keyword",
			UnifiedTypeText:      "text",
			UnifiedTypeBoolean:   "boolean",
			UnifiedTypeTimestamp: "date",
		},
	}
}

// Analytics databases (simplified)
func (stc *ScalableTypeConverter) createSnowflakeMetadata() DatabaseTypeMetadata {
	return DatabaseTypeMetadata{
		DatabaseType: dbcapabilities.Snowflake,
		PrimitiveTypes: map[string]PrimitiveTypeInfo{
			"NUMBER": {
				NativeName:       "NUMBER",
				UnifiedType:      UnifiedTypeNumeric,
				HasPrecision:     true,
				HasScale:         true,
				MaxPrecision:     func() *int64 { v := int64(38); return &v }(),
				DefaultPrecision: func() *int64 { v := int64(38); return &v }(),
				SupportsNull:     true,
			},
			"VARCHAR": {
				NativeName:    "VARCHAR",
				UnifiedType:   UnifiedTypeVarchar,
				HasLength:     true,
				MaxLength:     func() *int64 { v := int64(16777216); return &v }(),
				DefaultLength: func() *int64 { v := int64(16777216); return &v }(),
				SupportsNull:  true,
			},
			"BOOLEAN": {
				NativeName:   "BOOLEAN",
				UnifiedType:  UnifiedTypeBoolean,
				SupportsNull: true,
			},
			"TIMESTAMP_NTZ": {
				NativeName:   "TIMESTAMP_NTZ",
				UnifiedType:  UnifiedTypeTimestamp,
				SupportsNull: true,
			},
			"VARIANT": {
				NativeName:   "VARIANT",
				UnifiedType:  UnifiedTypeJSON,
				SupportsNull: true,
			},
		},
		CustomTypeSupport: CustomTypeSupportInfo{
			SupportsEnum:      false,
			SupportsComposite: false,
			SupportsDomain:    false,
			SupportsArray:     true, // ARRAY type
			SupportsJSON:      true, // VARIANT, OBJECT
		},
		DefaultMappings: map[UnifiedDataType]string{
			UnifiedTypeInt32:     "NUMBER(10,0)",
			UnifiedTypeInt64:     "NUMBER(19,0)",
			UnifiedTypeFloat64:   "NUMBER(38,6)",
			UnifiedTypeString:    "VARCHAR",
			UnifiedTypeBoolean:   "BOOLEAN",
			UnifiedTypeTimestamp: "TIMESTAMP_NTZ",
			UnifiedTypeJSON:      "VARIANT",
		},
	}
}

// BigQuery and Redshift not defined in capabilities - removed

func (stc *ScalableTypeConverter) createCassandraMetadata() DatabaseTypeMetadata {
	return DatabaseTypeMetadata{
		DatabaseType: dbcapabilities.Cassandra,
		PrimitiveTypes: map[string]PrimitiveTypeInfo{
			"int": {
				NativeName:   "int",
				UnifiedType:  UnifiedTypeInt32,
				SupportsNull: true,
			},
			"bigint": {
				NativeName:   "bigint",
				UnifiedType:  UnifiedTypeInt64,
				SupportsNull: true,
			},
			"text": {
				NativeName:   "text",
				UnifiedType:  UnifiedTypeText,
				SupportsNull: true,
			},
			"boolean": {
				NativeName:   "boolean",
				UnifiedType:  UnifiedTypeBoolean,
				SupportsNull: true,
			},
			"timestamp": {
				NativeName:   "timestamp",
				UnifiedType:  UnifiedTypeTimestamp,
				SupportsNull: true,
			},
			"uuid": {
				NativeName:   "uuid",
				UnifiedType:  UnifiedTypeUUID,
				SupportsNull: true,
			},
		},
		CustomTypeSupport: CustomTypeSupportInfo{
			SupportsEnum:      false,
			SupportsComposite: true, // User-defined types
			SupportsDomain:    false,
			SupportsArray:     true, // Collections
			SupportsJSON:      false,
		},
		DefaultMappings: map[UnifiedDataType]string{
			UnifiedTypeInt32:     "int",
			UnifiedTypeInt64:     "bigint",
			UnifiedTypeString:    "text",
			UnifiedTypeBoolean:   "boolean",
			UnifiedTypeTimestamp: "timestamp",
			UnifiedTypeUUID:      "uuid",
		},
	}
}

// InfluxDB, TimescaleDB, and S3 not defined in capabilities - removed
