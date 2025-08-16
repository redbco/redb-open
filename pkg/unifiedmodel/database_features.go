package unifiedmodel

import (
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// InitializeDatabaseFeatures populates the DatabaseFeatureRegistry with feature definitions
func init() {
	// PostgreSQL features
	DatabaseFeatureRegistry[dbcapabilities.PostgreSQL] = DatabaseFeatureSupport{
		DatabaseType: dbcapabilities.PostgreSQL,
		Paradigms:    []dbcapabilities.DataParadigm{dbcapabilities.ParadigmRelational},
		SupportedObjects: map[ObjectType]ObjectSupport{
			// Data container types
			ObjectTypeTable:            FullSupport(),
			ObjectTypeView:             FullSupport(),
			ObjectTypeMaterializedView: FullSupport(),
			ObjectTypeTemporaryTable:   FullSupport(),
			ObjectTypeExternalTable:    PartialSupport([]string{"foreign data wrappers"}, "Via foreign data wrappers"),
			ObjectTypeForeignTable:     FullSupport(),
			ObjectTypeCollection:       UnsupportedObject([]ObjectType{ObjectTypeTable}, "Use tables instead"),
			ObjectTypeNode:             UnsupportedObject([]ObjectType{ObjectTypeTable}, "Use tables with foreign keys"),
			ObjectTypeVector:           PartialSupport([]string{"requires pgvector extension"}, "Vector support via extension"),

			// Structural definition objects
			ObjectTypeColumn:   FullSupport(),
			ObjectTypeType:     FullSupport(),
			ObjectTypeSequence: FullSupport(),

			// Integrity and performance objects
			ObjectTypeIndex:      FullSupport(),
			ObjectTypeConstraint: FullSupport(),

			// Executable code objects
			ObjectTypeFunction:  FullSupport(),
			ObjectTypeProcedure: FullSupport(),
			ObjectTypeTrigger:   FullSupport(),
			ObjectTypeAggregate: FullSupport(),
			ObjectTypeOperator:  FullSupport(),
			ObjectTypePackage:   UnsupportedObject([]ObjectType{ObjectTypeFunction}, "Use functions and schemas"),
			ObjectTypeRule:      FullSupport(),

			// Security and access control
			ObjectTypeUser:   FullSupport(),
			ObjectTypeRole:   FullSupport(),
			ObjectTypeGrant:  FullSupport(),
			ObjectTypePolicy: FullSupport(),

			// Physical storage
			ObjectTypeTablespace: FullSupport(),
			ObjectTypeDatafile:   UnsupportedObject([]ObjectType{ObjectTypeTablespace}, "Managed by tablespaces"),

			// Connectivity and integration
			ObjectTypeServer:             FullSupport(),
			ObjectTypeConnection:         FullSupport(),
			ObjectTypeForeignDataWrapper: FullSupport(),
			ObjectTypeUserMapping:        FullSupport(),

			// Extensions and customization
			ObjectTypeExtension: FullSupport(),
			ObjectTypePlugin:    UnsupportedObject([]ObjectType{ObjectTypeExtension}, "Use extensions instead"),
		},
		ConversionCapabilities: ConversionCapabilities{
			CanBeSource:          true,
			CanBeTarget:          true,
			PreferredTargetTypes: []dbcapabilities.DatabaseType{dbcapabilities.MySQL, dbcapabilities.CockroachDB},
		},
	}

	// MySQL features
	DatabaseFeatureRegistry[dbcapabilities.MySQL] = DatabaseFeatureSupport{
		DatabaseType: dbcapabilities.MySQL,
		Paradigms:    []dbcapabilities.DataParadigm{dbcapabilities.ParadigmRelational},
		SupportedObjects: map[ObjectType]ObjectSupport{
			// Data container types
			ObjectTypeTable:            FullSupport(),
			ObjectTypeView:             FullSupport(),
			ObjectTypeMaterializedView: UnsupportedObject([]ObjectType{ObjectTypeView}, "Use views instead"),
			ObjectTypeTemporaryTable:   FullSupport(),
			ObjectTypeExternalTable:    UnsupportedObject([]ObjectType{ObjectTypeTable}, "Use regular tables"),
			ObjectTypeForeignTable:     UnsupportedObject([]ObjectType{ObjectTypeTable}, "Use regular tables"),
			ObjectTypeCollection:       UnsupportedObject([]ObjectType{ObjectTypeTable}, "Use tables instead"),
			ObjectTypeNode:             UnsupportedObject([]ObjectType{ObjectTypeTable}, "Use tables with foreign keys"),
			ObjectTypeVector:           UnsupportedObject(nil, "Vector operations not supported"),

			// Structural definition objects
			ObjectTypeColumn:   FullSupport(),
			ObjectTypeType:     PartialSupport([]string{"limited custom types"}, "Basic type support"),
			ObjectTypeSequence: UnsupportedObject([]ObjectType{ObjectTypeColumn}, "Use AUTO_INCREMENT columns"),

			// Integrity and performance objects
			ObjectTypeIndex:      FullSupport(),
			ObjectTypeConstraint: FullSupport(),

			// Executable code objects
			ObjectTypeFunction:  FullSupport(),
			ObjectTypeProcedure: FullSupport(),
			ObjectTypeTrigger:   FullSupport(),
			ObjectTypeAggregate: UnsupportedObject([]ObjectType{ObjectTypeFunction}, "Use functions instead"),
			ObjectTypeOperator:  UnsupportedObject(nil, "Custom operators not supported"),
			ObjectTypePackage:   UnsupportedObject([]ObjectType{ObjectTypeFunction}, "Use functions and databases"),
			ObjectTypeRule:      UnsupportedObject([]ObjectType{ObjectTypeTrigger}, "Use triggers instead"),

			// Security and access control
			ObjectTypeUser:   FullSupport(),
			ObjectTypeRole:   FullSupport(),
			ObjectTypeGrant:  FullSupport(),
			ObjectTypePolicy: UnsupportedObject([]ObjectType{ObjectTypeGrant}, "Use grants and views"),

			// Physical storage
			ObjectTypeTablespace: UnsupportedObject(nil, "Uses data directories"),
			ObjectTypeDatafile:   UnsupportedObject(nil, "Managed by MySQL"),

			// Connectivity and integration
			ObjectTypeServer:             UnsupportedObject(nil, "No federated server support"),
			ObjectTypeConnection:         UnsupportedObject(nil, "No persistent connections"),
			ObjectTypeForeignDataWrapper: UnsupportedObject(nil, "No FDW support"),
			ObjectTypeUserMapping:        UnsupportedObject(nil, "No user mapping"),

			// Extensions and customization
			ObjectTypeExtension: UnsupportedObject([]ObjectType{ObjectTypePlugin}, "Use plugins instead"),
			ObjectTypePlugin:    FullSupport(),
		},
		ConversionCapabilities: ConversionCapabilities{
			CanBeSource:          true,
			CanBeTarget:          true,
			PreferredTargetTypes: []dbcapabilities.DatabaseType{dbcapabilities.PostgreSQL, dbcapabilities.MariaDB},
		},
	}

	// MongoDB features
	DatabaseFeatureRegistry[dbcapabilities.MongoDB] = DatabaseFeatureSupport{
		DatabaseType: dbcapabilities.MongoDB,
		Paradigms:    []dbcapabilities.DataParadigm{dbcapabilities.ParadigmDocument},
		SupportedObjects: map[ObjectType]ObjectSupport{
			// Data container types
			ObjectTypeCollection:       FullSupport(),
			ObjectTypeDocument:         FullSupport(),
			ObjectTypeTable:            UnsupportedObject([]ObjectType{ObjectTypeCollection}, "Use collections instead"),
			ObjectTypeView:             PartialSupport([]string{"read-only views"}, "Views are read-only"),
			ObjectTypeMaterializedView: UnsupportedObject([]ObjectType{ObjectTypeView}, "Use views instead"),
			ObjectTypeTemporaryTable:   UnsupportedObject([]ObjectType{ObjectTypeCollection}, "Use collections with TTL"),
			ObjectTypeExternalTable:    UnsupportedObject([]ObjectType{ObjectTypeCollection}, "Use collections"),
			ObjectTypeForeignTable:     UnsupportedObject([]ObjectType{ObjectTypeCollection}, "Use collections"),
			ObjectTypeNode:             UnsupportedObject([]ObjectType{ObjectTypeDocument}, "Use documents with references"),
			ObjectTypeVector:           PartialSupport([]string{"requires Atlas Vector Search"}, "Vector search via Atlas"),

			// Structural definition objects
			ObjectTypeColumn:   UnsupportedObject([]ObjectType{ObjectTypeDocument}, "Schema-less documents"),
			ObjectTypeType:     UnsupportedObject([]ObjectType{ObjectTypeDocument}, "Dynamic typing"),
			ObjectTypeSequence: UnsupportedObject([]ObjectType{ObjectTypeDocument}, "Use ObjectId or counters"),

			// Integrity and performance objects
			ObjectTypeIndex:      FullSupport(),
			ObjectTypeConstraint: PartialSupport([]string{"schema validation"}, "Document validation rules"),

			// Executable code objects
			ObjectTypeFunction:  PartialSupport([]string{"JavaScript functions"}, "Server-side JavaScript"),
			ObjectTypeProcedure: UnsupportedObject([]ObjectType{ObjectTypeFunction}, "Use JavaScript functions"),
			ObjectTypeTrigger:   PartialSupport([]string{"change streams"}, "Database triggers via change streams"),
			ObjectTypeAggregate: FullSupport(),
			ObjectTypeOperator:  PartialSupport([]string{"aggregation operators"}, "Aggregation pipeline operators"),
			ObjectTypePackage:   UnsupportedObject(nil, "No package concept"),
			ObjectTypeRule:      UnsupportedObject([]ObjectType{ObjectTypeConstraint}, "Use validation rules"),

			// Security and access control
			ObjectTypeUser:   FullSupport(),
			ObjectTypeRole:   FullSupport(),
			ObjectTypeGrant:  FullSupport(),
			ObjectTypePolicy: UnsupportedObject([]ObjectType{ObjectTypeRole}, "Use role-based access"),

			// Physical storage
			ObjectTypeTablespace: UnsupportedObject(nil, "Managed by MongoDB"),
			ObjectTypeDatafile:   UnsupportedObject(nil, "Managed by MongoDB"),

			// Connectivity and integration
			ObjectTypeServer:             UnsupportedObject(nil, "No federated servers"),
			ObjectTypeConnection:         UnsupportedObject(nil, "Connection pooling handled by drivers"),
			ObjectTypeForeignDataWrapper: UnsupportedObject(nil, "No FDW concept"),
			ObjectTypeUserMapping:        UnsupportedObject(nil, "No user mapping"),

			// Extensions and customization
			ObjectTypeExtension: UnsupportedObject(nil, "No extension system"),
			ObjectTypePlugin:    UnsupportedObject(nil, "No plugin system"),
		},
		ConversionCapabilities: ConversionCapabilities{
			CanBeSource:           true,
			CanBeTarget:           true,
			PreferredSourceTypes:  []dbcapabilities.DatabaseType{dbcapabilities.PostgreSQL, dbcapabilities.MySQL},
			ConversionLimitations: []string{"Foreign key relationships become references or embedded documents"},
		},
	}

	// Neo4j features
	DatabaseFeatureRegistry[dbcapabilities.Neo4j] = DatabaseFeatureSupport{
		DatabaseType: dbcapabilities.Neo4j,
		Paradigms:    []dbcapabilities.DataParadigm{dbcapabilities.ParadigmGraph},
		SupportedObjects: map[ObjectType]ObjectSupport{
			ObjectTypeNode:         FullSupport(),
			ObjectTypeRelationship: FullSupport(),
			ObjectTypeGraph:        FullSupport(),
			ObjectTypeTable:        UnsupportedObject([]ObjectType{ObjectTypeNode}, "Use nodes instead"),
			ObjectTypeCollection:   UnsupportedObject([]ObjectType{ObjectTypeNode}, "Use nodes instead"),
			ObjectTypeVector:       PartialSupport([]string{"requires APOC or GDS"}, "Vector operations via plugins"),
		},
		ConversionCapabilities: ConversionCapabilities{
			CanBeSource:          true,
			CanBeTarget:          true,
			PreferredSourceTypes: []dbcapabilities.DatabaseType{dbcapabilities.PostgreSQL, dbcapabilities.MySQL},
			SpecialRequirements:  []string{"Requires enrichment data to identify entities and relationships"},
		},
	}

	// Elasticsearch features
	DatabaseFeatureRegistry[dbcapabilities.Elasticsearch] = DatabaseFeatureSupport{
		DatabaseType: dbcapabilities.Elasticsearch,
		Paradigms:    []dbcapabilities.DataParadigm{dbcapabilities.ParadigmSearchIndex},
		SupportedObjects: map[ObjectType]ObjectSupport{
			ObjectTypeSearchIndex: FullSupport(),
			ObjectTypeDocument:    FullSupport(),
			ObjectTypeTable:       UnsupportedObject([]ObjectType{ObjectTypeSearchIndex}, "Use indices instead"),
			ObjectTypeCollection:  UnsupportedObject([]ObjectType{ObjectTypeSearchIndex}, "Use indices instead"),
			ObjectTypeVector:      FullSupport().WithRequiredFields([]string{"dense_vector"}),
		},
		ConversionCapabilities: ConversionCapabilities{
			CanBeSource:           true,
			CanBeTarget:           true,
			PreferredSourceTypes:  []dbcapabilities.DatabaseType{dbcapabilities.MongoDB, dbcapabilities.PostgreSQL},
			ConversionLimitations: []string{"Optimized for search, not transactional operations"},
		},
	}

	// Milvus features (Vector Database)
	DatabaseFeatureRegistry[dbcapabilities.Milvus] = DatabaseFeatureSupport{
		DatabaseType: dbcapabilities.Milvus,
		Paradigms:    []dbcapabilities.DataParadigm{dbcapabilities.ParadigmVector},
		SupportedObjects: map[ObjectType]ObjectSupport{
			ObjectTypeVector:      FullSupport(),
			ObjectTypeVectorIndex: FullSupport(),
			ObjectTypeCollection:  PartialSupport([]string{"vector collections only"}, "Collections store vectors"),
			ObjectTypeTable:       UnsupportedObject([]ObjectType{ObjectTypeCollection}, "Use vector collections"),
			ObjectTypeDocument:    UnsupportedObject([]ObjectType{ObjectTypeVector}, "Use vectors with metadata"),
		},
		ConversionCapabilities: ConversionCapabilities{
			CanBeSource:          false, // Typically a target for vector data
			CanBeTarget:          true,
			PreferredSourceTypes: []dbcapabilities.DatabaseType{dbcapabilities.MongoDB, dbcapabilities.Elasticsearch},
			SpecialRequirements:  []string{"Requires vector embeddings to be generated from source data"},
		},
	}

	// Redis features
	DatabaseFeatureRegistry[dbcapabilities.Redis] = DatabaseFeatureSupport{
		DatabaseType: dbcapabilities.Redis,
		Paradigms:    []dbcapabilities.DataParadigm{dbcapabilities.ParadigmKeyValue},
		SupportedObjects: map[ObjectType]ObjectSupport{
			// Redis doesn't have traditional schema objects, but we can model its data structures
			ObjectTypeTable:      UnsupportedObject(nil, "Redis is key-value, not relational"),
			ObjectTypeCollection: UnsupportedObject(nil, "Redis is key-value, not document-based"),
			ObjectTypeVector:     PartialSupport([]string{"requires RedisSearch module"}, "Vector search via module"),
		},
		ConversionCapabilities: ConversionCapabilities{
			CanBeSource:           true,
			CanBeTarget:           true,
			ConversionLimitations: []string{"Limited schema conversion due to key-value nature"},
		},
	}

	// MariaDB features (MySQL-compatible)
	DatabaseFeatureRegistry[dbcapabilities.MariaDB] = DatabaseFeatureSupport{
		DatabaseType: dbcapabilities.MariaDB,
		Paradigms:    []dbcapabilities.DataParadigm{dbcapabilities.ParadigmRelational},
		SupportedObjects: map[ObjectType]ObjectSupport{
			ObjectTypeTable:            FullSupport(),
			ObjectTypeView:             FullSupport(),
			ObjectTypeMaterializedView: UnsupportedObject([]ObjectType{ObjectTypeView}, "Use views instead"),
			ObjectTypeCollection:       UnsupportedObject([]ObjectType{ObjectTypeTable}, "Use tables instead"),
			ObjectTypeNode:             UnsupportedObject([]ObjectType{ObjectTypeTable}, "Use tables with foreign keys"),
			ObjectTypeVector:           UnsupportedObject(nil, "Vector operations not supported"),
		},
		ConversionCapabilities: ConversionCapabilities{
			CanBeSource:          true,
			CanBeTarget:          true,
			PreferredTargetTypes: []dbcapabilities.DatabaseType{dbcapabilities.MySQL, dbcapabilities.PostgreSQL},
		},
	}

	// SQL Server features
	DatabaseFeatureRegistry[dbcapabilities.SQLServer] = DatabaseFeatureSupport{
		DatabaseType: dbcapabilities.SQLServer,
		Paradigms:    []dbcapabilities.DataParadigm{dbcapabilities.ParadigmRelational},
		SupportedObjects: map[ObjectType]ObjectSupport{
			ObjectTypeTable:            FullSupport(),
			ObjectTypeView:             FullSupport(),
			ObjectTypeMaterializedView: PartialSupport([]string{"indexed views only"}, "Materialized views via indexed views"),
			ObjectTypeCollection:       UnsupportedObject([]ObjectType{ObjectTypeTable}, "Use tables instead"),
			ObjectTypeNode:             UnsupportedObject([]ObjectType{ObjectTypeTable}, "Use tables with foreign keys"),
			ObjectTypeVector:           UnsupportedObject(nil, "Vector operations not natively supported"),
		},
		ConversionCapabilities: ConversionCapabilities{
			CanBeSource:          true,
			CanBeTarget:          true,
			PreferredTargetTypes: []dbcapabilities.DatabaseType{dbcapabilities.PostgreSQL, dbcapabilities.MySQL},
			SpecialRequirements:  []string{"May require schema name mapping"},
		},
	}

	// Oracle features
	DatabaseFeatureRegistry[dbcapabilities.Oracle] = DatabaseFeatureSupport{
		DatabaseType: dbcapabilities.Oracle,
		Paradigms:    []dbcapabilities.DataParadigm{dbcapabilities.ParadigmRelational},
		SupportedObjects: map[ObjectType]ObjectSupport{
			ObjectTypeTable:            FullSupport(),
			ObjectTypeView:             FullSupport(),
			ObjectTypeMaterializedView: FullSupport(),
			ObjectTypeCollection:       UnsupportedObject([]ObjectType{ObjectTypeTable}, "Use tables instead"),
			ObjectTypeNode:             UnsupportedObject([]ObjectType{ObjectTypeTable}, "Use tables with foreign keys"),
			ObjectTypeVector:           PartialSupport([]string{"Oracle AI Vector Search"}, "Vector support in 23c+"),
		},
		ConversionCapabilities: ConversionCapabilities{
			CanBeSource:           true,
			CanBeTarget:           true,
			PreferredTargetTypes:  []dbcapabilities.DatabaseType{dbcapabilities.PostgreSQL, dbcapabilities.SQLServer},
			ConversionLimitations: []string{"Complex PL/SQL may not convert directly", "Oracle-specific features may be lost"},
		},
	}

	// TiDB features (MySQL-compatible distributed)
	DatabaseFeatureRegistry[dbcapabilities.TiDB] = DatabaseFeatureSupport{
		DatabaseType: dbcapabilities.TiDB,
		Paradigms:    []dbcapabilities.DataParadigm{dbcapabilities.ParadigmRelational},
		SupportedObjects: map[ObjectType]ObjectSupport{
			ObjectTypeTable:            FullSupport(),
			ObjectTypeView:             FullSupport(),
			ObjectTypeMaterializedView: UnsupportedObject([]ObjectType{ObjectTypeView}, "Use views instead"),
			ObjectTypeCollection:       UnsupportedObject([]ObjectType{ObjectTypeTable}, "Use tables instead"),
			ObjectTypeNode:             UnsupportedObject([]ObjectType{ObjectTypeTable}, "Use tables with foreign keys"),
			ObjectTypeVector:           PartialSupport([]string{"TiDB Vector (beta)"}, "Vector support in development"),
		},
		ConversionCapabilities: ConversionCapabilities{
			CanBeSource:          true,
			CanBeTarget:          true,
			PreferredTargetTypes: []dbcapabilities.DatabaseType{dbcapabilities.MySQL, dbcapabilities.PostgreSQL},
		},
	}

	// ClickHouse features (Columnar analytics)
	DatabaseFeatureRegistry[dbcapabilities.ClickHouse] = DatabaseFeatureSupport{
		DatabaseType: dbcapabilities.ClickHouse,
		Paradigms:    []dbcapabilities.DataParadigm{dbcapabilities.ParadigmColumnar},
		SupportedObjects: map[ObjectType]ObjectSupport{
			ObjectTypeTable:            FullSupport(),
			ObjectTypeView:             FullSupport(),
			ObjectTypeMaterializedView: FullSupport(),
			ObjectTypeCollection:       UnsupportedObject([]ObjectType{ObjectTypeTable}, "Use tables instead"),
			ObjectTypeNode:             UnsupportedObject([]ObjectType{ObjectTypeTable}, "Use tables with foreign keys"),
			ObjectTypeVector:           PartialSupport([]string{"vector similarity functions"}, "Vector operations via functions"),
		},
		ConversionCapabilities: ConversionCapabilities{
			CanBeSource:           true,
			CanBeTarget:           true,
			PreferredSourceTypes:  []dbcapabilities.DatabaseType{dbcapabilities.PostgreSQL, dbcapabilities.MySQL},
			ConversionLimitations: []string{"Optimized for analytics, not OLTP", "Limited UPDATE/DELETE support"},
		},
	}

	// DB2 features
	DatabaseFeatureRegistry[dbcapabilities.DB2] = DatabaseFeatureSupport{
		DatabaseType: dbcapabilities.DB2,
		Paradigms:    []dbcapabilities.DataParadigm{dbcapabilities.ParadigmRelational},
		SupportedObjects: map[ObjectType]ObjectSupport{
			ObjectTypeTable:            FullSupport(),
			ObjectTypeView:             FullSupport(),
			ObjectTypeMaterializedView: FullSupport(),
			ObjectTypeCollection:       UnsupportedObject([]ObjectType{ObjectTypeTable}, "Use tables instead"),
			ObjectTypeNode:             UnsupportedObject([]ObjectType{ObjectTypeTable}, "Use tables with foreign keys"),
			ObjectTypeVector:           UnsupportedObject(nil, "Vector operations not supported"),
		},
		ConversionCapabilities: ConversionCapabilities{
			CanBeSource:           true,
			CanBeTarget:           true,
			PreferredTargetTypes:  []dbcapabilities.DatabaseType{dbcapabilities.PostgreSQL, dbcapabilities.Oracle},
			ConversionLimitations: []string{"DB2-specific features may not convert", "Complex stored procedures may need rewriting"},
		},
	}

	// CockroachDB features (Distributed SQL)
	DatabaseFeatureRegistry[dbcapabilities.CockroachDB] = DatabaseFeatureSupport{
		DatabaseType: dbcapabilities.CockroachDB,
		Paradigms:    []dbcapabilities.DataParadigm{dbcapabilities.ParadigmRelational},
		SupportedObjects: map[ObjectType]ObjectSupport{
			ObjectTypeTable:            FullSupport(),
			ObjectTypeView:             FullSupport(),
			ObjectTypeMaterializedView: UnsupportedObject([]ObjectType{ObjectTypeView}, "Use views instead"),
			ObjectTypeCollection:       UnsupportedObject([]ObjectType{ObjectTypeTable}, "Use tables instead"),
			ObjectTypeNode:             UnsupportedObject([]ObjectType{ObjectTypeTable}, "Use tables with foreign keys"),
			ObjectTypeVector:           PartialSupport([]string{"vector similarity"}, "Vector operations via extensions"),
		},
		ConversionCapabilities: ConversionCapabilities{
			CanBeSource:          true,
			CanBeTarget:          true,
			PreferredTargetTypes: []dbcapabilities.DatabaseType{dbcapabilities.PostgreSQL, dbcapabilities.MySQL},
		},
	}

	// DuckDB features (Embedded analytics)
	DatabaseFeatureRegistry[dbcapabilities.DuckDB] = DatabaseFeatureSupport{
		DatabaseType: dbcapabilities.DuckDB,
		Paradigms:    []dbcapabilities.DataParadigm{dbcapabilities.ParadigmColumnar},
		SupportedObjects: map[ObjectType]ObjectSupport{
			ObjectTypeTable:            FullSupport(),
			ObjectTypeView:             FullSupport(),
			ObjectTypeMaterializedView: UnsupportedObject([]ObjectType{ObjectTypeView}, "Use views instead"),
			ObjectTypeCollection:       UnsupportedObject([]ObjectType{ObjectTypeTable}, "Use tables instead"),
			ObjectTypeNode:             UnsupportedObject([]ObjectType{ObjectTypeTable}, "Use tables with foreign keys"),
			ObjectTypeVector:           PartialSupport([]string{"array operations"}, "Vector operations via arrays"),
		},
		ConversionCapabilities: ConversionCapabilities{
			CanBeSource:          true,
			CanBeTarget:          true,
			PreferredSourceTypes: []dbcapabilities.DatabaseType{dbcapabilities.PostgreSQL, dbcapabilities.ClickHouse},
		},
	}

	// EdgeDB features (Next-gen relational)
	DatabaseFeatureRegistry[dbcapabilities.EdgeDB] = DatabaseFeatureSupport{
		DatabaseType: dbcapabilities.EdgeDB,
		Paradigms:    []dbcapabilities.DataParadigm{dbcapabilities.ParadigmRelational, dbcapabilities.ParadigmGraph},
		SupportedObjects: map[ObjectType]ObjectSupport{
			ObjectTypeTable:            PartialSupport([]string{"object types instead of tables"}, "Uses object types"),
			ObjectTypeView:             UnsupportedObject([]ObjectType{ObjectTypeTable}, "Use computed properties"),
			ObjectTypeMaterializedView: UnsupportedObject([]ObjectType{ObjectTypeTable}, "Use computed properties"),
			ObjectTypeCollection:       UnsupportedObject([]ObjectType{ObjectTypeTable}, "Use object types"),
			ObjectTypeNode:             FullSupport(),
			ObjectTypeVector:           UnsupportedObject(nil, "Vector operations not supported"),
		},
		ConversionCapabilities: ConversionCapabilities{
			CanBeSource:          true,
			CanBeTarget:          true,
			PreferredSourceTypes: []dbcapabilities.DatabaseType{dbcapabilities.PostgreSQL, dbcapabilities.Neo4j},
			SpecialRequirements:  []string{"Requires schema redesign to EdgeDB object model"},
		},
	}

	// Cassandra features (Wide-column store)
	DatabaseFeatureRegistry[dbcapabilities.Cassandra] = DatabaseFeatureSupport{
		DatabaseType: dbcapabilities.Cassandra,
		Paradigms:    []dbcapabilities.DataParadigm{dbcapabilities.ParadigmWideColumn},
		SupportedObjects: map[ObjectType]ObjectSupport{
			ObjectTypeTable:            FullSupport(),
			ObjectTypeView:             PartialSupport([]string{"materialized views only"}, "Only materialized views supported"),
			ObjectTypeMaterializedView: FullSupport(),
			ObjectTypeCollection:       UnsupportedObject([]ObjectType{ObjectTypeTable}, "Use tables (column families)"),
			ObjectTypeNode:             UnsupportedObject([]ObjectType{ObjectTypeTable}, "Use tables with partition keys"),
			ObjectTypeVector:           PartialSupport([]string{"vector similarity search"}, "Vector search via plugins"),
		},
		ConversionCapabilities: ConversionCapabilities{
			CanBeSource:           true,
			CanBeTarget:           true,
			PreferredSourceTypes:  []dbcapabilities.DatabaseType{dbcapabilities.PostgreSQL, dbcapabilities.MongoDB},
			ConversionLimitations: []string{"Requires denormalization", "Limited JOIN support", "Partition key design critical"},
		},
	}

	// DynamoDB features (AWS key-value/document)
	DatabaseFeatureRegistry[dbcapabilities.DynamoDB] = DatabaseFeatureSupport{
		DatabaseType: dbcapabilities.DynamoDB,
		Paradigms:    []dbcapabilities.DataParadigm{dbcapabilities.ParadigmKeyValue, dbcapabilities.ParadigmDocument},
		SupportedObjects: map[ObjectType]ObjectSupport{
			ObjectTypeTable:      PartialSupport([]string{"NoSQL tables only"}, "Tables without fixed schema"),
			ObjectTypeCollection: PartialSupport([]string{"items in tables"}, "Tables store items (documents)"),
			ObjectTypeDocument:   FullSupport(),
			ObjectTypeView:       UnsupportedObject(nil, "No views in DynamoDB"),
			ObjectTypeNode:       UnsupportedObject([]ObjectType{ObjectTypeTable}, "Use tables with partition keys"),
			ObjectTypeVector:     UnsupportedObject(nil, "Vector operations not supported"),
		},
		ConversionCapabilities: ConversionCapabilities{
			CanBeSource:           true,
			CanBeTarget:           true,
			PreferredSourceTypes:  []dbcapabilities.DatabaseType{dbcapabilities.MongoDB, dbcapabilities.PostgreSQL},
			ConversionLimitations: []string{"No complex queries", "Single-table design preferred", "Partition key design critical"},
		},
	}

	// CosmosDB features (Multi-model database)
	DatabaseFeatureRegistry[dbcapabilities.CosmosDB] = DatabaseFeatureSupport{
		DatabaseType: dbcapabilities.CosmosDB,
		Paradigms:    []dbcapabilities.DataParadigm{dbcapabilities.ParadigmDocument, dbcapabilities.ParadigmGraph, dbcapabilities.ParadigmKeyValue},
		SupportedObjects: map[ObjectType]ObjectSupport{
			ObjectTypeCollection:   FullSupport(),
			ObjectTypeDocument:     FullSupport(),
			ObjectTypeNode:         FullSupport(),
			ObjectTypeRelationship: FullSupport(),
			ObjectTypeTable:        UnsupportedObject([]ObjectType{ObjectTypeCollection}, "Use collections instead"),
			ObjectTypeView:         UnsupportedObject(nil, "No views in CosmosDB"),
			ObjectTypeVector:       PartialSupport([]string{"vector search preview"}, "Vector search in preview"),
		},
		ConversionCapabilities: ConversionCapabilities{
			CanBeSource:          true,
			CanBeTarget:          true,
			PreferredSourceTypes: []dbcapabilities.DatabaseType{dbcapabilities.MongoDB, dbcapabilities.Neo4j, dbcapabilities.PostgreSQL},
			SpecialRequirements:  []string{"Choose appropriate API (SQL, MongoDB, Gremlin, Table)"},
		},
	}

	// Snowflake features (Cloud data warehouse)
	DatabaseFeatureRegistry[dbcapabilities.Snowflake] = DatabaseFeatureSupport{
		DatabaseType: dbcapabilities.Snowflake,
		Paradigms:    []dbcapabilities.DataParadigm{dbcapabilities.ParadigmColumnar},
		SupportedObjects: map[ObjectType]ObjectSupport{
			ObjectTypeTable:            FullSupport(),
			ObjectTypeView:             FullSupport(),
			ObjectTypeMaterializedView: FullSupport(),
			ObjectTypeCollection:       UnsupportedObject([]ObjectType{ObjectTypeTable}, "Use tables instead"),
			ObjectTypeNode:             UnsupportedObject([]ObjectType{ObjectTypeTable}, "Use tables with foreign keys"),
			ObjectTypeVector:           PartialSupport([]string{"vector functions"}, "Vector operations via functions"),
		},
		ConversionCapabilities: ConversionCapabilities{
			CanBeSource:           true,
			CanBeTarget:           true,
			PreferredSourceTypes:  []dbcapabilities.DatabaseType{dbcapabilities.PostgreSQL, dbcapabilities.ClickHouse},
			ConversionLimitations: []string{"Optimized for analytics workloads", "May require data modeling changes"},
		},
	}

	// Weaviate features (Vector search engine)
	DatabaseFeatureRegistry[dbcapabilities.Weaviate] = DatabaseFeatureSupport{
		DatabaseType: dbcapabilities.Weaviate,
		Paradigms:    []dbcapabilities.DataParadigm{dbcapabilities.ParadigmVector},
		SupportedObjects: map[ObjectType]ObjectSupport{
			ObjectTypeVector:      FullSupport(),
			ObjectTypeVectorIndex: FullSupport(),
			ObjectTypeCollection:  PartialSupport([]string{"classes for vector collections"}, "Classes store vector objects"),
			ObjectTypeDocument:    PartialSupport([]string{"objects with vectors"}, "Objects with vector embeddings"),
			ObjectTypeTable:       UnsupportedObject([]ObjectType{ObjectTypeCollection}, "Use classes instead"),
			ObjectTypeNode:        UnsupportedObject([]ObjectType{ObjectTypeDocument}, "Use objects instead"),
		},
		ConversionCapabilities: ConversionCapabilities{
			CanBeSource:          false, // Typically a target for vector data
			CanBeTarget:          true,
			PreferredSourceTypes: []dbcapabilities.DatabaseType{dbcapabilities.MongoDB, dbcapabilities.Elasticsearch, dbcapabilities.PostgreSQL},
			SpecialRequirements:  []string{"Requires vector embeddings", "Schema definition required"},
		},
	}

	// Pinecone features (Managed vector database)
	DatabaseFeatureRegistry[dbcapabilities.Pinecone] = DatabaseFeatureSupport{
		DatabaseType: dbcapabilities.Pinecone,
		Paradigms:    []dbcapabilities.DataParadigm{dbcapabilities.ParadigmVector},
		SupportedObjects: map[ObjectType]ObjectSupport{
			ObjectTypeVector:      FullSupport(),
			ObjectTypeVectorIndex: FullSupport(),
			ObjectTypeCollection:  UnsupportedObject([]ObjectType{ObjectTypeVectorIndex}, "Use indexes instead"),
			ObjectTypeDocument:    UnsupportedObject([]ObjectType{ObjectTypeVector}, "Use vectors with metadata"),
			ObjectTypeTable:       UnsupportedObject([]ObjectType{ObjectTypeVectorIndex}, "Use indexes instead"),
			ObjectTypeNode:        UnsupportedObject([]ObjectType{ObjectTypeVector}, "Use vectors instead"),
		},
		ConversionCapabilities: ConversionCapabilities{
			CanBeSource:          false, // Typically a target for vector data
			CanBeTarget:          true,
			PreferredSourceTypes: []dbcapabilities.DatabaseType{dbcapabilities.MongoDB, dbcapabilities.Elasticsearch},
			SpecialRequirements:  []string{"Requires vector embeddings", "Fixed vector dimensions"},
		},
	}

	// Chroma features (AI-native vector database)
	DatabaseFeatureRegistry[dbcapabilities.Chroma] = DatabaseFeatureSupport{
		DatabaseType: dbcapabilities.Chroma,
		Paradigms:    []dbcapabilities.DataParadigm{dbcapabilities.ParadigmVector},
		SupportedObjects: map[ObjectType]ObjectSupport{
			ObjectTypeVector:      FullSupport(),
			ObjectTypeVectorIndex: FullSupport(),
			ObjectTypeCollection:  FullSupport(),
			ObjectTypeDocument:    PartialSupport([]string{"documents with embeddings"}, "Documents with vector embeddings"),
			ObjectTypeTable:       UnsupportedObject([]ObjectType{ObjectTypeCollection}, "Use collections instead"),
			ObjectTypeNode:        UnsupportedObject([]ObjectType{ObjectTypeDocument}, "Use documents instead"),
		},
		ConversionCapabilities: ConversionCapabilities{
			CanBeSource:          true,
			CanBeTarget:          true,
			PreferredSourceTypes: []dbcapabilities.DatabaseType{dbcapabilities.MongoDB, dbcapabilities.Elasticsearch},
			SpecialRequirements:  []string{"Automatic embedding generation available"},
		},
	}

	// LanceDB features (Vector database)
	DatabaseFeatureRegistry[dbcapabilities.LanceDB] = DatabaseFeatureSupport{
		DatabaseType: dbcapabilities.LanceDB,
		Paradigms:    []dbcapabilities.DataParadigm{dbcapabilities.ParadigmVector},
		SupportedObjects: map[ObjectType]ObjectSupport{
			ObjectTypeVector:      FullSupport(),
			ObjectTypeVectorIndex: FullSupport(),
			ObjectTypeTable:       PartialSupport([]string{"tables store vectors"}, "Tables with vector columns"),
			ObjectTypeCollection:  UnsupportedObject([]ObjectType{ObjectTypeTable}, "Use tables instead"),
			ObjectTypeDocument:    UnsupportedObject([]ObjectType{ObjectTypeVector}, "Use vectors with metadata"),
			ObjectTypeNode:        UnsupportedObject([]ObjectType{ObjectTypeVector}, "Use vectors instead"),
		},
		ConversionCapabilities: ConversionCapabilities{
			CanBeSource:          true,
			CanBeTarget:          true,
			PreferredSourceTypes: []dbcapabilities.DatabaseType{dbcapabilities.PostgreSQL, dbcapabilities.MongoDB},
			SpecialRequirements:  []string{"Columnar storage format", "Supports SQL queries"},
		},
	}

	// S3 features (Amazon object storage)
	DatabaseFeatureRegistry[dbcapabilities.S3] = DatabaseFeatureSupport{
		DatabaseType: dbcapabilities.S3,
		Paradigms:    []dbcapabilities.DataParadigm{dbcapabilities.ParadigmObjectStore},
		SupportedObjects: map[ObjectType]ObjectSupport{
			ObjectTypeTable:      UnsupportedObject(nil, "Object storage, not relational"),
			ObjectTypeCollection: UnsupportedObject(nil, "Object storage, not document-based"),
			ObjectTypeDocument:   UnsupportedObject(nil, "Objects are files, not documents"),
			ObjectTypeNode:       UnsupportedObject(nil, "Object storage, not graph-based"),
			ObjectTypeVector:     UnsupportedObject(nil, "Object storage, not vector-based"),
		},
		ConversionCapabilities: ConversionCapabilities{
			CanBeSource:           true,
			CanBeTarget:           true,
			ConversionLimitations: []string{"File-based storage only", "No schema enforcement", "No querying capabilities"},
		},
	}

	// GCS features (Google Cloud Storage)
	DatabaseFeatureRegistry[dbcapabilities.GCS] = DatabaseFeatureSupport{
		DatabaseType: dbcapabilities.GCS,
		Paradigms:    []dbcapabilities.DataParadigm{dbcapabilities.ParadigmObjectStore},
		SupportedObjects: map[ObjectType]ObjectSupport{
			ObjectTypeTable:      UnsupportedObject(nil, "Object storage, not relational"),
			ObjectTypeCollection: UnsupportedObject(nil, "Object storage, not document-based"),
			ObjectTypeDocument:   UnsupportedObject(nil, "Objects are files, not documents"),
			ObjectTypeNode:       UnsupportedObject(nil, "Object storage, not graph-based"),
			ObjectTypeVector:     UnsupportedObject(nil, "Object storage, not vector-based"),
		},
		ConversionCapabilities: ConversionCapabilities{
			CanBeSource:           true,
			CanBeTarget:           true,
			ConversionLimitations: []string{"File-based storage only", "No schema enforcement", "No querying capabilities"},
		},
	}

	// AzureBlob features (Azure Blob Storage)
	DatabaseFeatureRegistry[dbcapabilities.AzureBlob] = DatabaseFeatureSupport{
		DatabaseType: dbcapabilities.AzureBlob,
		Paradigms:    []dbcapabilities.DataParadigm{dbcapabilities.ParadigmObjectStore},
		SupportedObjects: map[ObjectType]ObjectSupport{
			ObjectTypeTable:      UnsupportedObject(nil, "Object storage, not relational"),
			ObjectTypeCollection: UnsupportedObject(nil, "Object storage, not document-based"),
			ObjectTypeDocument:   UnsupportedObject(nil, "Objects are files, not documents"),
			ObjectTypeNode:       UnsupportedObject(nil, "Object storage, not graph-based"),
			ObjectTypeVector:     UnsupportedObject(nil, "Object storage, not vector-based"),
		},
		ConversionCapabilities: ConversionCapabilities{
			CanBeSource:           true,
			CanBeTarget:           true,
			ConversionLimitations: []string{"File-based storage only", "No schema enforcement", "No querying capabilities"},
		},
	}

	// MinIO features (S3-compatible storage)
	DatabaseFeatureRegistry[dbcapabilities.MinIO] = DatabaseFeatureSupport{
		DatabaseType: dbcapabilities.MinIO,
		Paradigms:    []dbcapabilities.DataParadigm{dbcapabilities.ParadigmObjectStore},
		SupportedObjects: map[ObjectType]ObjectSupport{
			ObjectTypeTable:      UnsupportedObject(nil, "Object storage, not relational"),
			ObjectTypeCollection: UnsupportedObject(nil, "Object storage, not document-based"),
			ObjectTypeDocument:   UnsupportedObject(nil, "Objects are files, not documents"),
			ObjectTypeNode:       UnsupportedObject(nil, "Object storage, not graph-based"),
			ObjectTypeVector:     UnsupportedObject(nil, "Object storage, not vector-based"),
		},
		ConversionCapabilities: ConversionCapabilities{
			CanBeSource:           true,
			CanBeTarget:           true,
			ConversionLimitations: []string{"File-based storage only", "No schema enforcement", "S3-compatible API"},
		},
	}

}
