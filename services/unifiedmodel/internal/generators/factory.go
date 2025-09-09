// internal/generators/factory.go
package generators

type GeneratorFactory struct {
	generators map[string]StatementGenerator
}

func NewGeneratorFactory() *GeneratorFactory {
	factory := &GeneratorFactory{
		generators: make(map[string]StatementGenerator),
	}

	// Register refactored generators (using shared UnifiedModel types)
	factory.RegisterGenerator("postgres", &PostgresGenerator{})
	factory.RegisterGenerator("mysql", &MySQLGenerator{})
	factory.RegisterGenerator("mongodb", &MongoDBGenerator{})
	factory.RegisterGenerator("mariadb", &MariaDBGenerator{})

	// TODO: The following generators need to be refactored to use pkg/unifiedmodel types
	// They are temporarily disabled until refactoring is complete
	// factory.RegisterGenerator("oracle", &OracleGenerator{})
	// factory.RegisterGenerator("db2", &Db2Generator{})
	// factory.RegisterGenerator("mssql", &MSSQLGenerator{})
	factory.RegisterGenerator("cassandra", &CassandraGenerator{})
	// factory.RegisterGenerator("cockroach", &CockroachGenerator{})
	// factory.RegisterGenerator("clickhouse", &ClickhouseGenerator{})
	// factory.RegisterGenerator("elasticsearch", &ElasticsearchGenerator{})
	factory.RegisterGenerator("edgedb", &EdgeDBGenerator{})
	factory.RegisterGenerator("neo4j", &Neo4jGenerator{})
	// factory.RegisterGenerator("pinecone", &PineconeGenerator{})
	// factory.RegisterGenerator("snowflake", &SnowflakeGenerator{})
	// factory.RegisterGenerator("redis", &RedisGenerator{})

	return factory
}

func (f *GeneratorFactory) RegisterGenerator(dbType string, generator StatementGenerator) {
	f.generators[dbType] = generator
}

func (f *GeneratorFactory) GetGenerator(dbType string) (StatementGenerator, bool) {
	generator, ok := f.generators[dbType]
	return generator, ok
}
