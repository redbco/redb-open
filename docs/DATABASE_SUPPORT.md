## Database Support

The Anchor service provides adapters across multiple data paradigms. Support evolves quickly; use this document as a guide and check the codebase for the latest adapters.

### Paradigms and Examples

- RELATIONAL: PostgreSQL, MySQL, SQL Server, Oracle, MariaDB, Db2, CockroachDB, Snowflake, DuckDB
- DOCUMENT: MongoDB, Azure CosmosDB
- GRAPH: Neo4j, EdgeDB
- VECTOR: Chroma, Milvus (incl. Zilliz), Pinecone, LanceDB, Weaviate
- COLUMNAR: ClickHouse, Cassandra
- KEY_VALUE: Redis
- SEARCH: Elasticsearch
- WIDE_COLUMN: Amazon DynamoDB
- OBJECT_STORAGE: Amazon S3, Google Cloud Storage, Azure Blob, MinIO

Notes
- Coverage also extends via the Unified Model conversion layer; see `pkg/unifiedmodel/`.
- Exact counts change as adapters are added/refined. Treat lists above as representative.

### Adding a New Database Adapter

- Start with `docs/ADDING_NEW_DATABASE_SUPPORT.md`
- Then see `docs/DATABASE_ADAPTER_IMPLEMENTATION_GUIDE.md`
- Validate extraction with the Unified Model types and comparison utilities


