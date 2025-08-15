# UnifiedModel Package

The `unifiedmodel` package provides a comprehensive, technology-agnostic schema representation for all supported database technologies. This package serves as the foundation for schema discovery, comparison, conversion, and analytics across the entire redb ecosystem.

## Overview

The UnifiedModel package enables seamless database schema management across 20+ database technologies spanning multiple paradigms:

- **Relational**: PostgreSQL, MySQL, Oracle, SQL Server, MariaDB, TiDB, ClickHouse, DB2, CockroachDB, DuckDB, EdgeDB
- **Document**: MongoDB, CosmosDB  
- **Key-Value**: DynamoDB, Redis
- **Graph**: Neo4j, EdgeDB, CosmosDB
- **Vector**: Milvus, Weaviate, Pinecone, Chroma, LanceDB
- **Search**: Elasticsearch
- **Analytics**: Snowflake, BigQuery, Redshift
- **Wide-Column**: Cassandra
- **Time-Series**: InfluxDB, TimescaleDB
- **Object Storage**: S3, GCS, Azure Blob, MinIO

## Core Components

### 1. UnifiedModel - Schema Structure

The central `UnifiedModel` struct represents database schemas with 165+ object types:

```go
type UnifiedModel struct {
    DatabaseType string `json:"database_type"`
    
    // Primary data containers
    Tables       map[string]Table       `json:"tables"`
    Collections  map[string]Collection  `json:"collections"`
    Nodes        map[string]Node        `json:"nodes"`
    Views        map[string]View        `json:"views"`
    
    // Performance objects
    Indexes      map[string]Index       `json:"indexes"`
    Constraints  map[string]Constraint  `json:"constraints"`
    
    // Executable code
    Functions    map[string]Function    `json:"functions"`
    Procedures   map[string]Procedure   `json:"procedures"`
    
    // Security
    Users        map[string]DBUser      `json:"users"`
    Roles        map[string]DBRole      `json:"roles"`
    
    // ... 165+ total object types
}
```

### 2. UnifiedModelSampleData - Privileged Data Detection

Sample data collection for sophisticated privileged data detection:

```go
type UnifiedModelSampleData struct {
    SchemaID      string    `json:"schema_id"`
    CollectedAt   time.Time `json:"collected_at"`
    SampleConfig  SampleDataConfig `json:"sample_config"`
    
    // Multi-paradigm sample data
    TableSamples      map[string]TableSampleData      `json:"table_samples"`
    CollectionSamples map[string]CollectionSampleData `json:"collection_samples"`
    KeyValueSamples   map[string]KeyValueSampleData   `json:"key_value_samples"`
    GraphSamples      map[string]GraphSampleData      `json:"graph_samples"`
    VectorSamples     map[string]VectorSampleData     `json:"vector_samples"`
    // ... supports all database paradigms
    
    // Privacy controls
    ContainsPII      bool     `json:"contains_pii"`
    RedactionApplied bool     `json:"redaction_applied"`
}
```

**Key Features:**
- 🚫 **Never Persisted**: Sample data is used only for real-time analysis
- 🔒 **Privacy-First**: Built-in redaction and sensitivity controls
- 🌐 **Multi-Paradigm**: Specialized structures for each database type
- ⚙️ **Configurable**: Flexible sampling strategies and limits
- 🔍 **PII Detection**: Automatic pattern recognition and classification

### 3. UnifiedModelMetrics - Analytics & Metrics

Comprehensive analytics separate from structural schema:

```go
type UnifiedModelMetrics struct {
    SchemaID        string    `json:"schema_id"`
    MetricsVersion  string    `json:"metrics_version"`
    GeneratedAt     time.Time `json:"generated_at"`
    
    ObjectCounts       ObjectCounts       `json:"object_counts"`
    SizeMetrics        SizeMetrics        `json:"size_metrics"`
    RowMetrics         RowMetrics         `json:"row_metrics"`
    PerformanceMetrics PerformanceMetrics `json:"performance_metrics"`
    TrendMetrics       TrendMetrics       `json:"trend_metrics"`
    CapacityMetrics    CapacityMetrics    `json:"capacity_metrics"`
    QualityMetrics     QualityMetrics     `json:"quality_metrics"`
}
```

### 3. UnifiedModelEnrichment - Analysis Metadata

AI-derived insights and classification data:

```go
type UnifiedModelEnrichment struct {
    SchemaID          string    `json:"schema_id"`
    EnrichmentVersion string    `json:"enrichment_version"`
    GeneratedAt       time.Time `json:"generated_at"`
    
    TableEnrichments  map[string]TableEnrichment  `json:"table_enrichments"`
    ColumnEnrichments map[string]ColumnEnrichment `json:"column_enrichments"`
    // ... enrichment for all object types
    
    ComplianceSummary ComplianceSummary `json:"compliance_summary"`
    RiskAssessment    RiskAssessment    `json:"risk_assessment"`
    Recommendations   []Recommendation  `json:"recommendations"`
}
```

### 4. Comparison & Context

Schema comparison and conversion context:

```go
type ComparisonResult struct {
    SourceSchema         string             `json:"source_schema"`
    TargetSchema         string             `json:"target_schema"`
    HasStructuralChanges bool               `json:"has_structural_changes"`
    StructuralChanges    []StructuralChange `json:"structural_changes"`
    OverallSimilarity    float64            `json:"overall_similarity"`
    MigrationComplexity  MigrationComplexity `json:"migration_complexity"`
}
```

## Key Features

### ✅ **Complete Database Coverage**
- **165+ Object Types**: Covers all database objects across all paradigms
- **Universal Representation**: Single schema format for all database technologies
- **Paradigm Support**: Relational, Document, Graph, Vector, Search, Analytics, Time-Series

### ✅ **Three-Level Privileged Data Detection**
- **Schema-Only**: Fast detection using column names, types, and constraints (30-80% confidence)
- **Enriched**: Enhanced detection using business context and classifications (40-90% confidence)
- **Full**: Comprehensive detection with sample data analysis (60-99% confidence)

### ✅ **Separation of Concerns**
- **Structure**: Pure schema definition in `UnifiedModel`
- **Analytics**: Metrics and performance data in `UnifiedModelMetrics`
- **Intelligence**: AI insights and classification in `UnifiedModelEnrichment`
- **Context**: Comparison and conversion guidance in context types

### ✅ **Advanced Analytics**
- **500+ Metrics**: Comprehensive object counts, sizes, performance, trends
- **Growth Analysis**: Trend analysis and capacity planning
- **Quality Assessment**: Data quality scores and issue detection
- **Performance Tracking**: Query metrics, throughput, resource utilization

### ✅ **Schema Operations**
- **Comparison**: Detailed diff analysis with similarity scoring
- **Validation**: Schema consistency and completeness checks
- **Serialization**: JSON marshaling/unmarshaling for storage
- **Cloning/Merging**: Schema manipulation and combination

## Usage Examples

### Basic Schema Creation

```go
import "github.com/redbco/redb-open/pkg/unifiedmodel"

// Create a new schema
schema := &unifiedmodel.UnifiedModel{
    DatabaseType: "postgres",
    Tables: map[string]unifiedmodel.Table{
        "users": {
            Name: "users",
            Columns: map[string]unifiedmodel.Column{
                "id": {
                    Name:         "id",
                    DataType:     "bigserial",
                    IsPrimaryKey: true,
                },
                "email": {
                    Name:     "email",
                    DataType: "varchar(320)",
                    Nullable: false,
                },
            },
        },
    },
    Indexes: map[string]unifiedmodel.Index{
        "idx_users_email": {
            Name:    "idx_users_email",
            Columns: []string{"email"},
            Unique:  true,
        },
    },
}
```

### Schema Operations

```go
// Generate schema ID
schemaID := unifiedmodel.GenerateSchemaID(schema)

// Validate schema
errors := unifiedmodel.ValidateSchema(schema)
if len(errors) > 0 {
    for _, err := range errors {
        fmt.Printf("Validation error: %s\n", err.Message)
    }
}

// Generate metrics
metrics := schema.GetBasicMetrics(schemaID)
fmt.Printf("Total objects: %d\n", metrics.ObjectCounts.GetTotalObjectCount())

// Serialize for storage
jsonBytes, err := unifiedmodel.SerializeSchema(schema)
if err != nil {
    log.Fatal(err)
}

// Deserialize from storage
loadedSchema, err := unifiedmodel.DeserializeSchema(jsonBytes)
if err != nil {
    log.Fatal(err)
}
```

### Schema Comparison

```go
// Basic comparison with production-ready defaults
options := unifiedmodel.DefaultComparisonOptions()
result, err := unifiedmodel.CompareSchemas(oldSchema, newSchema, options)
if err != nil {
    log.Fatal(err)
}

// Fast comparison for CI/CD pipelines
fastOptions := unifiedmodel.FastComparisonOptions()
result, err := unifiedmodel.CompareSchemas(oldSchema, newSchema, fastOptions)

// Enhanced comparison with enrichment context
enrichedOptions := unifiedmodel.EnrichedComparisonOptions()
result, err := unifiedmodel.CompareSchemas(oldSchema, newSchema, enrichedOptions)

// Check for significant changes (optimized for versioning)
hasChanges, err := unifiedmodel.HasSignificantChanges(oldSchema, newSchema)
if err != nil {
    log.Fatal(err)
}

if hasChanges {
    fmt.Printf("Found %d structural changes\n", len(result.StructuralChanges))
    fmt.Printf("Overall similarity: %.2f\n", result.OverallSimilarity)
    fmt.Printf("Migration complexity: %s\n", result.MigrationComplexity)
    
    // Analyze specific changes with field-level detail
    for _, change := range result.StructuralChanges {
        fmt.Printf("%s: %s at %s (severity: %s, breaking: %t)\n", 
                   change.ChangeType, change.Description, 
                   change.ObjectPath, change.Severity, change.IsBreaking)
    }
}
```

### Sample Data and Privileged Data Detection

```go
// Create sample data for detection
sampleData := unifiedmodel.NewUnifiedModelSampleData(schemaID)

// Configure privacy-aware sampling
config := unifiedmodel.PrivacyAwareSampleDataConfig()
collector := unifiedmodel.NewSampleDataCollector(config)

// Process table sample (would come from anchor service)
tableRows := []map[string]interface{}{
    {"id": 1, "email": "user@example.com", "name": "John Doe"},
    {"id": 2, "email": "jane@example.com", "name": "Jane Smith"},
}
tableSample := collector.ProcessTableSample("users", tableRows, 1000000)
sampleData.TableSamples["users"] = tableSample

// Three-level detection approach
// Level 1: Schema-only detection (fast, 30-80% confidence)
request := unifiedmodel.NewDetectionRequest(schema, unifiedmodel.DetectionLevelSchema)
// Implementation would be in unified model microservice

// Level 2: Enriched detection (enhanced accuracy, 40-90% confidence)
request = request.WithEnrichment(enrichment).WithComplianceFrameworks("GDPR", "HIPAA")

// Level 3: Full detection with sample data (highest accuracy, 60-99% confidence)
request = request.WithSampleData(sampleData)

// Sample data redaction for privacy
err := unifiedmodel.RedactSensitiveData(sampleData)
if err != nil {
    log.Fatal(err)
}

// Validate sample data before use
errors := unifiedmodel.ValidateSampleData(sampleData)
if len(errors) > 0 {
    log.Printf("Sample data validation warnings: %d", len(errors))
}
```

### Advanced Analytics

```go
// Generate comprehensive metrics
metrics := unifiedmodel.GenerateBasicMetrics(schema, schemaID)

// Add size information
metrics.AddTableSize("users", 1024*1024, 512*1024) // 1MB data, 512KB indexes
metrics.AddTableRows("users", 10000)               // 10K rows

// Performance data (from monitoring system)
metrics.PerformanceMetrics.AvgQueryTime = &25.5     // 25.5ms average
metrics.PerformanceMetrics.QueriesPerSecond = &1200 // 1200 QPS

// Get summary
summary := metrics.GetMetricsSummary()
fmt.Printf("Schema: %s, Total Size: %d bytes, Quality Score: %.2f\n",
    summary.SchemaID, summary.TotalSizeBytes, summary.OverallQualityScore)
```

### Object Access

```go
// Retrieve specific objects
table, exists := schema.GetTable("users")
if exists {
    fmt.Printf("Table %s has %d columns\n", table.Name, len(table.Columns))
}

// Check object existence
hasUsers := schema.HasObject(unifiedmodel.ObjectTypeTable, "users")

// Get all objects of a type
tables := schema.GetObjectsByType(unifiedmodel.ObjectTypeTable)

// Find object references
refs := unifiedmodel.FindObjectReferences(schema, unifiedmodel.ObjectTypeTable, "users")
for _, ref := range refs {
    fmt.Printf("%s %s references table users\n", ref.SourceType, ref.SourceName)
}
```

## Service Integration

### Anchor Service Usage

The anchor service extracts schemas from databases and converts them to UnifiedModel format:

```go
// In anchor service
func DiscoverSchema(dbConnection DatabaseConnection) (*unifiedmodel.UnifiedModel, error) {
    schema := &unifiedmodel.UnifiedModel{
        DatabaseType: dbConnection.Type,
    }
    
    // Extract tables, indexes, constraints, etc.
    // ... database-specific extraction logic
    
    // Validate before returning
    if errors := unifiedmodel.ValidateSchema(schema); len(errors) > 0 {
        return nil, fmt.Errorf("invalid schema: %v", errors)
    }
    
    return schema, nil
}
```

### Unified Model Service Usage

The unified model service handles schema versioning and comparison:

```go
// In unified model service
func StoreSchemaVersion(schema *unifiedmodel.UnifiedModel) error {
    schemaID := unifiedmodel.GenerateSchemaID(schema)
    
    // Check if schema already exists
    existing, err := repository.GetLatestSchema(schema.DatabaseType)
    if err != nil {
        return err
    }
    
    if existing != nil {
        // Compare with existing version
        hasChanges, err := unifiedmodel.HasSignificantChanges(existing, schema)
        if err != nil {
            return err
        }
        
        if !hasChanges {
            return nil // No changes, skip versioning
        }
    }
    
    // Store new version
    return repository.StoreSchema(schemaID, schema)
}
```

### Core Service Usage

The core service provides gRPC endpoints for schema operations:

```go
// In core service gRPC handlers
func (s *Server) GetSchema(ctx context.Context, req *GetSchemaRequest) (*GetSchemaResponse, error) {
    schema, err := s.repository.GetSchema(req.SchemaId)
    if err != nil {
        return nil, err
    }
    
    // Generate current metrics
    metrics := schema.GetBasicMetrics(req.SchemaId)
    
    // Get schema info
    info := unifiedmodel.GetSchemaInfo(schema)
    
    return &GetSchemaResponse{
        Schema:  schema,
        Metrics: metrics,
        Info:    info,
    }, nil
}
```

## Type Safety

The package provides extensive type safety through string enums:

```go
// Object types
type ObjectType string
const (
    ObjectTypeTable            ObjectType = "table"
    ObjectTypeCollection       ObjectType = "collection"
    ObjectTypeView             ObjectType = "view"
    // ... 30+ object types
)

// Constraint types
type ConstraintType string
const (
    ConstraintTypePrimaryKey   ConstraintType = "primary_key"
    ConstraintTypeForeignKey   ConstraintType = "foreign_key"
    // ... 7 constraint types
)

// Index types
type IndexType string
const (
    IndexTypeBTree             IndexType = "btree"
    IndexTypeHash              IndexType = "hash"
    // ... 15+ index types
)
```

## Best Practices

### 1. **Schema Validation**
Always validate schemas before storage or comparison:

```go
if errors := unifiedmodel.ValidateSchema(schema); len(errors) > 0 {
    // Handle validation errors
    for _, err := range errors {
        log.Printf("Validation error: %s", err.Message)
    }
    return fmt.Errorf("schema validation failed")
}
```

### 2. **Metrics Separation**
Generate metrics separately from schema structure:

```go
// Store schema
schemaID := unifiedmodel.GenerateSchemaID(schema)
err := storeSchema(schemaID, schema)

// Generate and store metrics separately
metrics := schema.GetBasicMetrics(schemaID)
err = storeMetrics(schemaID, metrics)
```

### 3. **Comparison Options**
Use appropriate comparison options for your use case:

```go
// For version control (structural only)
options := unifiedmodel.DefaultComparisonOptions()

// For migration planning (include enrichment)
options := unifiedmodel.EnrichedComparisonOptions()
```

### 4. **Error Handling**
Handle errors gracefully with informative messages:

```go
result, err := unifiedmodel.CompareSchemas(source, target, options)
if err != nil {
    return fmt.Errorf("schema comparison failed: %w", err)
}

if result.HasStructuralChanges {
    log.Printf("Found %d changes, similarity: %.2f", 
               len(result.StructuralChanges), result.OverallSimilarity)
}
```

## Architecture Benefits

### 🎯 **Single Source of Truth**
- One schema format for all database technologies
- Consistent object representation across paradigms
- Unified analytics and metrics

### 🔄 **Clean Separation**
- **Structure**: Pure schema definition
- **Analytics**: Performance and sizing metrics  
- **Intelligence**: AI-derived insights and classification
- **Context**: Comparison and conversion guidance

### 🚀 **Service Enablement**
- **Anchor Service**: Schema discovery and sample data extraction
- **Unified Model Service**: Schema versioning, comparison, and privileged data detection
- **Core Service**: gRPC API for schema operations
- **Shared Package**: Common types, utilities, and detection interfaces

**Detection Architecture Decision**: While detection types and interfaces are defined in the shared package for consistency, the actual detection implementation remains in the Unified Model microservice. This provides flexibility for:
- Complex business logic and compliance rules
- Machine learning model integration  
- Custom pattern libraries and configuration
- Performance optimization and caching
- Service-specific security and audit requirements

### 📈 **Comprehensive Analytics**
- 500+ metric fields across 7 categories
- Growth trend analysis and capacity planning
- Data quality assessment and issue detection
- Performance monitoring and optimization guidance

### 🔒 **Type Safety**
- String enums for all object types
- Compile-time validation of type usage
- IDE auto-completion and error detection

## API Reference

### Core Functions

| Function | Purpose | Usage |
|----------|---------|-------|
| `GenerateSchemaID(schema)` | Generate unique schema identifier | Schema versioning |
| `GenerateSchemaHash(schema)` | Generate content hash | Change detection |
| `ValidateSchema(schema)` | Validate schema consistency | Quality assurance |
| `CompareSchemas(source, target, options)` | Compare two schemas | Version control |
| `HasSignificantChanges(source, target)` | Check for meaningful changes | Automated versioning |
| `SerializeSchema(schema)` | Convert to JSON | Storage |
| `DeserializeSchema(data)` | Parse from JSON | Loading |
| `CloneSchema(schema)` | Deep copy schema | Manipulation |
| `MergeSchemas(base, overlay)` | Combine schemas | Updates |

### Schema Methods

| Method | Purpose | Usage |
|--------|---------|-------|
| `schema.GetBasicMetrics(id)` | Generate metrics | Analytics |
| `schema.GetTable(name)` | Retrieve table | Object access |
| `schema.AddTable(table)` | Add table | Schema building |
| `schema.HasObject(type, name)` | Check existence | Validation |
| `schema.GetObjectsByType(type)` | Get objects by type | Filtering |

### Utility Functions

| Function | Purpose | Usage |
|----------|---------|-------|
| `GetSchemaInfo(schema)` | Schema summary | Overview |
| `GetObjectNames(schema)` | Object name lists | Navigation |
| `FilterObjects(schema, filter)` | Filter schema | Subset operations |
| `FindObjectReferences(schema, type, name)` | Find dependencies | Impact analysis |

This package provides the foundation for all schema operations across the redb ecosystem, enabling seamless database technology integration and comprehensive schema management capabilities.
