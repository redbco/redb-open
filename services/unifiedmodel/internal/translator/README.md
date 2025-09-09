# Unified Translator v2

A sophisticated, paradigm-aware database schema translator that leverages the shared UnifiedModel infrastructure to perform intelligent conversions between different database technologies.

## Overview

The Unified Translator v2 is a complete rewrite of the original translator, designed to handle the complexity of modern database ecosystems. It supports both same-paradigm translations (e.g., PostgreSQL → MySQL) and cross-paradigm translations (e.g., Relational → Document, Relational → Graph).

## Key Features

### 🎯 Paradigm-Aware Translation
- **Same-Paradigm**: Direct object mapping with type conversion (PostgreSQL ↔ MySQL)
- **Cross-Paradigm**: Structural transformation with enrichment data (Relational → Document/Graph/Vector)
- **Multi-Step**: Complex conversions through intermediate databases when direct conversion isn't optimal

### 🧠 Intelligent Conversion Strategies
- **Normalization**: Document → Relational (flatten nested structures)
- **Denormalization**: Relational → Document (embed related data)
- **Decomposition**: Relational → Graph/Vector (extract entities and relationships)
- **Aggregation**: Graph → Relational/Document (combine nodes and edges)
- **Hybrid**: Adaptive strategy selection based on schema characteristics

### 📊 Enrichment-Driven Conversion
- **Data Classification**: Entity vs Junction vs Lookup table identification
- **Relationship Analysis**: Foreign key semantics and cardinality
- **Access Patterns**: Query patterns to optimize target structure
- **Business Rules**: Domain logic preservation across paradigms
- **Performance Hints**: Optimization guidance for target database

### 🔍 Comprehensive Analysis
- **Feasibility Analysis**: Pre-translation compatibility assessment
- **Complexity Estimation**: Processing time and success rate predictions
- **Feature Gap Analysis**: Unsupported feature identification with alternatives
- **Schema Health Metrics**: Quality assessment and improvement suggestions

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Unified Translator v2                    │
├─────────────────────────────────────────────────────────────┤
│  Core Components                                            │
│  ├── UnifiedTranslator (Main Interface)                     │
│  ├── ParadigmAnalyzer (Routing Logic)                       │
│  └── TranslationContext (State Management)                  │
├─────────────────────────────────────────────────────────────┤
│  Same-Paradigm Translation                                  │
│  ├── ObjectMapper (Direct Mapping)                          │
│  ├── CapabilityFilter (Feature Support)                     │
│  └── TypeConverter (Data Type Conversion)                   │
├─────────────────────────────────────────────────────────────┤
│  Cross-Paradigm Translation                                 │
│  ├── EnrichmentAnalyzer (Context Analysis)                  │
│  ├── StructureTransformer (Schema Transformation)           │
│  └── RelationshipMapper (Relationship Conversion)           │
├─────────────────────────────────────────────────────────────┤
│  Shared Infrastructure                                      │
│  ├── pkg/unifiedmodel (165+ Object Types)                   │
│  ├── pkg/dbcapabilities (Database Features)                 │
│  └── ConversionEngine (Type & Paradigm Conversion)          │
└─────────────────────────────────────────────────────────────┘
```

## Supported Database Paradigms

| Source Paradigm | Target Paradigms | Strategy | Complexity |
|-----------------|------------------|----------|------------|
| Relational | Relational | Direct/Transform | Trivial-Simple |
| Relational | Document | Denormalization | Moderate |
| Relational | Graph | Decomposition | Complex |
| Relational | Vector | Decomposition | Complex |
| Document | Relational | Normalization | Moderate |
| Document | Graph | Decomposition | Moderate |
| Document | Vector | Decomposition | Moderate |
| Graph | Relational | Aggregation | Complex |
| Graph | Document | Aggregation | Moderate |

## Usage Examples

### Basic Translation

```go
import "github.com/redbco/redb-open/services/unifiedmodel/internal/translator"

// Create translator
translator := translator.NewUnifiedTranslator()

// Create translation request
request := &core.TranslationRequest{
    SourceDatabase: dbcapabilities.PostgreSQL,
    TargetDatabase: dbcapabilities.MongoDB,
    SourceSchema:   sourceSchemaJSON,
    Preferences: core.TranslationPreferences{
        PreferredStrategy:      core.ConversionStrategyDenormalization,
        AcceptDataLoss:         true,
        PreserveRelationships:  true,
    },
    RequestID: "my-translation-001",
}

// Analyze feasibility
ctx := context.Background()
analysis, err := translator.AnalyzeTranslation(ctx, request)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Conversion supported: %t\n", analysis.ConversionSupported)
fmt.Printf("Complexity: %s\n", analysis.TranslationComplexity)
fmt.Printf("Success rate: %.2f%%\n", analysis.EstimatedSuccessRate*100)

// Perform translation
if analysis.ConversionSupported {
    result, err := translator.Translate(ctx, request)
    if err != nil {
        log.Fatal(err)
    }
    
    if result.Success {
        fmt.Printf("Translation completed in %v\n", result.ProcessingTime)
        // Use result.UnifiedSchema for the converted schema
    }
}
```

### Same-Paradigm Translation (PostgreSQL → MySQL)

```go
request := &core.TranslationRequest{
    SourceDatabase: dbcapabilities.PostgreSQL,
    TargetDatabase: dbcapabilities.MySQL,
    SourceSchema:   postgresSchemaJSON,
    Preferences: core.TranslationPreferences{
        OptimizeForPerformance: true,
        GenerateComments:       true,
    },
}

result, err := translator.Translate(ctx, request)
// Direct object mapping with type conversion
```

### Cross-Paradigm Translation with Enrichment

```go
// Create enrichment data to guide conversion
enrichment := &unifiedmodel.UnifiedModelEnrichment{
    // Data classification, relationships, access patterns, etc.
}

request := &core.TranslationRequest{
    SourceDatabase: dbcapabilities.PostgreSQL,
    TargetDatabase: dbcapabilities.Neo4j,
    SourceSchema:   postgresSchemaJSON,
    Enrichment:     enrichment,
    Preferences: core.TranslationPreferences{
        PreferredStrategy: core.ConversionStrategyDecomposition,
    },
}

result, err := translator.Translate(ctx, request)
// Tables → Nodes, Foreign Keys → Edges
```

## Translation Strategies

### Denormalization (Relational → Document)
- **Entity Tables** → Collections with embedded related data
- **Junction Tables** → Arrays or document references
- **Foreign Keys** → Document references or embedding
- **Lookup Tables** → Embedded enums or reference collections

### Decomposition (Relational → Graph)
- **Entity Tables** → Node types
- **Foreign Keys** → Edge relationships
- **Junction Tables** → Relationship properties or separate edges
- **Attributes** → Node/Edge properties

### Normalization (Document → Relational)
- **Collections** → Tables with extracted nested objects
- **Nested Objects** → Separate tables with foreign keys
- **Arrays** → Junction tables
- **References** → Foreign key constraints

## Configuration Options

### Translation Preferences

```go
type TranslationPreferences struct {
    // Strategy preferences
    PreferredStrategy      ConversionStrategy
    AcceptDataLoss         bool
    OptimizeForPerformance bool
    OptimizeForStorage     bool
    PreserveRelationships  bool
    
    // Interactive mode
    InteractiveMode    bool
    AutoApproveSimple  bool
    CustomMappings     map[string]string
    ExcludeObjects     []string
    
    // Output preferences
    GenerateComments       bool
    IncludeOriginalNames   bool
    UseQualifiedNames      bool
    PreserveCaseStyle      bool
}
```

### Enrichment Data Types

- **Data Classification**: Entity/Junction/Lookup table identification
- **Relationships**: Semantic relationship information
- **Access Patterns**: Query patterns and frequency
- **Business Rules**: Domain logic and constraints
- **Performance Hints**: Optimization recommendations
- **Data Flow**: ETL and data movement patterns

## Validation and Quality Assurance

The translator includes comprehensive validation:

```go
validator := utils.NewSchemaValidator()

// Validate translation request
validation := validator.ValidateTranslationRequest(request)
if !validation.IsValid {
    // Handle validation errors
}

// Validate translation result
resultValidation := validator.ValidateTranslationResult(result)
fmt.Printf("Schema health score: %.1f/100\n", resultValidation.SchemaHealth.OverallScore)
```

## Error Handling and Warnings

The translator provides detailed feedback:

- **Critical Errors**: Block translation (missing required fields, unsupported databases)
- **Warnings**: Potential issues (data loss, performance impact, feature limitations)
- **Suggestions**: Optimization recommendations (naming conventions, structure improvements)
- **Unsupported Features**: Features that cannot be converted with suggested alternatives

## Performance Considerations

- **Parallel Processing**: Large schemas are processed in parallel where possible
- **Incremental Translation**: Support for schema updates and partial translations
- **Caching**: Conversion rules and paradigm mappings are cached
- **Memory Management**: Streaming processing for very large schemas
- **Progress Tracking**: Real-time progress updates for long-running translations

## Extensibility

The translator is designed for extensibility:

- **Custom Strategies**: Plugin architecture for new conversion strategies
- **Database Support**: Easy addition of new database types and paradigms
- **Enrichment Providers**: Pluggable enrichment data sources
- **Validation Rules**: Custom validation and quality rules
- **Output Formats**: Multiple output format support (SQL, JSON, etc.)

## Migration from v1

The v2 translator maintains backward compatibility with v1 interfaces while providing enhanced functionality:

```go
// v1 style (still supported)
translator := translator.NewSchemaTranslator()
result, err := translator.Translate(sourceType, targetType, sourceSchema)

// v2 style (recommended)
translator := translator.NewUnifiedTranslator()
result, err := translator.Translate(ctx, request)
```

## Testing

Run the examples to see the translator in action:

```go
import "github.com/redbco/redb-open/services/unifiedmodel/internal/translator"

// Run all examples
translator.RunAllExamples()
```

## Future Enhancements

- **AI-Powered Enrichment**: Automatic enrichment data generation using ML
- **Real-time Translation**: Streaming translation for live schema changes
- **Visual Schema Designer**: GUI for complex translation configuration
- **Translation Templates**: Reusable translation patterns for common scenarios
- **Performance Benchmarking**: Automated performance testing and optimization
- **Multi-target Translation**: Single source to multiple target databases

## Contributing

When adding new database support or conversion strategies:

1. Add database capabilities to `pkg/dbcapabilities`
2. Implement paradigm-specific conversion strategies
3. Add comprehensive test coverage
4. Update documentation and examples
5. Ensure backward compatibility

The Unified Translator v2 represents a significant advancement in database schema translation technology, providing the intelligence and flexibility needed for modern multi-database architectures.
