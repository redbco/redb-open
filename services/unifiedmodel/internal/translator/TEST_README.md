# Translator V2 Test Suite

This directory contains a comprehensive test suite for the Translator V2 implementation, covering all components and scenarios.

## Test Structure

### Core Tests (`core/`)
- **`unified_translator_test.go`**: Tests for the main translator orchestrator
  - Request validation
  - Translation analysis
  - Same-paradigm and cross-paradigm routing
  - Error handling
  - Supported conversions

### Same-Paradigm Tests (`same_paradigm/`)
- **`translator_test.go`**: Tests for same-paradigm translations
  - PostgreSQL ↔ MySQL conversions
  - Object mapping and type conversion
  - Constraint preservation
  - Capability filtering
  - Excluded objects handling

### Cross-Paradigm Tests (`cross_paradigm/`)
- **`translator_test.go`**: Tests for cross-paradigm translations
  - Relational → Document (PostgreSQL → MongoDB)
  - Relational → Graph (PostgreSQL → Neo4j)
  - Document → Relational (MongoDB → PostgreSQL)
  - Different conversion strategies (Denormalization, Normalization, Hybrid)
  - Enrichment-guided translations

- **`enrichment_analyzer_test.go`**: Tests for enrichment analysis
  - Heuristic analysis without enrichment data
  - Enrichment data processing
  - Entity classification (primary, lookup, junction)
  - Performance hints processing
  - Compliance data analysis
  - Helper function validation

### Integration Tests
- **`integration_test.go`**: End-to-end system tests
  - Complete translation workflows
  - Real-world e-commerce schemas
  - Performance metrics validation
  - Error handling scenarios
  - Complex schema handling

### Validation Tests (`utils/`)
- **`validation_test.go`**: Schema validation tests
  - UnifiedModel structure validation
  - Tables, collections, nodes validation
  - Constraint reference validation
  - Mixed object type detection

### Performance Tests
- **`benchmark_test.go`**: Performance benchmarks
  - Small, medium, large schema benchmarks
  - Same-paradigm vs cross-paradigm performance
  - Memory allocation analysis
  - Concurrent translation benchmarks
  - Enrichment processing performance

## Running Tests

### Run All Tests
```bash
cd services/unifiedmodel/internal/translator
go test ./...
```

### Run Specific Test Suites
```bash
# Core translator tests
go test ./core

# Same-paradigm tests
go test ./same_paradigm

# Cross-paradigm tests
go test ./cross_paradigm

# Integration tests
go test -run TestTranslatorV2_EndToEnd

# Validation tests
go test ./utils
```

### Run Benchmarks
```bash
# All benchmarks
go test -bench=.

# Specific benchmarks
go test -bench=BenchmarkTranslatorV2_SameParadigm
go test -bench=BenchmarkTranslatorV2_CrossParadigm

# Memory allocation benchmarks
go test -bench=BenchmarkTranslatorV2_MemoryAllocation -benchmem

# Concurrent benchmarks
go test -bench=BenchmarkTranslatorV2_Concurrent -cpu=1,2,4,8
```

### Run Tests with Coverage
```bash
# Generate coverage report
go test ./... -coverprofile=coverage.out

# View coverage in browser
go tool cover -html=coverage.out

# Coverage summary
go tool cover -func=coverage.out
```

### Run Tests with Race Detection
```bash
go test ./... -race
```

## Test Scenarios Covered

### 1. **Same-Paradigm Translations**
- ✅ PostgreSQL → MySQL
- ✅ MySQL → PostgreSQL
- ✅ Data type mapping
- ✅ Constraint preservation
- ✅ Unsupported feature handling
- ✅ Object exclusion
- ✅ Validation errors

### 2. **Cross-Paradigm Translations**
- ✅ Relational → Document (PostgreSQL → MongoDB)
- ✅ Relational → Graph (PostgreSQL → Neo4j)
- ✅ Document → Relational (MongoDB → PostgreSQL)
- ✅ Conversion strategies:
  - Denormalization
  - Normalization
  - Decomposition
  - Aggregation
  - Hybrid
- ✅ Enrichment-guided translations
- ✅ Complex relationship handling

### 3. **Enrichment Analysis**
- ✅ Heuristic table classification
- ✅ Entity type detection (primary, lookup, junction)
- ✅ Access pattern analysis
- ✅ Performance hint processing
- ✅ Compliance rule generation
- ✅ Relationship strength analysis

### 4. **Error Handling & Validation**
- ✅ Invalid request validation
- ✅ Unsupported database combinations
- ✅ Malformed schema handling
- ✅ Missing enrichment data
- ✅ Constraint reference validation
- ✅ Mixed object type detection

### 5. **Performance & Scalability**
- ✅ Small schema performance (< 5 tables)
- ✅ Medium schema performance (5-20 tables)
- ✅ Large schema performance (20+ tables)
- ✅ Memory allocation efficiency
- ✅ Concurrent translation safety
- ✅ Enrichment processing overhead

### 6. **Integration Scenarios**
- ✅ E-commerce schema translations
- ✅ Complex multi-paradigm schemas
- ✅ Real-world constraint patterns
- ✅ Performance metrics collection
- ✅ Translation report generation

## Test Data

### Schema Complexity Levels
- **Small**: 1-2 tables, basic columns, minimal constraints
- **Medium**: 5-10 tables, relationships, various data types
- **Large**: 20+ tables, complex relationships, multiple constraints
- **Complex**: Mixed object types, views, functions, sequences

### Database Combinations Tested
- **Same-Paradigm**: PostgreSQL ↔ MySQL, Oracle ↔ SQL Server
- **Cross-Paradigm**: 
  - Relational → Document (PostgreSQL → MongoDB)
  - Relational → Graph (PostgreSQL → Neo4j)
  - Document → Relational (MongoDB → PostgreSQL)
  - Graph → Document (Neo4j → MongoDB)

### Enrichment Data Scenarios
- **No Enrichment**: Pure heuristic analysis
- **Partial Enrichment**: Some tables with enrichment data
- **Full Enrichment**: Complete enrichment for all objects
- **Performance Hints**: Index, partition, denormalization recommendations
- **Compliance Data**: GDPR, HIPAA, PCI requirements

## Expected Test Results

### Performance Benchmarks (Approximate)
- **Small Schema**: < 10ms per translation
- **Medium Schema**: < 100ms per translation
- **Large Schema**: < 1s per translation
- **Memory Usage**: < 50MB for medium schemas
- **Concurrent Safety**: No race conditions detected

### Coverage Targets
- **Overall Coverage**: > 85%
- **Core Components**: > 90%
- **Critical Paths**: > 95%
- **Error Handling**: > 80%

## Troubleshooting Tests

### Common Issues
1. **Import Errors**: Ensure all dependencies are available
2. **Timeout Issues**: Increase test timeout for large schemas
3. **Race Conditions**: Run with `-race` flag to detect
4. **Memory Issues**: Monitor with `-benchmem` flag

### Debug Mode
```bash
# Run tests with verbose output
go test -v ./...

# Run specific test with debug info
go test -v -run TestSpecificFunction

# Enable debug logging (if implemented)
DEBUG=true go test ./...
```

## Contributing to Tests

### Adding New Tests
1. Follow existing naming conventions
2. Include both positive and negative test cases
3. Add performance benchmarks for new features
4. Update this README with new test scenarios

### Test Guidelines
- Use table-driven tests for multiple scenarios
- Include edge cases and error conditions
- Mock external dependencies appropriately
- Ensure tests are deterministic and repeatable
- Add meaningful assertions and error messages

### Performance Test Guidelines
- Benchmark realistic data sizes
- Include memory allocation analysis
- Test concurrent scenarios
- Compare against baseline performance
- Document expected performance characteristics
