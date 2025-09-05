package unifiedmodel

import (
	"testing"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

func TestTypeConverter_ConvertDataType(t *testing.T) {
	converter := NewTypeConverter()

	tests := []struct {
		name            string
		sourceDB        dbcapabilities.DatabaseType
		targetDB        dbcapabilities.DatabaseType
		sourceType      string
		expectedTarget  string
		expectedUnified UnifiedDataType
		expectError     bool
	}{
		// PostgreSQL to MongoDB conversions
		{
			name:            "PostgreSQL integer to MongoDB",
			sourceDB:        dbcapabilities.PostgreSQL,
			targetDB:        dbcapabilities.MongoDB,
			sourceType:      "integer",
			expectedTarget:  "int32",
			expectedUnified: UnifiedTypeInt32,
			expectError:     false,
		},
		{
			name:            "PostgreSQL bigint to MongoDB",
			sourceDB:        dbcapabilities.PostgreSQL,
			targetDB:        dbcapabilities.MongoDB,
			sourceType:      "bigint",
			expectedTarget:  "int64",
			expectedUnified: UnifiedTypeInt64,
			expectError:     false,
		},
		{
			name:            "PostgreSQL text to MongoDB",
			sourceDB:        dbcapabilities.PostgreSQL,
			targetDB:        dbcapabilities.MongoDB,
			sourceType:      "text",
			expectedTarget:  "string",
			expectedUnified: UnifiedTypeString,
			expectError:     false,
		},
		{
			name:            "PostgreSQL boolean to MongoDB",
			sourceDB:        dbcapabilities.PostgreSQL,
			targetDB:        dbcapabilities.MongoDB,
			sourceType:      "boolean",
			expectedTarget:  "boolean",
			expectedUnified: UnifiedTypeBoolean,
			expectError:     false,
		},
		{
			name:            "PostgreSQL jsonb to MongoDB",
			sourceDB:        dbcapabilities.PostgreSQL,
			targetDB:        dbcapabilities.MongoDB,
			sourceType:      "jsonb",
			expectedTarget:  "object",
			expectedUnified: UnifiedTypeJSON,
			expectError:     false,
		},

		// MongoDB to PostgreSQL conversions
		{
			name:            "MongoDB int32 to PostgreSQL",
			sourceDB:        dbcapabilities.MongoDB,
			targetDB:        dbcapabilities.PostgreSQL,
			sourceType:      "int32",
			expectedTarget:  "integer",
			expectedUnified: UnifiedTypeInt32,
			expectError:     false,
		},
		{
			name:            "MongoDB string to PostgreSQL",
			sourceDB:        dbcapabilities.MongoDB,
			targetDB:        dbcapabilities.PostgreSQL,
			sourceType:      "string",
			expectedTarget:  "text",
			expectedUnified: UnifiedTypeString,
			expectError:     false,
		},
		{
			name:            "MongoDB object to PostgreSQL",
			sourceDB:        dbcapabilities.MongoDB,
			targetDB:        dbcapabilities.PostgreSQL,
			sourceType:      "object",
			expectedTarget:  "jsonb",
			expectedUnified: UnifiedTypeJSON,
			expectError:     false,
		},

		// PostgreSQL to MySQL conversions
		{
			name:            "PostgreSQL integer to MySQL",
			sourceDB:        dbcapabilities.PostgreSQL,
			targetDB:        dbcapabilities.MySQL,
			sourceType:      "integer",
			expectedTarget:  "int",
			expectedUnified: UnifiedTypeInt32,
			expectError:     false,
		},
		{
			name:            "PostgreSQL text to MySQL",
			sourceDB:        dbcapabilities.PostgreSQL,
			targetDB:        dbcapabilities.MySQL,
			sourceType:      "text",
			expectedTarget:  "text",
			expectedUnified: UnifiedTypeString,
			expectError:     false,
		},

		// MySQL to PostgreSQL conversions
		{
			name:            "MySQL int to PostgreSQL",
			sourceDB:        dbcapabilities.MySQL,
			targetDB:        dbcapabilities.PostgreSQL,
			sourceType:      "int",
			expectedTarget:  "integer",
			expectedUnified: UnifiedTypeInt32,
			expectError:     false,
		},

		// Cross-paradigm conversions (MySQL to Neo4j)
		{
			name:            "MySQL int to Neo4j",
			sourceDB:        dbcapabilities.MySQL,
			targetDB:        dbcapabilities.Neo4j,
			sourceType:      "int",
			expectedTarget:  "INTEGER",
			expectedUnified: UnifiedTypeInt32,
			expectError:     false,
		},
		{
			name:            "MySQL boolean to Neo4j",
			sourceDB:        dbcapabilities.MySQL,
			targetDB:        dbcapabilities.Neo4j,
			sourceType:      "boolean",
			expectedTarget:  "BOOLEAN",
			expectedUnified: UnifiedTypeBoolean,
			expectError:     false,
		},

		// Type aliases
		{
			name:            "PostgreSQL int4 alias to MongoDB",
			sourceDB:        dbcapabilities.PostgreSQL,
			targetDB:        dbcapabilities.MongoDB,
			sourceType:      "int4",
			expectedTarget:  "int32",
			expectedUnified: UnifiedTypeInt32,
			expectError:     false,
		},
		{
			name:            "PostgreSQL int8 alias to MongoDB",
			sourceDB:        dbcapabilities.PostgreSQL,
			targetDB:        dbcapabilities.MongoDB,
			sourceType:      "int8",
			expectedTarget:  "int64",
			expectedUnified: UnifiedTypeInt64,
			expectError:     false,
		},

		// Error cases
		{
			name:        "Unknown source type",
			sourceDB:    dbcapabilities.PostgreSQL,
			targetDB:    dbcapabilities.MongoDB,
			sourceType:  "unknown_type",
			expectError: true,
		},
		{
			name:        "Unsupported database combination",
			sourceDB:    dbcapabilities.PostgreSQL,
			targetDB:    "unsupported_db",
			sourceType:  "integer",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := converter.ConvertDataType(tt.sourceDB, tt.targetDB, tt.sourceType)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if result.ConvertedType != tt.expectedTarget {
				t.Errorf("Expected target type %s, got %s", tt.expectedTarget, result.ConvertedType)
			}

			if result.UnifiedType != tt.expectedUnified {
				t.Errorf("Expected unified type %s, got %s", tt.expectedUnified, result.UnifiedType)
			}

			if result.OriginalType != tt.sourceType {
				t.Errorf("Expected original type %s, got %s", tt.sourceType, result.OriginalType)
			}
		})
	}
}

func TestTypeConverter_ConvertDataTypeWithParameters(t *testing.T) {
	converter := NewTypeConverter()

	tests := []struct {
		name           string
		sourceDB       dbcapabilities.DatabaseType
		targetDB       dbcapabilities.DatabaseType
		sourceType     string
		expectedTarget string
		expectError    bool
	}{
		{
			name:           "PostgreSQL varchar with length to MySQL",
			sourceDB:       dbcapabilities.PostgreSQL,
			targetDB:       dbcapabilities.MySQL,
			sourceType:     "varchar(255)",
			expectedTarget: "varchar(255)", // Should preserve parameters
			expectError:    false,
		},
		{
			name:           "PostgreSQL varchar to MongoDB (no parameters)",
			sourceDB:       dbcapabilities.PostgreSQL,
			targetDB:       dbcapabilities.MongoDB,
			sourceType:     "varchar(100)",
			expectedTarget: "string", // MongoDB doesn't preserve length parameters
			expectError:    false,
		},
		{
			name:           "MySQL varchar to PostgreSQL",
			sourceDB:       dbcapabilities.MySQL,
			targetDB:       dbcapabilities.PostgreSQL,
			sourceType:     "varchar(50)",
			expectedTarget: "varchar(50)",
			expectError:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := converter.ConvertDataType(tt.sourceDB, tt.targetDB, tt.sourceType)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if result.ConvertedType != tt.expectedTarget {
				t.Errorf("Expected target type %s, got %s", tt.expectedTarget, result.ConvertedType)
			}
		})
	}
}

func TestTypeConverter_ConvertColumn(t *testing.T) {
	converter := NewTypeConverter()

	column := Column{
		Name:     "test_column",
		DataType: "integer",
		Nullable: true,
	}

	result, err := converter.ConvertColumn(column, dbcapabilities.PostgreSQL, dbcapabilities.MongoDB)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if result.DataType != "int32" {
		t.Errorf("Expected converted data type int32, got %s", result.DataType)
	}

	if result.Name != "test_column" {
		t.Errorf("Expected column name to be preserved")
	}

	// Check conversion metadata
	if result.Options == nil {
		t.Errorf("Expected conversion metadata in options")
	} else {
		if result.Options["original_type"] != "integer" {
			t.Errorf("Expected original_type metadata")
		}
		if result.Options["unified_type"] != "int32" {
			t.Errorf("Expected unified_type metadata")
		}
	}
}

func TestTypeConverter_ConvertField(t *testing.T) {
	converter := NewTypeConverter()

	field := Field{
		Name: "test_field",
		Type: "string",
	}

	result, err := converter.ConvertField(field, dbcapabilities.MongoDB, dbcapabilities.PostgreSQL)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if result.Type != "text" {
		t.Errorf("Expected converted type text, got %s", result.Type)
	}

	if result.Name != "test_field" {
		t.Errorf("Expected field name to be preserved")
	}
}

func TestTypeConverter_ValidateTypeConversion(t *testing.T) {
	converter := NewTypeConverter()

	tests := []struct {
		name        string
		sourceDB    dbcapabilities.DatabaseType
		targetDB    dbcapabilities.DatabaseType
		sourceType  string
		expectValid bool
	}{
		{
			name:        "Valid conversion",
			sourceDB:    dbcapabilities.PostgreSQL,
			targetDB:    dbcapabilities.MongoDB,
			sourceType:  "integer",
			expectValid: true,
		},
		{
			name:        "Invalid conversion",
			sourceDB:    dbcapabilities.PostgreSQL,
			targetDB:    dbcapabilities.MongoDB,
			sourceType:  "unknown_type",
			expectValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validation, err := converter.ValidateTypeConversion(tt.sourceDB, tt.targetDB, tt.sourceType)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if validation.IsSupported != tt.expectValid {
				t.Errorf("Expected validation result %t, got %t", tt.expectValid, validation.IsSupported)
			}
		})
	}
}

func TestTypeConverter_GetSupportedConversions(t *testing.T) {
	converter := NewTypeConverter()

	// Test getting supported conversions for a specific database pair
	conversions := converter.GetSupportedConversions(dbcapabilities.PostgreSQL, dbcapabilities.MongoDB)

	// Now that we generate conversions from metadata, we should get actual results
	if len(conversions) == 0 {
		t.Errorf("Expected some conversions to be returned from metadata")
	}

	// Verify we get expected conversions
	expectedConversions := map[string]string{
		"integer": "int32",
		"bigint":  "int64",
		"text":    "string",
		"boolean": "boolean",
		"jsonb":   "object",
	}

	conversionMap := make(map[string]string)
	for _, conv := range conversions {
		conversionMap[conv.SourceType] = conv.TargetType
	}

	for sourceType, expectedTarget := range expectedConversions {
		if actualTarget, exists := conversionMap[sourceType]; !exists {
			t.Errorf("Expected conversion for %s not found", sourceType)
		} else if actualTarget != expectedTarget {
			t.Errorf("Expected %s -> %s, got %s -> %s", sourceType, expectedTarget, sourceType, actualTarget)
		}
	}

	t.Logf("Found %d supported conversions from PostgreSQL to MongoDB", len(conversions))
}

// Benchmark tests to ensure the unified approach is performant
func BenchmarkTypeConverter_ConvertDataType(b *testing.B) {
	converter := NewTypeConverter()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := converter.ConvertDataType(dbcapabilities.PostgreSQL, dbcapabilities.MongoDB, "integer")
		if err != nil {
			b.Fatalf("Unexpected error: %v", err)
		}
	}
}

func BenchmarkTypeConverter_ConvertDataTypeCrossParadigm(b *testing.B) {
	converter := NewTypeConverter()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := converter.ConvertDataType(dbcapabilities.MySQL, dbcapabilities.Neo4j, "int")
		if err != nil {
			b.Fatalf("Unexpected error: %v", err)
		}
	}
}

// Test the scalability improvement
func TestScalabilityImprovement(t *testing.T) {
	converter := NewTypeConverter()

	// Get all supported database types
	allDBs := dbcapabilities.IDs()

	if len(allDBs) < 25 {
		t.Errorf("Expected at least 25 supported databases, got %d", len(allDBs))
	}

	// Test conversions between different paradigms
	testCases := []struct {
		sourceDB   dbcapabilities.DatabaseType
		targetDB   dbcapabilities.DatabaseType
		sourceType string
	}{
		{dbcapabilities.PostgreSQL, dbcapabilities.MongoDB, "integer"},
		{dbcapabilities.MySQL, dbcapabilities.Neo4j, "int"},
		{dbcapabilities.MongoDB, dbcapabilities.PostgreSQL, "string"},
	}

	successCount := 0
	for _, tc := range testCases {
		_, err := converter.ConvertDataType(tc.sourceDB, tc.targetDB, tc.sourceType)
		if err == nil {
			successCount++
		}
	}

	if successCount == 0 {
		t.Errorf("Expected at least some cross-paradigm conversions to succeed")
	}

	t.Logf("Successfully converted %d/%d cross-paradigm type conversions", successCount, len(testCases))
	t.Logf("Total supported databases: %d", len(allDBs))
	t.Logf("Old approach would require: %d direct rules (O(n²))", len(allDBs)*(len(allDBs)-1))
	t.Logf("New approach requires: %d unified mappings (O(n))", len(allDBs))
}
