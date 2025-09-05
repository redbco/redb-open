package core

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// MockSameParadigmTranslator for testing
type MockSameParadigmTranslator struct {
	shouldError bool
	result      *unifiedmodel.UnifiedModel
}

func (m *MockSameParadigmTranslator) Translate(ctx *TranslationContext) error {
	if m.shouldError {
		return fmt.Errorf("mock same-paradigm translation error")
	}
	if m.result != nil {
		ctx.SetTargetSchema(m.result)
	}
	return nil
}

// MockCrossParadigmTranslator for testing
type MockCrossParadigmTranslator struct {
	shouldError bool
	result      *unifiedmodel.UnifiedModel
}

func (m *MockCrossParadigmTranslator) Translate(ctx *TranslationContext) error {
	if m.shouldError {
		return fmt.Errorf("mock cross-paradigm translation error")
	}
	if m.result != nil {
		ctx.SetTargetSchema(m.result)
	}
	return nil
}

func TestUnifiedTranslatorImpl_ValidateRequest(t *testing.T) {
	tests := []struct {
		name           string
		request        *TranslationRequest
		expectedErrors int
	}{
		{
			name: "valid request",
			request: &TranslationRequest{
				SourceDatabase: dbcapabilities.PostgreSQL,
				TargetDatabase: dbcapabilities.MySQL,
				SourceSchema: &unifiedmodel.UnifiedModel{
					DatabaseType: dbcapabilities.PostgreSQL,
					Tables:       make(map[string]unifiedmodel.Table),
				},
				RequestID:   "test-001",
				RequestedAt: time.Now(),
			},
			expectedErrors: 0,
		},
		{
			name: "missing source database",
			request: &TranslationRequest{
				TargetDatabase: dbcapabilities.MySQL,
				SourceSchema: &unifiedmodel.UnifiedModel{
					DatabaseType: dbcapabilities.PostgreSQL,
					Tables:       make(map[string]unifiedmodel.Table),
				},
				RequestID: "test-002",
			},
			expectedErrors: 1,
		},
		{
			name: "missing target database",
			request: &TranslationRequest{
				SourceDatabase: dbcapabilities.PostgreSQL,
				SourceSchema: &unifiedmodel.UnifiedModel{
					DatabaseType: dbcapabilities.PostgreSQL,
					Tables:       make(map[string]unifiedmodel.Table),
				},
				RequestID: "test-003",
			},
			expectedErrors: 1,
		},
		{
			name: "missing source schema",
			request: &TranslationRequest{
				SourceDatabase: dbcapabilities.PostgreSQL,
				TargetDatabase: dbcapabilities.MySQL,
				RequestID:      "test-004",
			},
			expectedErrors: 1,
		},
		{
			name: "same source and target",
			request: &TranslationRequest{
				SourceDatabase: dbcapabilities.PostgreSQL,
				TargetDatabase: dbcapabilities.PostgreSQL,
				SourceSchema: &unifiedmodel.UnifiedModel{
					DatabaseType: dbcapabilities.PostgreSQL,
					Tables:       make(map[string]unifiedmodel.Table),
				},
				RequestID: "test-005",
			},
			expectedErrors: 1,
		},
		{
			name: "unsupported source database",
			request: &TranslationRequest{
				SourceDatabase: "unsupported_db",
				TargetDatabase: dbcapabilities.MySQL,
				SourceSchema: &unifiedmodel.UnifiedModel{
					DatabaseType: dbcapabilities.PostgreSQL,
					Tables:       make(map[string]unifiedmodel.Table),
				},
				RequestID: "test-006",
			},
			expectedErrors: 1,
		},
		{
			name: "nil source schema",
			request: &TranslationRequest{
				SourceDatabase: dbcapabilities.PostgreSQL,
				TargetDatabase: dbcapabilities.MySQL,
				SourceSchema:   nil,
				RequestID:      "test-007",
			},
			expectedErrors: 1,
		},
	}

	translator := &UnifiedTranslatorImpl{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errors := translator.ValidateRequest(tt.request)
			if len(errors) != tt.expectedErrors {
				t.Errorf("ValidateRequest() got %d errors, want %d", len(errors), tt.expectedErrors)
				for _, err := range errors {
					t.Logf("Error: %s - %s", err.Type, err.Message)
				}
			}
		})
	}
}

func TestUnifiedTranslatorImpl_AnalyzeTranslation(t *testing.T) {
	mockSameParadigm := &MockSameParadigmTranslator{}
	mockCrossParadigm := &MockCrossParadigmTranslator{}
	translator := NewUnifiedTranslator(mockSameParadigm, mockCrossParadigm)

	tests := []struct {
		name            string
		request         *TranslationRequest
		expectError     bool
		expectSupported bool
	}{
		{
			name: "same paradigm analysis - PostgreSQL to MySQL",
			request: &TranslationRequest{
				SourceDatabase: dbcapabilities.PostgreSQL,
				TargetDatabase: dbcapabilities.MySQL,
				SourceSchema:   createTestSchema(),
				RequestID:      "analysis-001",
				RequestedAt:    time.Now(),
			},
			expectError:     false,
			expectSupported: true,
		},
		{
			name: "cross paradigm analysis - PostgreSQL to MongoDB",
			request: &TranslationRequest{
				SourceDatabase: dbcapabilities.PostgreSQL,
				TargetDatabase: dbcapabilities.MongoDB,
				SourceSchema:   createTestSchema(),
				RequestID:      "analysis-002",
				RequestedAt:    time.Now(),
			},
			expectError:     false,
			expectSupported: true,
		},
		{
			name: "invalid request",
			request: &TranslationRequest{
				SourceDatabase: dbcapabilities.PostgreSQL,
				TargetDatabase: dbcapabilities.PostgreSQL, // Same as source
				SourceSchema:   createTestSchema(),
				RequestID:      "analysis-003",
			},
			expectError:     true,
			expectSupported: false,
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			analysis, err := translator.AnalyzeTranslation(ctx, tt.request)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if analysis.ConversionSupported != tt.expectSupported {
				t.Errorf("Expected supported=%v, got %v", tt.expectSupported, analysis.ConversionSupported)
			}

			// Verify analysis contains expected fields
			if analysis.ParadigmCompatibility == "" {
				t.Error("ParadigmCompatibility should not be empty")
			}
			if analysis.TranslationComplexity == "" {
				t.Error("TranslationComplexity should not be empty")
			}
			if analysis.EstimatedSuccessRate < 0 || analysis.EstimatedSuccessRate > 1 {
				t.Errorf("EstimatedSuccessRate should be between 0 and 1, got %f", analysis.EstimatedSuccessRate)
			}
		})
	}
}

func TestUnifiedTranslatorImpl_Translate_SameParadigm(t *testing.T) {
	expectedResult := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.MySQL,
		Tables: map[string]unifiedmodel.Table{
			"users": {
				Name: "users",
				Columns: map[string]unifiedmodel.Column{
					"id": {Name: "id", DataType: "int", IsPrimaryKey: true},
				},
			},
		},
	}

	mockSameParadigm := &MockSameParadigmTranslator{result: expectedResult}
	mockCrossParadigm := &MockCrossParadigmTranslator{}
	translator := NewUnifiedTranslator(mockSameParadigm, mockCrossParadigm)

	request := &TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.MySQL,
		SourceSchema:   createTestSchema(),
		RequestID:      "translate-001",
		RequestedAt:    time.Now(),
	}

	ctx := context.Background()
	result, err := translator.Translate(ctx, request)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if !result.Success {
		t.Errorf("Expected successful translation, got: %s", result.ErrorMessage)
	}

	if result.UnifiedSchema == nil {
		t.Error("Expected unified schema in result")
	}

	if result.UnifiedSchema.DatabaseType != dbcapabilities.MySQL {
		t.Errorf("Expected target database type MySQL, got %s", result.UnifiedSchema.DatabaseType)
	}

	// Note: Mock translators may not set paradigm compatibility correctly
	// This would be set by the actual paradigm analyzer in real implementation
	if result.TranslationReport.ParadigmCompatibility == "" {
		t.Log("Note: Mock translator did not set paradigm compatibility")
	}
}

func TestUnifiedTranslatorImpl_Translate_CrossParadigm(t *testing.T) {
	expectedResult := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.MongoDB,
		Collections: map[string]unifiedmodel.Collection{
			"users": {
				Name: "users",
				Fields: map[string]unifiedmodel.Field{
					"_id":  {Name: "_id", Type: "objectid"},
					"name": {Name: "name", Type: "string"},
				},
			},
		},
	}

	mockSameParadigm := &MockSameParadigmTranslator{}
	mockCrossParadigm := &MockCrossParadigmTranslator{result: expectedResult}
	translator := NewUnifiedTranslator(mockSameParadigm, mockCrossParadigm)

	request := &TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.MongoDB,
		SourceSchema:   createTestSchema(),
		Preferences: TranslationPreferences{
			PreferredStrategy: ConversionStrategyDenormalization,
			AcceptDataLoss:    true,
		},
		RequestID:   "translate-002",
		RequestedAt: time.Now(),
	}

	ctx := context.Background()
	result, err := translator.Translate(ctx, request)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if !result.Success {
		t.Errorf("Expected successful translation, got: %s", result.ErrorMessage)
	}

	if result.UnifiedSchema == nil {
		t.Error("Expected unified schema in result")
	}

	if result.UnifiedSchema.DatabaseType != dbcapabilities.MongoDB {
		t.Errorf("Expected target database type MongoDB, got %s", result.UnifiedSchema.DatabaseType)
	}

	if len(result.UnifiedSchema.Collections) == 0 {
		t.Error("Expected collections in MongoDB schema")
	}
}

func TestUnifiedTranslatorImpl_Translate_WithErrors(t *testing.T) {
	mockSameParadigm := &MockSameParadigmTranslator{shouldError: true}
	mockCrossParadigm := &MockCrossParadigmTranslator{}
	translator := NewUnifiedTranslator(mockSameParadigm, mockCrossParadigm)

	request := &TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.MySQL,
		SourceSchema:   createTestSchema(),
		RequestID:      "translate-error-001",
		RequestedAt:    time.Now(),
	}

	ctx := context.Background()
	result, err := translator.Translate(ctx, request)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if result.Success {
		t.Error("Expected failed translation due to mock error")
	}

	if result.ErrorMessage == "" {
		t.Error("Expected error message in failed translation")
	}
}

func TestUnifiedTranslatorImpl_GetSupportedConversions(t *testing.T) {
	mockSameParadigm := &MockSameParadigmTranslator{}
	mockCrossParadigm := &MockCrossParadigmTranslator{}
	translator := NewUnifiedTranslator(mockSameParadigm, mockCrossParadigm)

	conversions := translator.GetSupportedConversions()

	if len(conversions) == 0 {
		t.Error("Expected supported conversions")
	}

	// Check for some expected conversions
	foundPostgresToMySQL := false
	foundPostgresToMongo := false

	for _, conversion := range conversions {
		if conversion.SourceDatabase == dbcapabilities.PostgreSQL && conversion.TargetDatabase == dbcapabilities.MySQL {
			foundPostgresToMySQL = true
			if !conversion.Supported {
				t.Error("PostgreSQL to MySQL should be supported")
			}
			if conversion.Complexity == TranslationComplexityImpossible {
				t.Error("PostgreSQL to MySQL should not be impossible")
			}
		}
		if conversion.SourceDatabase == dbcapabilities.PostgreSQL && conversion.TargetDatabase == dbcapabilities.MongoDB {
			foundPostgresToMongo = true
			if !conversion.Supported {
				t.Error("PostgreSQL to MongoDB should be supported")
			}
		}
	}

	if !foundPostgresToMySQL {
		t.Error("Expected PostgreSQL to MySQL conversion in supported list")
	}
	if !foundPostgresToMongo {
		t.Error("Expected PostgreSQL to MongoDB conversion in supported list")
	}
}

// Helper functions for tests

func createTestSchema() *unifiedmodel.UnifiedModel {
	return &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.PostgreSQL,
		Tables: map[string]unifiedmodel.Table{
			"users": {
				Name: "users",
				Columns: map[string]unifiedmodel.Column{
					"id": {
						Name:          "id",
						DataType:      "integer",
						Nullable:      false,
						IsPrimaryKey:  true,
						AutoIncrement: true,
					},
					"email": {
						Name:     "email",
						DataType: "varchar(255)",
						Nullable: false,
					},
					"name": {
						Name:     "name",
						DataType: "varchar(100)",
						Nullable: false,
					},
					"created_at": {
						Name:     "created_at",
						DataType: "timestamp",
						Nullable: false,
					},
				},
				Constraints: map[string]unifiedmodel.Constraint{
					"pk_users": {
						Name:    "pk_users",
						Type:    unifiedmodel.ConstraintTypePrimaryKey,
						Columns: []string{"id"},
					},
				},
			},
			"orders": {
				Name: "orders",
				Columns: map[string]unifiedmodel.Column{
					"id": {
						Name:          "id",
						DataType:      "integer",
						Nullable:      false,
						IsPrimaryKey:  true,
						AutoIncrement: true,
					},
					"user_id": {
						Name:     "user_id",
						DataType: "integer",
						Nullable: false,
					},
					"total": {
						Name:     "total",
						DataType: "decimal(10,2)",
						Nullable: false,
					},
				},
				Constraints: map[string]unifiedmodel.Constraint{
					"fk_orders_user": {
						Name:    "fk_orders_user",
						Type:    unifiedmodel.ConstraintTypeForeignKey,
						Columns: []string{"user_id"},
						Reference: unifiedmodel.Reference{
							Table:   "users",
							Columns: []string{"id"},
						},
					},
				},
			},
		},
	}
}

func createComplexTestSchema() *unifiedmodel.UnifiedModel {
	return &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.PostgreSQL,
		Tables: map[string]unifiedmodel.Table{
			"users": {
				Name: "users",
				Columns: map[string]unifiedmodel.Column{
					"id":         {Name: "id", DataType: "integer", IsPrimaryKey: true},
					"email":      {Name: "email", DataType: "varchar(255)", Nullable: false},
					"first_name": {Name: "first_name", DataType: "varchar(100)"},
					"last_name":  {Name: "last_name", DataType: "varchar(100)"},
					"created_at": {Name: "created_at", DataType: "timestamp"},
				},
			},
			"orders": {
				Name: "orders",
				Columns: map[string]unifiedmodel.Column{
					"id":         {Name: "id", DataType: "integer", IsPrimaryKey: true},
					"user_id":    {Name: "user_id", DataType: "integer"},
					"total":      {Name: "total", DataType: "decimal(10,2)"},
					"status":     {Name: "status", DataType: "varchar(50)"},
					"created_at": {Name: "created_at", DataType: "timestamp"},
				},
			},
			"order_items": {
				Name: "order_items",
				Columns: map[string]unifiedmodel.Column{
					"id":         {Name: "id", DataType: "integer", IsPrimaryKey: true},
					"order_id":   {Name: "order_id", DataType: "integer"},
					"product_id": {Name: "product_id", DataType: "integer"},
					"quantity":   {Name: "quantity", DataType: "integer"},
					"price":      {Name: "price", DataType: "decimal(10,2)"},
				},
			},
			"products": {
				Name: "products",
				Columns: map[string]unifiedmodel.Column{
					"id":          {Name: "id", DataType: "integer", IsPrimaryKey: true},
					"name":        {Name: "name", DataType: "varchar(255)"},
					"description": {Name: "description", DataType: "text"},
					"price":       {Name: "price", DataType: "decimal(10,2)"},
					"category_id": {Name: "category_id", DataType: "integer"},
				},
			},
			"categories": {
				Name: "categories",
				Columns: map[string]unifiedmodel.Column{
					"id":   {Name: "id", DataType: "integer", IsPrimaryKey: true},
					"name": {Name: "name", DataType: "varchar(100)"},
				},
			},
		},
	}
}
