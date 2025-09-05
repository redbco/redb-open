package cross_paradigm

import (
	"context"
	"testing"
	"time"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/translator/core"
)

func TestEnrichmentAnalyzer_AnalyzeEnrichment_WithoutEnrichment(t *testing.T) {
	analyzer := NewEnrichmentAnalyzer()

	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.MongoDB,
		RequestID:      "enrichment-test-001",
		RequestedAt:    time.Now(),
	}

	ctx := core.NewTranslationContext(context.Background(), request)

	// Set up source schema
	sourceSchema := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.PostgreSQL,
		Tables: map[string]unifiedmodel.Table{
			"users": {
				Name: "users",
				Columns: map[string]unifiedmodel.Column{
					"id":    {Name: "id", DataType: "integer", IsPrimaryKey: true},
					"email": {Name: "email", DataType: "varchar(255)"},
				},
			},
			"user_roles": {
				Name: "user_roles",
				Columns: map[string]unifiedmodel.Column{
					"user_id": {Name: "user_id", DataType: "integer"},
					"role_id": {Name: "role_id", DataType: "integer"},
				},
			},
			"roles": {
				Name: "roles",
				Columns: map[string]unifiedmodel.Column{
					"id":   {Name: "id", DataType: "integer", IsPrimaryKey: true},
					"name": {Name: "name", DataType: "varchar(50)"},
				},
			},
		},
		Constraints: map[string]unifiedmodel.Constraint{
			"fk_user_roles_user": {
				Name:    "fk_user_roles_user",
				Type:    unifiedmodel.ConstraintTypeForeignKey,
				Columns: []string{"user_id"},
				Reference: unifiedmodel.Reference{
					Table:   "users",
					Columns: []string{"id"},
				},
			},
			"fk_user_roles_role": {
				Name:    "fk_user_roles_role",
				Type:    unifiedmodel.ConstraintTypeForeignKey,
				Columns: []string{"role_id"},
				Reference: unifiedmodel.Reference{
					Table:   "roles",
					Columns: []string{"id"},
				},
			},
		},
	}
	ctx.SetSourceSchema(sourceSchema)

	enrichmentCtx, err := analyzer.AnalyzeEnrichment(ctx)
	if err != nil {
		t.Fatalf("AnalyzeEnrichment failed: %v", err)
	}

	// Verify heuristic analysis was performed
	if len(enrichmentCtx.EntityTables) == 0 {
		t.Error("Expected entity tables to be classified heuristically")
	}

	if len(enrichmentCtx.ForeignKeys) == 0 {
		t.Error("Expected foreign keys to be analyzed heuristically")
	}

	if len(enrichmentCtx.AccessPatterns) == 0 {
		t.Error("Expected access patterns to be generated")
	}

	// Check specific classifications
	if _, exists := enrichmentCtx.EntityTables["users"]; !exists {
		t.Error("Expected users table to be classified as entity")
	}

	// user_roles should be classified as junction table (has 2 foreign keys)
	if _, exists := enrichmentCtx.JunctionTables["user_roles"]; !exists {
		t.Error("Expected user_roles to be classified as junction table")
	}

	// roles should be classified as lookup table (based on naming)
	if _, exists := enrichmentCtx.LookupTables["roles"]; !exists {
		t.Error("Expected roles to be classified as lookup table")
	}
}

func TestEnrichmentAnalyzer_AnalyzeEnrichment_WithEnrichment(t *testing.T) {
	analyzer := NewEnrichmentAnalyzer()

	// Create enrichment data
	enrichment := &unifiedmodel.UnifiedModelEnrichment{
		TableEnrichments: map[string]unifiedmodel.TableEnrichment{
			"users": {
				PrimaryCategory:          unifiedmodel.TableCategoryTransactional,
				ClassificationConfidence: 0.95,
				AccessPattern:            unifiedmodel.AccessPatternReadWrite,
				RelatedTables:            []string{"orders", "user_profiles"},
				BusinessPurpose:          "User account management",
				Tags:                     []string{"entity", "primary"},
			},
			"categories": {
				PrimaryCategory:          unifiedmodel.TableCategoryReference,
				ClassificationConfidence: 0.90,
				AccessPattern:            unifiedmodel.AccessPatternReadHeavy,
				BusinessPurpose:          "Product categorization",
				Tags:                     []string{"lookup", "value_column:name", "display_column:description"},
			},
			"user_sessions": {
				PrimaryCategory:          unifiedmodel.TableCategoryMetadata,
				ClassificationConfidence: 0.85,
				AccessPattern:            unifiedmodel.AccessPatternWriteHeavy,
				RelatedTables:            []string{"users", "sessions"},
				BusinessPurpose:          "Session tracking",
			},
		},
		RelationshipEnrichments: map[string]unifiedmodel.RelationshipEnrichment{
			"user_orders": {
				Frequency:       func() *int64 { v := int64(1000); return &v }(),
				Strength:        func() *float64 { v := 0.8; return &v }(),
				BusinessMeaning: "Users place multiple orders",
				Context: map[string]string{
					"source_entity": "users",
					"target_entity": "orders",
					"cardinality":   "one_to_many",
				},
			},
		},
		PerformanceHints: []unifiedmodel.PerformanceHint{
			{
				TargetDatabase:   "mongodb",
				Category:         "indexing",
				Priority:         unifiedmodel.ConversionPriorityHigh,
				Hint:             "Create compound index on user_id and created_at",
				ObjectPath:       "tables.orders.columns.user_id",
				EstimatedBenefit: "50% query performance improvement",
			},
		},
		ComplianceSummary: unifiedmodel.ComplianceSummary{
			OverallRiskLevel:   "medium",
			RequiredFrameworks: []unifiedmodel.ComplianceFramework{unifiedmodel.ComplianceGDPR, unifiedmodel.ComplianceHIPAA},
		},
	}

	request := &core.TranslationRequest{
		SourceDatabase: dbcapabilities.PostgreSQL,
		TargetDatabase: dbcapabilities.MongoDB,
		Enrichment:     enrichment,
		RequestID:      "enrichment-test-002",
		RequestedAt:    time.Now(),
	}

	ctx := core.NewTranslationContext(context.Background(), request)

	sourceSchema := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.PostgreSQL,
		Tables: map[string]unifiedmodel.Table{
			"users":         {Name: "users"},
			"categories":    {Name: "categories"},
			"user_sessions": {Name: "user_sessions"},
		},
	}
	ctx.SetSourceSchema(sourceSchema)

	enrichmentCtx, err := analyzer.AnalyzeEnrichment(ctx)
	if err != nil {
		t.Fatalf("AnalyzeEnrichment failed: %v", err)
	}

	// Verify enrichment data was processed
	if len(enrichmentCtx.EntityTables) == 0 {
		t.Error("Expected entity tables from enrichment")
	}

	// Check users table classification
	if userEntity, exists := enrichmentCtx.EntityTables["users"]; exists {
		if userEntity.EntityType != "primary" {
			t.Errorf("Expected users to be classified as primary entity, got %s", userEntity.EntityType)
		}
		if userEntity.EmbedStrategy != "separate" {
			t.Errorf("Expected users embed strategy to be separate, got %s", userEntity.EmbedStrategy)
		}
	} else {
		t.Error("Expected users table in entity classifications")
	}

	// Check categories table classification (should be lookup)
	if categoryLookup, exists := enrichmentCtx.LookupTables["categories"]; exists {
		if categoryLookup.ConversionType != "embed" {
			t.Errorf("Expected categories conversion type to be embed, got %s", categoryLookup.ConversionType)
		}
		if categoryLookup.ValueColumn != "name" {
			t.Errorf("Expected categories value column to be name, got %s", categoryLookup.ValueColumn)
		}
		if categoryLookup.DisplayColumn != "description" {
			t.Errorf("Expected categories display column to be description, got %s", categoryLookup.DisplayColumn)
		}
	} else {
		t.Error("Expected categories table in lookup classifications")
	}

	// Check user_sessions table classification (should be junction due to metadata category and multiple related tables)
	if sessionJunction, exists := enrichmentCtx.JunctionTables["user_sessions"]; exists {
		if sessionJunction.ConversionType != "array" {
			t.Errorf("Expected user_sessions conversion type to be array, got %s", sessionJunction.ConversionType)
		}
	} else {
		t.Error("Expected user_sessions table in junction classifications")
	}

	// Check relationships
	if len(enrichmentCtx.Relationships) == 0 {
		t.Error("Expected relationships from enrichment")
	}

	if userOrdersRel, exists := enrichmentCtx.Relationships["user_orders"]; exists {
		if userOrdersRel.SourceEntity != "users" {
			t.Errorf("Expected source entity to be users, got %s", userOrdersRel.SourceEntity)
		}
		if userOrdersRel.TargetEntity != "orders" {
			t.Errorf("Expected target entity to be orders, got %s", userOrdersRel.TargetEntity)
		}
		if userOrdersRel.Strength != "strong" {
			t.Errorf("Expected relationship strength to be strong, got %s", userOrdersRel.Strength)
		}
	} else {
		t.Error("Expected user_orders relationship")
	}

	// Check performance hints
	if len(enrichmentCtx.PerformanceHints) == 0 {
		t.Error("Expected performance hints from enrichment")
	}

	// Check business rules
	if len(enrichmentCtx.BusinessRules) == 0 {
		t.Error("Expected business rules from compliance data")
	}

	// Verify compliance frameworks were converted to business rules
	if _, exists := enrichmentCtx.BusinessRules["gdpr"]; !exists {
		t.Error("Expected GDPR compliance rule")
	}

	if _, exists := enrichmentCtx.BusinessRules["hipaa"]; !exists {
		t.Error("Expected HIPAA compliance rule")
	}

	if _, exists := enrichmentCtx.BusinessRules["risk_management"]; !exists {
		t.Error("Expected risk management rule")
	}
}

func TestEnrichmentAnalyzer_ClassifyTableByStructure(t *testing.T) {
	analyzer := NewEnrichmentAnalyzer()

	tests := []struct {
		name         string
		tableName    string
		table        unifiedmodel.Table
		expectedType string
	}{
		{
			name:      "junction table with two foreign keys",
			tableName: "user_roles",
			table: unifiedmodel.Table{
				Name: "user_roles",
				Columns: map[string]unifiedmodel.Column{
					"user_id": {Name: "user_id", DataType: "integer"},
					"role_id": {Name: "role_id", DataType: "integer"},
				},
			},
			expectedType: "junction",
		},
		{
			name:      "lookup table with few columns",
			tableName: "status_types",
			table: unifiedmodel.Table{
				Name: "status_types",
				Columns: map[string]unifiedmodel.Column{
					"id":   {Name: "id", DataType: "integer", IsPrimaryKey: true},
					"name": {Name: "name", DataType: "varchar(50)"},
				},
			},
			expectedType: "lookup",
		},
		{
			name:      "entity table with many columns",
			tableName: "users",
			table: unifiedmodel.Table{
				Name: "users",
				Columns: map[string]unifiedmodel.Column{
					"id":         {Name: "id", DataType: "integer", IsPrimaryKey: true},
					"email":      {Name: "email", DataType: "varchar(255)"},
					"first_name": {Name: "first_name", DataType: "varchar(100)"},
					"last_name":  {Name: "last_name", DataType: "varchar(100)"},
					"created_at": {Name: "created_at", DataType: "timestamp"},
				},
			},
			expectedType: "entity",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := analyzer.classifyTableByStructure(tt.tableName, tt.table, &unifiedmodel.UnifiedModel{})
			if result.EntityType != tt.expectedType {
				t.Errorf("Expected entity type %s, got %s", tt.expectedType, result.EntityType)
			}
		})
	}
}

func TestEnrichmentAnalyzer_HelperFunctions(t *testing.T) {
	analyzer := NewEnrichmentAnalyzer()

	t.Run("isForeignKeyColumn", func(t *testing.T) {
		tests := []struct {
			columnName string
			expected   bool
		}{
			{"user_id", true},
			{"role_id", true},
			{"id", false}, // Plain "id" is typically a primary key, not foreign key
			{"customer_ref", true},
			{"name", false},
			{"email", false},
			{"created_at", false},
		}

		for _, tt := range tests {
			result := analyzer.isForeignKeyColumn(tt.columnName)
			if result != tt.expected {
				t.Errorf("isForeignKeyColumn(%s) = %v, want %v", tt.columnName, result, tt.expected)
			}
		}
	})

	t.Run("isLookupTableName", func(t *testing.T) {
		tests := []struct {
			tableName string
			expected  bool
		}{
			{"user_types", true},
			{"status_lookup", true},
			{"reference_data", true},
			{"categories", true},
			{"users", false},
			{"orders", false},
			{"products", false},
		}

		for _, tt := range tests {
			result := analyzer.isLookupTableName(tt.tableName)
			if result != tt.expected {
				t.Errorf("isLookupTableName(%s) = %v, want %v", tt.tableName, result, tt.expected)
			}
		}
	})

	t.Run("findValueColumnFromTags", func(t *testing.T) {
		tests := []struct {
			tags     []string
			expected string
		}{
			{[]string{"value_column:code", "other_tag"}, "code"},
			{[]string{"other_tag", "value_column:name"}, "name"},
			{[]string{"no_value_column"}, "value"},
			{[]string{}, "value"},
		}

		for _, tt := range tests {
			result := analyzer.findValueColumnFromTags(tt.tags)
			if result != tt.expected {
				t.Errorf("findValueColumnFromTags(%v) = %s, want %s", tt.tags, result, tt.expected)
			}
		}
	})

	t.Run("deriveFrequencyFromPattern", func(t *testing.T) {
		tests := []struct {
			pattern  unifiedmodel.AccessPattern
			expected string
		}{
			{unifiedmodel.AccessPatternReadHeavy, "high"},
			{unifiedmodel.AccessPatternWriteHeavy, "high"},
			{unifiedmodel.AccessPatternBatch, "low"},
			{unifiedmodel.AccessPatternRealTime, "high"},
			{unifiedmodel.AccessPatternReadWrite, "medium"},
		}

		for _, tt := range tests {
			result := analyzer.deriveFrequencyFromPattern(tt.pattern)
			if result != tt.expected {
				t.Errorf("deriveFrequencyFromPattern(%s) = %s, want %s", tt.pattern, result, tt.expected)
			}
		}
	})

	t.Run("extractObjectNameFromPath", func(t *testing.T) {
		tests := []struct {
			path     string
			expected string
		}{
			{"tables.users.columns.email", "users"},
			{"collections.products.fields.name", "products"},
			{"indexes.idx_user_email", "idx_user_email"},
			{"invalid_path", "unknown_object"},
			{"", "unknown_object"},
		}

		for _, tt := range tests {
			result := analyzer.extractObjectNameFromPath(tt.path)
			if result != tt.expected {
				t.Errorf("extractObjectNameFromPath(%s) = %s, want %s", tt.path, result, tt.expected)
			}
		}
	})
}

func TestEnrichmentAnalyzer_EdgeCases(t *testing.T) {
	analyzer := NewEnrichmentAnalyzer()

	t.Run("empty enrichment", func(t *testing.T) {
		request := &core.TranslationRequest{
			SourceDatabase: dbcapabilities.PostgreSQL,
			TargetDatabase: dbcapabilities.MongoDB,
			Enrichment:     &unifiedmodel.UnifiedModelEnrichment{}, // Empty enrichment
			RequestID:      "enrichment-test-003",
			RequestedAt:    time.Now(),
		}

		ctx := core.NewTranslationContext(context.Background(), request)
		ctx.SetSourceSchema(&unifiedmodel.UnifiedModel{DatabaseType: dbcapabilities.PostgreSQL})

		enrichmentCtx, err := analyzer.AnalyzeEnrichment(ctx)
		if err != nil {
			t.Fatalf("AnalyzeEnrichment failed: %v", err)
		}

		// Should still work with empty enrichment
		if enrichmentCtx == nil {
			t.Error("Expected enrichment context to be created")
		}
	})

	t.Run("nil source schema", func(t *testing.T) {
		request := &core.TranslationRequest{
			SourceDatabase: dbcapabilities.PostgreSQL,
			TargetDatabase: dbcapabilities.MongoDB,
			RequestID:      "enrichment-test-004",
			RequestedAt:    time.Now(),
		}

		ctx := core.NewTranslationContext(context.Background(), request)
		// Don't set source schema

		enrichmentCtx, err := analyzer.AnalyzeEnrichment(ctx)
		if err != nil {
			t.Fatalf("AnalyzeEnrichment failed: %v", err)
		}

		// Should handle nil source schema gracefully
		if enrichmentCtx == nil {
			t.Error("Expected enrichment context to be created")
		}
	})
}
