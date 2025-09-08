package matching

import (
	"testing"

	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

func TestNewUnifiedModelMatcher(t *testing.T) {
	matcher := NewUnifiedModelMatcher()

	if matcher == nil {
		t.Fatal("NewUnifiedModelMatcher should not return nil")
	}
}

func TestDefaultUnifiedMatchOptions(t *testing.T) {
	options := DefaultUnifiedMatchOptions()

	if options.NameSimilarityThreshold != 0.3 {
		t.Errorf("Expected NameSimilarityThreshold 0.3, got %f", options.NameSimilarityThreshold)
	}

	if options.PoorMatchThreshold != 0.4 {
		t.Errorf("Expected PoorMatchThreshold 0.4, got %f", options.PoorMatchThreshold)
	}

	if options.NameWeight != 0.4 {
		t.Errorf("Expected NameWeight 0.4, got %f", options.NameWeight)
	}

	if !options.EnableCrossTableMatching {
		t.Error("Expected EnableCrossTableMatching to be true")
	}
}

func TestMatchUnifiedModels_NilModels(t *testing.T) {
	matcher := NewUnifiedModelMatcher()

	// Test nil source model
	_, err := matcher.MatchUnifiedModels(nil, nil, &unifiedmodel.UnifiedModel{}, nil, nil)
	if err == nil {
		t.Error("Expected error when source model is nil")
	}

	// Test nil target model
	_, err = matcher.MatchUnifiedModels(&unifiedmodel.UnifiedModel{}, nil, nil, nil, nil)
	if err == nil {
		t.Error("Expected error when target model is nil")
	}
}

func TestMatchUnifiedModels_EmptyModels(t *testing.T) {
	matcher := NewUnifiedModelMatcher()

	sourceModel := &unifiedmodel.UnifiedModel{
		Tables: map[string]unifiedmodel.Table{},
	}

	targetModel := &unifiedmodel.UnifiedModel{
		Tables: map[string]unifiedmodel.Table{},
	}

	result, err := matcher.MatchUnifiedModels(sourceModel, nil, targetModel, nil, nil)
	if err != nil {
		t.Fatalf("MatchUnifiedModels should not fail with empty models: %v", err)
	}

	if result == nil {
		t.Fatal("Result should not be nil")
	}

	if len(result.TableMatches) != 0 {
		t.Errorf("Expected 0 table matches, got %d", len(result.TableMatches))
	}

	if result.OverallSimilarityScore != 0.0 {
		t.Errorf("Expected overall similarity score 0.0, got %f", result.OverallSimilarityScore)
	}
}

func TestMatchUnifiedModels_IdenticalTables(t *testing.T) {
	matcher := NewUnifiedModelMatcher()

	// Create identical models
	table := unifiedmodel.Table{
		Name: "users",
		Columns: map[string]unifiedmodel.Column{
			"id": {
				Name:         "id",
				DataType:     "integer",
				IsPrimaryKey: true,
			},
			"email": {
				Name:     "email",
				DataType: "varchar",
			},
		},
	}

	sourceModel := &unifiedmodel.UnifiedModel{
		Tables: map[string]unifiedmodel.Table{
			"users": table,
		},
	}

	targetModel := &unifiedmodel.UnifiedModel{
		Tables: map[string]unifiedmodel.Table{
			"users": table,
		},
	}

	result, err := matcher.MatchUnifiedModels(sourceModel, nil, targetModel, nil, nil)
	if err != nil {
		t.Fatalf("MatchUnifiedModels failed: %v", err)
	}

	if len(result.TableMatches) != 1 {
		t.Errorf("Expected 1 table match, got %d", len(result.TableMatches))
	}

	tableMatch := result.TableMatches[0]
	if tableMatch.SourceTable != "users" || tableMatch.TargetTable != "users" {
		t.Errorf("Expected users->users match, got %s->%s", tableMatch.SourceTable, tableMatch.TargetTable)
	}

	if tableMatch.Score <= 0.5 {
		t.Errorf("Expected high similarity score for identical tables, got %f", tableMatch.Score)
	}

	if tableMatch.MatchedColumns != 2 {
		t.Errorf("Expected 2 matched columns, got %d", tableMatch.MatchedColumns)
	}

	if result.OverallSimilarityScore <= 0.5 {
		t.Errorf("Expected high overall similarity score, got %f", result.OverallSimilarityScore)
	}
}

func TestMatchUnifiedModels_WithEnrichments(t *testing.T) {
	matcher := NewUnifiedModelMatcher()

	// Create models with different table names but similar structure
	sourceTable := unifiedmodel.Table{
		Name: "user_accounts",
		Columns: map[string]unifiedmodel.Column{
			"user_id": {
				Name:         "user_id",
				DataType:     "integer",
				IsPrimaryKey: true,
			},
			"email_address": {
				Name:     "email_address",
				DataType: "varchar",
			},
		},
	}

	targetTable := unifiedmodel.Table{
		Name: "users",
		Columns: map[string]unifiedmodel.Column{
			"id": {
				Name:         "id",
				DataType:     "integer",
				IsPrimaryKey: true,
			},
			"email": {
				Name:     "email",
				DataType: "varchar",
			},
		},
	}

	sourceModel := &unifiedmodel.UnifiedModel{
		Tables: map[string]unifiedmodel.Table{
			"user_accounts": sourceTable,
		},
	}

	targetModel := &unifiedmodel.UnifiedModel{
		Tables: map[string]unifiedmodel.Table{
			"users": targetTable,
		},
	}

	// Create enrichments
	sourceEnrichment := &unifiedmodel.UnifiedModelEnrichment{
		TableEnrichments: map[string]unifiedmodel.TableEnrichment{
			"user_accounts": {
				PrimaryCategory:          unifiedmodel.TableCategoryTransactional,
				ClassificationConfidence: 0.9,
				AccessPattern:            unifiedmodel.AccessPatternReadWrite,
			},
		},
		ColumnEnrichments: map[string]unifiedmodel.ColumnEnrichment{
			"user_accounts.email_address": {
				IsPrivilegedData:     true,
				DataCategory:         unifiedmodel.DataCategoryEmail,
				PrivilegedConfidence: 0.95,
				RiskLevel:            unifiedmodel.RiskLevelHigh,
			},
		},
	}

	targetEnrichment := &unifiedmodel.UnifiedModelEnrichment{
		TableEnrichments: map[string]unifiedmodel.TableEnrichment{
			"users": {
				PrimaryCategory:          unifiedmodel.TableCategoryTransactional,
				ClassificationConfidence: 0.85,
				AccessPattern:            unifiedmodel.AccessPatternReadWrite,
			},
		},
		ColumnEnrichments: map[string]unifiedmodel.ColumnEnrichment{
			"users.email": {
				IsPrivilegedData:     true,
				DataCategory:         unifiedmodel.DataCategoryEmail,
				PrivilegedConfidence: 0.9,
				RiskLevel:            unifiedmodel.RiskLevelHigh,
			},
		},
	}

	result, err := matcher.MatchUnifiedModels(sourceModel, sourceEnrichment, targetModel, targetEnrichment, nil)
	if err != nil {
		t.Fatalf("MatchUnifiedModels failed: %v", err)
	}

	if len(result.TableMatches) != 1 {
		t.Errorf("Expected 1 table match, got %d", len(result.TableMatches))
	}

	tableMatch := result.TableMatches[0]
	if tableMatch.ClassificationMatch != "transactional" {
		t.Errorf("Expected classification match 'transactional', got '%s'", tableMatch.ClassificationMatch)
	}

	// Check column matches
	emailMatch := false
	for _, colMatch := range tableMatch.ColumnMatches {
		if colMatch.SourceColumn == "email_address" && colMatch.TargetColumn == "email" {
			emailMatch = true
			if !colMatch.PrivilegedDataMatch {
				t.Error("Expected privileged data match for email columns")
			}
			if colMatch.DataCategoryMatch != "email" {
				t.Errorf("Expected data category match 'email', got '%s'", colMatch.DataCategoryMatch)
			}
		}
	}

	if !emailMatch {
		t.Error("Expected email column match")
	}
}

func TestMatchUnifiedModels_DifferentStructures(t *testing.T) {
	matcher := NewUnifiedModelMatcher()

	// Source: simple user table
	sourceModel := &unifiedmodel.UnifiedModel{
		Tables: map[string]unifiedmodel.Table{
			"users": {
				Name: "users",
				Columns: map[string]unifiedmodel.Column{
					"id":   {Name: "id", DataType: "integer"},
					"name": {Name: "name", DataType: "varchar"},
				},
			},
		},
	}

	// Target: complex user table with more columns
	targetModel := &unifiedmodel.UnifiedModel{
		Tables: map[string]unifiedmodel.Table{
			"user_profiles": {
				Name: "user_profiles",
				Columns: map[string]unifiedmodel.Column{
					"user_id":    {Name: "user_id", DataType: "integer"},
					"first_name": {Name: "first_name", DataType: "varchar"},
					"last_name":  {Name: "last_name", DataType: "varchar"},
					"email":      {Name: "email", DataType: "varchar"},
					"created_at": {Name: "created_at", DataType: "timestamp"},
				},
			},
		},
	}

	result, err := matcher.MatchUnifiedModels(sourceModel, nil, targetModel, nil, nil)
	if err != nil {
		t.Fatalf("MatchUnifiedModels failed: %v", err)
	}

	if len(result.TableMatches) != 1 {
		t.Errorf("Expected 1 table match, got %d", len(result.TableMatches))
	}

	tableMatch := result.TableMatches[0]
	if tableMatch.TotalSourceColumns != 2 {
		t.Errorf("Expected 2 source columns, got %d", tableMatch.TotalSourceColumns)
	}

	if tableMatch.TotalTargetColumns != 5 {
		t.Errorf("Expected 5 target columns, got %d", tableMatch.TotalTargetColumns)
	}

	// Should have some matches but not perfect due to structure differences
	if tableMatch.Score >= 1.0 {
		t.Errorf("Expected imperfect match due to structure differences, got score %f", tableMatch.Score)
	}
}

func TestCalculateStringSimilarity(t *testing.T) {
	matcher := NewUnifiedModelMatcher()

	// Identical strings
	score := matcher.calculateStringSimilarity("users", "users")
	if score != 1.0 {
		t.Errorf("Expected similarity 1.0 for identical strings, got %f", score)
	}

	// Substring match
	score = matcher.calculateStringSimilarity("user", "users")
	if score <= 0.0 {
		t.Errorf("Expected positive similarity for substring match, got %f", score)
	}

	// No match
	score = matcher.calculateStringSimilarity("users", "products")
	if score != 0.0 {
		t.Errorf("Expected similarity 0.0 for unrelated strings, got %f", score)
	}

	// Case insensitive
	score = matcher.calculateStringSimilarity("Users", "USERS")
	if score != 1.0 {
		t.Errorf("Expected similarity 1.0 for case-insensitive match, got %f", score)
	}
}

func TestAreTypesCompatible(t *testing.T) {
	matcher := NewUnifiedModelMatcher()

	// Identical types
	if !matcher.areTypesCompatible("integer", "integer") {
		t.Error("Expected identical types to be compatible")
	}

	// Integer family
	if !matcher.areTypesCompatible("integer", "int") {
		t.Error("Expected integer types to be compatible")
	}

	if !matcher.areTypesCompatible("int", "bigint") {
		t.Error("Expected integer types to be compatible")
	}

	// Float family
	if !matcher.areTypesCompatible("float", "double") {
		t.Error("Expected float types to be compatible")
	}

	if !matcher.areTypesCompatible("decimal", "numeric") {
		t.Error("Expected decimal types to be compatible")
	}

	// String family
	if !matcher.areTypesCompatible("varchar", "text") {
		t.Error("Expected string types to be compatible")
	}

	if !matcher.areTypesCompatible("char", "character") {
		t.Error("Expected character types to be compatible")
	}

	// Date family
	if !matcher.areTypesCompatible("date", "datetime") {
		t.Error("Expected date types to be compatible")
	}

	if !matcher.areTypesCompatible("timestamp", "timestamptz") {
		t.Error("Expected timestamp types to be compatible")
	}

	// Cross-family compatibility (integer and float)
	if !matcher.areTypesCompatible("integer", "float") {
		t.Error("Expected integer and float to be somewhat compatible")
	}

	// Incompatible types
	if matcher.areTypesCompatible("integer", "varchar") {
		t.Error("Expected integer and varchar to be incompatible")
	}

	if matcher.areTypesCompatible("date", "boolean") {
		t.Error("Expected date and boolean to be incompatible")
	}
}

func TestCalculateStructureSimilarity(t *testing.T) {
	matcher := NewUnifiedModelMatcher()

	// Identical structures
	table1 := unifiedmodel.Table{
		Columns: map[string]unifiedmodel.Column{
			"id":   {DataType: "integer"},
			"name": {DataType: "varchar"},
		},
	}

	table2 := unifiedmodel.Table{
		Columns: map[string]unifiedmodel.Column{
			"user_id":   {DataType: "integer"},
			"user_name": {DataType: "varchar"},
		},
	}

	score := matcher.calculateStructureSimilarity(table1, table2)
	if score != 1.0 {
		t.Errorf("Expected perfect structure similarity, got %f", score)
	}

	// Different column counts
	table3 := unifiedmodel.Table{
		Columns: map[string]unifiedmodel.Column{
			"id":    {DataType: "integer"},
			"name":  {DataType: "varchar"},
			"email": {DataType: "varchar"},
		},
	}

	score = matcher.calculateStructureSimilarity(table1, table3)
	if score >= 1.0 {
		t.Errorf("Expected imperfect similarity for different column counts, got %f", score)
	}

	// Empty tables
	emptyTable1 := unifiedmodel.Table{Columns: map[string]unifiedmodel.Column{}}
	emptyTable2 := unifiedmodel.Table{Columns: map[string]unifiedmodel.Column{}}

	score = matcher.calculateStructureSimilarity(emptyTable1, emptyTable2)
	if score != 1.0 {
		t.Errorf("Expected perfect similarity for empty tables, got %f", score)
	}
}

func TestCalculateClassificationSimilarity(t *testing.T) {
	matcher := NewUnifiedModelMatcher()

	// Identical classifications
	enrichment1 := &unifiedmodel.TableEnrichment{
		PrimaryCategory:          unifiedmodel.TableCategoryTransactional,
		ClassificationConfidence: 0.9,
		AccessPattern:            unifiedmodel.AccessPatternReadWrite,
	}

	enrichment2 := &unifiedmodel.TableEnrichment{
		PrimaryCategory:          unifiedmodel.TableCategoryTransactional,
		ClassificationConfidence: 0.9,
		AccessPattern:            unifiedmodel.AccessPatternReadWrite,
	}

	score := matcher.calculateClassificationSimilarity(enrichment1, enrichment2)
	if score != 1.0 {
		t.Errorf("Expected perfect classification similarity, got %f", score)
	}

	// Different categories
	enrichment3 := &unifiedmodel.TableEnrichment{
		PrimaryCategory:          unifiedmodel.TableCategoryAnalytical,
		ClassificationConfidence: 0.8,
		AccessPattern:            unifiedmodel.AccessPatternReadHeavy,
	}

	score = matcher.calculateClassificationSimilarity(enrichment1, enrichment3)
	if score >= 1.0 {
		t.Errorf("Expected imperfect similarity for different classifications, got %f", score)
	}
}

func TestCalculatePrivilegedDataSimilarity(t *testing.T) {
	matcher := NewUnifiedModelMatcher()

	// Identical privileged data info
	enrichment1 := unifiedmodel.ColumnEnrichment{
		IsPrivilegedData: true,
		DataCategory:     unifiedmodel.DataCategoryEmail,
		RiskLevel:        unifiedmodel.RiskLevelHigh,
	}

	enrichment2 := unifiedmodel.ColumnEnrichment{
		IsPrivilegedData: true,
		DataCategory:     unifiedmodel.DataCategoryEmail,
		RiskLevel:        unifiedmodel.RiskLevelHigh,
	}

	score := matcher.calculatePrivilegedDataSimilarity(enrichment1, enrichment2)
	if score != 1.0 {
		t.Errorf("Expected perfect privileged data similarity, got %f", score)
	}

	// Different data categories
	enrichment3 := unifiedmodel.ColumnEnrichment{
		IsPrivilegedData: true,
		DataCategory:     unifiedmodel.DataCategoryPhone,
		RiskLevel:        unifiedmodel.RiskLevelMedium,
	}

	score = matcher.calculatePrivilegedDataSimilarity(enrichment1, enrichment3)
	if score >= 1.0 {
		t.Errorf("Expected imperfect similarity for different privileged data, got %f", score)
	}

	// Non-privileged data
	enrichment4 := unifiedmodel.ColumnEnrichment{
		IsPrivilegedData: false,
		DataCategory:     unifiedmodel.DataCategoryBusiness,
		RiskLevel:        unifiedmodel.RiskLevelMinimal,
	}

	enrichment5 := unifiedmodel.ColumnEnrichment{
		IsPrivilegedData: false,
		DataCategory:     unifiedmodel.DataCategoryBusiness,
		RiskLevel:        unifiedmodel.RiskLevelMinimal,
	}

	score = matcher.calculatePrivilegedDataSimilarity(enrichment4, enrichment5)
	if score != 1.0 {
		t.Errorf("Expected perfect similarity for matching non-privileged data, got %f", score)
	}
}

func TestCalculateOverallSimilarity(t *testing.T) {
	matcher := NewUnifiedModelMatcher()

	// No matches
	score := matcher.calculateOverallSimilarity([]UnifiedTableMatch{}, 2, 2)
	if score != 0.0 {
		t.Errorf("Expected 0.0 similarity for no matches, got %f", score)
	}

	// Perfect matches
	matches := []UnifiedTableMatch{
		{Score: 1.0},
		{Score: 1.0},
	}

	score = matcher.calculateOverallSimilarity(matches, 2, 2)
	if score != 1.0 {
		t.Errorf("Expected 1.0 similarity for perfect matches, got %f", score)
	}

	// Partial matches with unmatched tables
	matches = []UnifiedTableMatch{
		{Score: 0.8},
	}

	score = matcher.calculateOverallSimilarity(matches, 2, 2)
	if score >= 0.8 {
		t.Errorf("Expected penalty for unmatched tables, got %f", score)
	}
}
