package classifier

import (
	"testing"

	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

func TestNewTableClassifier(t *testing.T) {
	classifier := NewTableClassifier()

	if classifier == nil {
		t.Fatal("NewTableClassifier should not return nil")
	}

	if classifier.service == nil {
		t.Error("Expected service to be initialized")
	}
}

func TestNewTableClassifierWithConfig(t *testing.T) {
	classifier := NewTableClassifierWithConfig(nil)

	if classifier == nil {
		t.Fatal("NewTableClassifierWithConfig should not return nil")
	}

	if classifier.service == nil {
		t.Error("Expected service to be initialized")
	}
}

func TestClassifyUnifiedModel_NilModel(t *testing.T) {
	classifier := NewTableClassifier()
	result, err := classifier.ClassifyUnifiedModel(nil, nil)

	if err == nil {
		t.Error("Expected error when passing nil model")
	}

	if result != nil {
		t.Error("Expected nil result when passing nil model")
	}

	expectedError := "unified model cannot be nil"
	if err.Error() != expectedError {
		t.Errorf("Expected error message '%s', got '%s'", expectedError, err.Error())
	}
}

func TestClassifyUnifiedModel_EmptyModel(t *testing.T) {
	testModel := &unifiedmodel.UnifiedModel{
		Tables: map[string]unifiedmodel.Table{},
	}

	classifier := NewTableClassifier()
	result, err := classifier.ClassifyUnifiedModel(testModel, nil)

	if err != nil {
		t.Fatalf("Classification should not fail with empty model: %v", err)
	}

	if result == nil {
		t.Fatal("Result should not be nil")
	}

	if len(result) != 0 {
		t.Error("Expected no enrichments for empty model")
	}
}

func TestClassifyUnifiedModel_SingleTable(t *testing.T) {
	testModel := &unifiedmodel.UnifiedModel{
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
						DataType: "varchar",
						Nullable: false,
					},
					"first_name": {
						Name:     "first_name",
						DataType: "varchar",
						Nullable: true,
					},
					"created_at": {
						Name:     "created_at",
						DataType: "timestamp",
						Nullable: false,
					},
				},
				Indexes: map[string]unifiedmodel.Index{
					"idx_users_email": {
						Name:    "idx_users_email",
						Columns: []string{"email"},
						Unique:  true,
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
		},
	}

	classifier := NewTableClassifier()
	result, err := classifier.ClassifyUnifiedModel(testModel, nil)

	if err != nil {
		t.Fatalf("Classification failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result should not be nil")
	}

	if len(result) != 1 {
		t.Errorf("Expected 1 enrichment, got %d", len(result))
	}

	enrichment := result[0]

	// Verify enrichment has required fields
	if enrichment.PrimaryCategory == "" {
		t.Error("Expected primary category to be set")
	}

	if enrichment.ClassificationConfidence <= 0 {
		t.Error("Expected positive classification confidence")
	}

	if len(enrichment.ClassificationScores) == 0 {
		t.Error("Expected classification scores to be populated")
	}

	// Verify scores have required fields
	for i, score := range enrichment.ClassificationScores {
		if score.Category == "" {
			t.Errorf("Score %d should have category", i)
		}
		if score.Score < 0 || score.Score > 1 {
			t.Errorf("Score %d should be between 0 and 1, got %f", i, score.Score)
		}
	}
}

func TestClassifyUnifiedModel_MultipleTables(t *testing.T) {
	testModel := &unifiedmodel.UnifiedModel{
		Tables: map[string]unifiedmodel.Table{
			"users": {
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
			},
			"orders": {
				Name: "orders",
				Columns: map[string]unifiedmodel.Column{
					"id": {
						Name:         "id",
						DataType:     "integer",
						IsPrimaryKey: true,
					},
					"user_id": {
						Name:     "user_id",
						DataType: "integer",
					},
					"total": {
						Name:     "total",
						DataType: "decimal",
					},
					"created_at": {
						Name:     "created_at",
						DataType: "timestamp",
					},
				},
			},
			"products": {
				Name: "products",
				Columns: map[string]unifiedmodel.Column{
					"id": {
						Name:         "id",
						DataType:     "integer",
						IsPrimaryKey: true,
					},
					"name": {
						Name:     "name",
						DataType: "varchar",
					},
					"description": {
						Name:     "description",
						DataType: "text",
					},
					"price": {
						Name:     "price",
						DataType: "decimal",
					},
				},
			},
		},
	}

	classifier := NewTableClassifier()
	result, err := classifier.ClassifyUnifiedModel(testModel, nil)

	if err != nil {
		t.Fatalf("Classification failed: %v", err)
	}

	if len(result) != 3 {
		t.Errorf("Expected 3 enrichments, got %d", len(result))
	}

	// Verify each enrichment has valid data
	for i, enrichment := range result {
		if enrichment.PrimaryCategory == "" {
			t.Errorf("Enrichment %d should have primary category", i)
		}

		if enrichment.ClassificationConfidence <= 0 {
			t.Errorf("Enrichment %d should have positive confidence", i)
		}

		if len(enrichment.ClassificationScores) == 0 {
			t.Errorf("Enrichment %d should have classification scores", i)
		}
	}
}

func TestClassifyUnifiedModel_WithOptions(t *testing.T) {
	testModel := &unifiedmodel.UnifiedModel{
		Tables: map[string]unifiedmodel.Table{
			"test_table": {
				Name: "test_table",
				Columns: map[string]unifiedmodel.Column{
					"id": {
						Name:         "id",
						DataType:     "integer",
						IsPrimaryKey: true,
					},
					"data": {
						Name:     "data",
						DataType: "varchar",
					},
				},
			},
		},
	}

	options := &ClassificationOptions{
		TopN:      5,
		Threshold: 0.05,
	}

	classifier := NewTableClassifier()
	result, err := classifier.ClassifyUnifiedModel(testModel, options)

	if err != nil {
		t.Fatalf("Classification failed: %v", err)
	}

	if len(result) != 1 {
		t.Errorf("Expected 1 enrichment, got %d", len(result))
	}

	enrichment := result[0]

	// With lower threshold and higher TopN, we might get more scores
	if len(enrichment.ClassificationScores) == 0 {
		t.Error("Expected classification scores with custom options")
	}
}

func TestClassifyTable_BasicMetadata(t *testing.T) {
	metadata := TableMetadata{
		Name: "users",
		Columns: []ColumnMetadata{
			{
				Name:         "id",
				DataType:     "integer",
				IsPrimaryKey: true,
				IsNullable:   false,
			},
			{
				Name:       "email",
				DataType:   "varchar",
				IsNullable: false,
			},
			{
				Name:       "name",
				DataType:   "varchar",
				IsNullable: true,
			},
		},
		Indexes: []IndexMetadata{
			{
				Name:     "idx_users_email",
				Columns:  []string{"email"},
				IsUnique: true,
			},
		},
		Constraints: []string{"PRIMARY KEY pk_users"},
		Tags:        map[string]string{"purpose": "user_management"},
	}

	classifier := NewTableClassifier()
	result, err := classifier.ClassifyTable(metadata, nil)

	if err != nil {
		t.Fatalf("Classification failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result should not be nil")
	}

	if result.TableName != "users" {
		t.Errorf("Expected table name 'users', got '%s'", result.TableName)
	}

	if result.PrimaryCategory == "" {
		t.Error("Expected primary category to be set")
	}

	if result.Confidence <= 0 {
		t.Error("Expected positive confidence")
	}

	if len(result.Scores) == 0 {
		t.Error("Expected classification scores")
	}

	// Verify scores are properly structured
	for i, score := range result.Scores {
		if score.Category == "" {
			t.Errorf("Score %d should have category", i)
		}
		if score.Score < 0 || score.Score > 1 {
			t.Errorf("Score %d should be between 0 and 1, got %f", i, score.Score)
		}
		if score.Reason == "" {
			t.Errorf("Score %d should have reason", i)
		}
	}
}

func TestClassifyTables_MultipleMetadata(t *testing.T) {
	tables := []TableMetadata{
		{
			Name: "users",
			Columns: []ColumnMetadata{
				{
					Name:         "id",
					DataType:     "integer",
					IsPrimaryKey: true,
				},
				{
					Name:     "email",
					DataType: "varchar",
				},
			},
		},
		{
			Name: "orders",
			Columns: []ColumnMetadata{
				{
					Name:         "id",
					DataType:     "integer",
					IsPrimaryKey: true,
				},
				{
					Name:     "total",
					DataType: "decimal",
				},
			},
		},
	}

	classifier := NewTableClassifier()
	results, err := classifier.ClassifyTables(tables, nil)

	if err != nil {
		t.Fatalf("Classification failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	// Verify each result
	for i, result := range results {
		if result.TableName == "" {
			t.Errorf("Result %d should have table name", i)
		}
		if result.PrimaryCategory == "" {
			t.Errorf("Result %d should have primary category", i)
		}
		if result.Confidence <= 0 {
			t.Errorf("Result %d should have positive confidence", i)
		}
	}
}

func TestDefaultClassificationOptions(t *testing.T) {
	options := DefaultClassificationOptions()

	if options.TopN != 3 {
		t.Errorf("Expected default TopN to be 3, got %d", options.TopN)
	}

	if options.Threshold != 0.1 {
		t.Errorf("Expected default Threshold to be 0.1, got %f", options.Threshold)
	}
}

func TestConvenienceFunctions(t *testing.T) {
	// Test ClassifyTable convenience function
	metadata := TableMetadata{
		Name: "test_table",
		Columns: []ColumnMetadata{
			{
				Name:         "id",
				DataType:     "integer",
				IsPrimaryKey: true,
			},
		},
	}

	result, err := ClassifyTable(metadata, nil)
	if err != nil {
		t.Fatalf("ClassifyTable convenience function failed: %v", err)
	}
	if result == nil {
		t.Error("ClassifyTable should return result")
	}

	// Test ClassifyTables convenience function
	tables := []TableMetadata{metadata}
	results, err := ClassifyTables(tables, nil)
	if err != nil {
		t.Fatalf("ClassifyTables convenience function failed: %v", err)
	}
	if len(results) != 1 {
		t.Error("ClassifyTables should return one result")
	}

	// Test ClassifyUnifiedModel convenience function
	model := &unifiedmodel.UnifiedModel{
		Tables: map[string]unifiedmodel.Table{
			"test_table": {
				Name: "test_table",
				Columns: map[string]unifiedmodel.Column{
					"id": {
						Name:         "id",
						DataType:     "integer",
						IsPrimaryKey: true,
					},
				},
			},
		},
	}

	enrichments, err := ClassifyUnifiedModel(model, nil)
	if err != nil {
		t.Fatalf("ClassifyUnifiedModel convenience function failed: %v", err)
	}
	if len(enrichments) != 1 {
		t.Error("ClassifyUnifiedModel should return one enrichment")
	}
}

func TestConvertTableToMetadata(t *testing.T) {
	classifier := NewTableClassifier()

	table := unifiedmodel.Table{
		Name: "test_table",
		Columns: map[string]unifiedmodel.Column{
			"id": {
				Name:          "id",
				DataType:      "integer",
				Nullable:      false,
				IsPrimaryKey:  true,
				AutoIncrement: true,
				Default:       "nextval('seq')",
			},
			"name": {
				Name:     "name",
				DataType: "varchar",
				Nullable: true,
			},
		},
		Indexes: map[string]unifiedmodel.Index{
			"idx_test": {
				Name:    "idx_test",
				Columns: []string{"name"},
				Unique:  true,
			},
		},
		Constraints: map[string]unifiedmodel.Constraint{
			"pk_test": {
				Name:    "pk_test",
				Type:    unifiedmodel.ConstraintTypePrimaryKey,
				Columns: []string{"id"},
			},
		},
	}

	metadata := classifier.convertTableToMetadata(table)

	if metadata.Name != "test_table" {
		t.Errorf("Expected name 'test_table', got '%s'", metadata.Name)
	}

	if len(metadata.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(metadata.Columns))
	}

	// Find ID column (order is not guaranteed in map iteration)
	var idCol *ColumnMetadata
	var nameCol *ColumnMetadata
	for i := range metadata.Columns {
		if metadata.Columns[i].Name == "id" {
			idCol = &metadata.Columns[i]
		} else if metadata.Columns[i].Name == "name" {
			nameCol = &metadata.Columns[i]
		}
	}

	if idCol == nil {
		t.Fatal("Expected to find 'id' column")
	}
	if nameCol == nil {
		t.Fatal("Expected to find 'name' column")
	}

	// Check ID column
	if idCol.DataType != "integer" {
		t.Errorf("Expected data type 'integer', got '%s'", idCol.DataType)
	}
	if idCol.IsNullable {
		t.Error("Expected id column to be not nullable")
	}
	if !idCol.IsPrimaryKey {
		t.Error("Expected id column to be primary key")
	}
	if !idCol.IsAutoIncrement {
		t.Error("Expected id column to be auto increment")
	}
	if idCol.ColumnDefault == nil || *idCol.ColumnDefault != "nextval('seq')" {
		t.Error("Expected id column to have default value")
	}

	// Check name column
	if nameCol.DataType != "varchar" {
		t.Errorf("Expected name column data type 'varchar', got '%s'", nameCol.DataType)
	}
	if !nameCol.IsNullable {
		t.Error("Expected name column to be nullable")
	}

	// Check indexes
	if len(metadata.Indexes) != 1 {
		t.Errorf("Expected 1 index, got %d", len(metadata.Indexes))
	}

	idx := metadata.Indexes[0]
	if idx.Name != "idx_test" {
		t.Errorf("Expected index name 'idx_test', got '%s'", idx.Name)
	}
	if !idx.IsUnique {
		t.Error("Expected index to be unique")
	}

	// Check constraints
	if len(metadata.Constraints) != 1 {
		t.Errorf("Expected 1 constraint, got %d", len(metadata.Constraints))
	}

	constraint := metadata.Constraints[0]
	if constraint != "primary_key pk_test" {
		t.Errorf("Expected constraint 'primary_key pk_test', got '%s'", constraint)
	}
}

func TestConvertResultToEnrichment(t *testing.T) {
	classifier := NewTableClassifier()

	result := &ClassificationResult{
		TableName:       "test_table",
		PrimaryCategory: "transactional",
		Confidence:      0.85,
		Scores: []CategoryScore{
			{
				Category: "transactional",
				Score:    0.85,
				Reason:   "Has primary key and foreign keys",
			},
			{
				Category: "analytical",
				Score:    0.15,
				Reason:   "Few aggregation patterns",
			},
		},
	}

	enrichment := classifier.convertResultToEnrichment(result)

	if enrichment.PrimaryCategory != unifiedmodel.TableCategory("transactional") {
		t.Errorf("Expected primary category 'transactional', got '%s'", enrichment.PrimaryCategory)
	}

	if enrichment.ClassificationConfidence != 0.85 {
		t.Errorf("Expected confidence 0.85, got %f", enrichment.ClassificationConfidence)
	}

	if len(enrichment.ClassificationScores) != 2 {
		t.Errorf("Expected 2 scores, got %d", len(enrichment.ClassificationScores))
	}

	// Check first score
	score := enrichment.ClassificationScores[0]
	if score.Category != "transactional" {
		t.Errorf("Expected category 'transactional', got '%s'", score.Category)
	}
	if score.Score != 0.85 {
		t.Errorf("Expected score 0.85, got %f", score.Score)
	}
	if score.Reason != "Has primary key and foreign keys" {
		t.Errorf("Expected reason 'Has primary key and foreign keys', got '%s'", score.Reason)
	}

	// Check default values
	if enrichment.AccessPattern != unifiedmodel.AccessPatternReadWrite {
		t.Errorf("Expected default access pattern, got '%s'", enrichment.AccessPattern)
	}

	if enrichment.HasPrivilegedData {
		t.Error("Expected HasPrivilegedData to be false by default")
	}

	if enrichment.DataSensitivity != 0.0 {
		t.Errorf("Expected DataSensitivity to be 0.0, got %f", enrichment.DataSensitivity)
	}
}
