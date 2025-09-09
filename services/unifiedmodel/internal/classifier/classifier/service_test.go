package classifier

import (
	"context"
	"testing"

	pb "github.com/redbco/redb-open/api/proto/unifiedmodel/v1"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

func TestService_ClassifyUnifiedModel(t *testing.T) {
	service := NewService()

	// Create a test protobuf unified model
	testModel := &pb.UnifiedModel{
		DatabaseType: string(dbcapabilities.PostgreSQL),
		Tables: map[string]*pb.Table{
			"users": {
				Name: "users",
				Columns: map[string]*pb.Column{
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
				Indexes: map[string]*pb.Index{
					"idx_users_email": {
						Name:    "idx_users_email",
						Columns: []string{"email"},
						Unique:  true,
					},
				},
				Constraints: map[string]*pb.Constraint{
					"pk_users": {
						Name:    "pk_users",
						Type:    string(unifiedmodel.ConstraintTypePrimaryKey),
						Columns: []string{"id"},
					},
				},
			},
			"orders": {
				Name: "orders",
				Columns: map[string]*pb.Column{
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
		},
	}

	// Create request
	req := &pb.ClassifyUnifiedModelRequest{
		UnifiedModel: testModel,
	}

	// Call the service
	resp, err := service.ClassifyUnifiedModel(context.Background(), req)
	if err != nil {
		t.Fatalf("ClassifyUnifiedModel failed: %v", err)
	}

	// Verify response
	if resp == nil {
		t.Fatal("Response should not be nil")
	}

	if resp.UnifiedModelEnrichment == nil {
		t.Fatal("UnifiedModelEnrichment should not be nil")
	}

	// Verify enrichment structure
	enrichment := resp.UnifiedModelEnrichment
	if enrichment.SchemaId == "" {
		t.Error("SchemaID should be set")
	}

	if enrichment.EnrichmentVersion == "" {
		t.Error("EnrichmentVersion should be set")
	}

	if enrichment.GeneratedBy != "classifier-service" {
		t.Errorf("Expected GeneratedBy to be 'classifier-service', got '%s'", enrichment.GeneratedBy)
	}

	// Verify table enrichments
	if len(enrichment.TableEnrichments) != 2 {
		t.Errorf("Expected 2 table enrichments, got %d", len(enrichment.TableEnrichments))
	}

	// Check users table enrichment
	usersEnrichment, exists := enrichment.TableEnrichments["users"]
	if !exists {
		t.Error("Expected users table enrichment to exist")
	} else {
		if usersEnrichment.PrimaryCategory == "" {
			t.Error("Users table should have primary category")
		}
		if usersEnrichment.ClassificationConfidence <= 0 {
			t.Error("Users table should have positive classification confidence")
		}
		if len(usersEnrichment.ClassificationScores) == 0 {
			t.Error("Users table should have classification scores")
		}
		if usersEnrichment.AccessPattern == "" {
			t.Error("Users table should have access pattern")
		}
	}

	// Check orders table enrichment
	ordersEnrichment, exists := enrichment.TableEnrichments["orders"]
	if !exists {
		t.Error("Expected orders table enrichment to exist")
	} else {
		if ordersEnrichment.PrimaryCategory == "" {
			t.Error("Orders table should have primary category")
		}
		if ordersEnrichment.ClassificationConfidence <= 0 {
			t.Error("Orders table should have positive classification confidence")
		}
	}

	// Verify column enrichments
	expectedColumns := []string{"users.id", "users.email", "users.first_name", "users.created_at", "orders.id", "orders.user_id", "orders.total", "orders.created_at"}
	if len(enrichment.ColumnEnrichments) != len(expectedColumns) {
		t.Errorf("Expected %d column enrichments, got %d", len(expectedColumns), len(enrichment.ColumnEnrichments))
	}

	// Check specific column enrichments
	for _, columnKey := range expectedColumns {
		columnEnrichment, exists := enrichment.ColumnEnrichments[columnKey]
		if !exists {
			t.Errorf("Expected column enrichment for %s to exist", columnKey)
			continue
		}

		if columnEnrichment.DataCategory == "" {
			t.Errorf("Column %s should have data category", columnKey)
		}
		if columnEnrichment.RiskLevel == "" {
			t.Errorf("Column %s should have risk level", columnKey)
		}
		if columnEnrichment.RecommendedIndexType == "" {
			t.Errorf("Column %s should have recommended index type", columnKey)
		}
	}

	// Verify index enrichments
	expectedIndexes := []string{"idx_users_email"}
	if len(enrichment.IndexEnrichments) != len(expectedIndexes) {
		t.Errorf("Expected %d index enrichments, got %d", len(expectedIndexes), len(enrichment.IndexEnrichments))
	}

	for _, indexName := range expectedIndexes {
		_, exists := enrichment.IndexEnrichments[indexName]
		if !exists {
			t.Errorf("Expected index enrichment for %s to exist", indexName)
		}
	}
}

func TestService_ClassifyUnifiedModel_NilRequest(t *testing.T) {
	service := NewService()

	req := &pb.ClassifyUnifiedModelRequest{
		UnifiedModel: nil,
	}

	resp, err := service.ClassifyUnifiedModel(context.Background(), req)

	if err == nil {
		t.Error("Expected error when UnifiedModel is nil")
	}

	if resp != nil {
		t.Error("Expected nil response when UnifiedModel is nil")
	}

	expectedError := "unified_model is required"
	if err.Error() != expectedError {
		t.Errorf("Expected error message '%s', got '%s'", expectedError, err.Error())
	}
}

func TestService_ClassifyUnifiedModel_EmptyUnifiedModel(t *testing.T) {
	service := NewService()

	req := &pb.ClassifyUnifiedModelRequest{
		UnifiedModel: &pb.UnifiedModel{}, // Empty but valid protobuf message
	}

	resp, err := service.ClassifyUnifiedModel(context.Background(), req)

	if err != nil {
		t.Fatalf("ClassifyUnifiedModel should not fail with empty model: %v", err)
	}

	if resp == nil {
		t.Fatal("Response should not be nil")
	}

	// Verify empty enrichments
	enrichment := resp.UnifiedModelEnrichment
	if len(enrichment.TableEnrichments) != 0 {
		t.Errorf("Expected 0 table enrichments, got %d", len(enrichment.TableEnrichments))
	}
}

func TestService_ClassifyUnifiedModel_EmptyModel(t *testing.T) {
	service := NewService()

	// Create empty unified model
	testModel := &pb.UnifiedModel{
		DatabaseType: string(dbcapabilities.PostgreSQL),
		Tables:       make(map[string]*pb.Table),
	}

	req := &pb.ClassifyUnifiedModelRequest{
		UnifiedModel: testModel,
	}

	resp, err := service.ClassifyUnifiedModel(context.Background(), req)
	if err != nil {
		t.Fatalf("ClassifyUnifiedModel should not fail with empty model: %v", err)
	}

	if resp == nil {
		t.Fatal("Response should not be nil")
	}

	// Verify empty enrichments
	enrichment := resp.UnifiedModelEnrichment
	if len(enrichment.TableEnrichments) != 0 {
		t.Errorf("Expected 0 table enrichments, got %d", len(enrichment.TableEnrichments))
	}

	if len(enrichment.ColumnEnrichments) != 0 {
		t.Errorf("Expected 0 column enrichments, got %d", len(enrichment.ColumnEnrichments))
	}

	if len(enrichment.IndexEnrichments) != 0 {
		t.Errorf("Expected 0 index enrichments, got %d", len(enrichment.IndexEnrichments))
	}
}

func TestService_ClassifyUnifiedModel_SingleTableNoIndexes(t *testing.T) {
	service := NewService()

	// Create model with single table and no indexes
	testModel := &pb.UnifiedModel{
		DatabaseType: string(dbcapabilities.PostgreSQL),
		Tables: map[string]*pb.Table{
			"simple_table": {
				Name: "simple_table",
				Columns: map[string]*pb.Column{
					"id": {
						Name:         "id",
						DataType:     "integer",
						IsPrimaryKey: true,
					},
					"data": {
						Name:     "data",
						DataType: "text",
					},
				},
				Indexes:     make(map[string]*pb.Index),
				Constraints: make(map[string]*pb.Constraint),
			},
		},
	}

	req := &pb.ClassifyUnifiedModelRequest{
		UnifiedModel: testModel,
	}

	resp, err := service.ClassifyUnifiedModel(context.Background(), req)
	if err != nil {
		t.Fatalf("ClassifyUnifiedModel failed: %v", err)
	}

	// Verify enrichment
	enrichment := resp.UnifiedModelEnrichment

	// Verify single table enrichment
	if len(enrichment.TableEnrichments) != 1 {
		t.Errorf("Expected 1 table enrichment, got %d", len(enrichment.TableEnrichments))
	}

	// Verify column enrichments for 2 columns
	if len(enrichment.ColumnEnrichments) != 2 {
		t.Errorf("Expected 2 column enrichments, got %d", len(enrichment.ColumnEnrichments))
	}

	// Verify no index enrichments
	if len(enrichment.IndexEnrichments) != 0 {
		t.Errorf("Expected 0 index enrichments, got %d", len(enrichment.IndexEnrichments))
	}
}

func TestService_InferAccessPattern(t *testing.T) {
	service := NewService()

	// Test read-heavy pattern (many indexes)
	tableWithManyIndexes := unifiedmodel.Table{
		Name: "read_heavy_table",
		Columns: map[string]unifiedmodel.Column{
			"col1": {Name: "col1", DataType: "integer"},
			"col2": {Name: "col2", DataType: "varchar"},
		},
		Indexes: map[string]unifiedmodel.Index{
			"idx1": {Name: "idx1", Columns: []string{"col1"}},
			"idx2": {Name: "idx2", Columns: []string{"col2"}},
			"idx3": {Name: "idx3", Columns: []string{"col1", "col2"}},
		},
	}

	pattern := service.inferAccessPattern(tableWithManyIndexes)
	if pattern != unifiedmodel.AccessPatternReadHeavy {
		t.Errorf("Expected read-heavy pattern, got %s", pattern)
	}

	// Test append-only pattern (has timestamp)
	tableWithTimestamp := unifiedmodel.Table{
		Name: "log_table",
		Columns: map[string]unifiedmodel.Column{
			"id":         {Name: "id", DataType: "integer"},
			"created_at": {Name: "created_at", DataType: "timestamp"},
		},
		Indexes: map[string]unifiedmodel.Index{},
	}

	pattern = service.inferAccessPattern(tableWithTimestamp)
	if pattern != unifiedmodel.AccessPatternAppendOnly {
		t.Errorf("Expected append-only pattern, got %s", pattern)
	}

	// Test default pattern
	simpleTable := unifiedmodel.Table{
		Name: "simple_table",
		Columns: map[string]unifiedmodel.Column{
			"id":   {Name: "id", DataType: "integer"},
			"data": {Name: "data", DataType: "varchar"},
		},
		Indexes: map[string]unifiedmodel.Index{},
	}

	pattern = service.inferAccessPattern(simpleTable)
	if pattern != unifiedmodel.AccessPatternReadWrite {
		t.Errorf("Expected read-write pattern, got %s", pattern)
	}
}

func TestService_GetRecommendedIndexType(t *testing.T) {
	service := NewService()

	// Test primary key column
	pkColumn := unifiedmodel.Column{
		Name:         "id",
		DataType:     "integer",
		IsPrimaryKey: true,
	}

	indexType := service.getRecommendedIndexType(pkColumn)
	if indexType != unifiedmodel.IndexTypeBTree {
		t.Errorf("Expected BTree index for primary key, got %s", indexType)
	}

	// Test text column
	textColumn := unifiedmodel.Column{
		Name:     "description",
		DataType: "varchar",
	}

	indexType = service.getRecommendedIndexType(textColumn)
	if indexType != unifiedmodel.IndexTypeBTree {
		t.Errorf("Expected BTree index for text column, got %s", indexType)
	}

	// Test JSON column
	jsonColumn := unifiedmodel.Column{
		Name:     "metadata",
		DataType: "json",
	}

	indexType = service.getRecommendedIndexType(jsonColumn)
	if indexType != unifiedmodel.IndexTypeGIN {
		t.Errorf("Expected GIN index for JSON column, got %s", indexType)
	}

	// Test default case
	defaultColumn := unifiedmodel.Column{
		Name:     "value",
		DataType: "decimal",
	}

	indexType = service.getRecommendedIndexType(defaultColumn)
	if indexType != unifiedmodel.IndexTypeBTree {
		t.Errorf("Expected BTree index for default case, got %s", indexType)
	}
}

func TestService_GenerateSchemaID(t *testing.T) {
	// Test with tables
	modelWithTables := &unifiedmodel.UnifiedModel{
		Tables: map[string]unifiedmodel.Table{
			"table1": {Name: "table1"},
			"table2": {Name: "table2"},
		},
	}

	schemaID := generateSchemaID(modelWithTables)
	if schemaID == "" {
		t.Error("Schema ID should not be empty")
	}

	// Test with empty model
	emptyModel := &unifiedmodel.UnifiedModel{
		Tables: map[string]unifiedmodel.Table{},
	}

	schemaID = generateSchemaID(emptyModel)
	if schemaID == "" {
		t.Error("Schema ID should not be empty even for empty model")
	}
}

func TestService_ConvertTableToProtoMetadata(t *testing.T) {
	service := NewService()

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
	}

	metadata := service.convertTableToProtoMetadata(table, "test_table")

	if metadata.Name != "test_table" {
		t.Errorf("Expected name 'test_table', got '%s'", metadata.Name)
	}

	if len(metadata.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(metadata.Columns))
	}

	// Find ID column
	var idCol *pb.ColumnMetadata
	for _, col := range metadata.Columns {
		if col.Name == "id" {
			idCol = col
			break
		}
	}

	if idCol == nil {
		t.Fatal("Expected to find 'id' column")
	}

	if idCol.Type != "integer" {
		t.Errorf("Expected type 'integer', got '%s'", idCol.Type)
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

	if idCol.ColumnDefault != "nextval('seq')" {
		t.Errorf("Expected default value 'nextval('seq')', got '%s'", idCol.ColumnDefault)
	}
}
