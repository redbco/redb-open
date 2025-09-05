package engine

import (
	"context"
	"testing"

	pb "github.com/redbco/redb-open/api/proto/unifiedmodel/v1"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

func TestServer_MatchUnifiedModelsEnriched(t *testing.T) {
	server := NewServer(NewEngine(nil))

	// Create test protobuf unified models
	sourceModel := &pb.UnifiedModel{
		DatabaseType: string(dbcapabilities.PostgreSQL),
		Tables: map[string]*pb.Table{
			"users": {
				Name: "users",
				Columns: map[string]*pb.Column{
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
		},
	}

	targetModel := &pb.UnifiedModel{
		DatabaseType: string(dbcapabilities.PostgreSQL),
		Tables: map[string]*pb.Table{
			"user_accounts": {
				Name: "user_accounts",
				Columns: map[string]*pb.Column{
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
			},
		},
	}

	// Create protobuf enrichments
	sourceEnrichment := &pb.UnifiedModelEnrichment{
		SchemaId:          "source_schema",
		EnrichmentVersion: "1.0.0",
		TableEnrichments: map[string]*pb.TableEnrichment{
			"users": {
				PrimaryCategory:          string(unifiedmodel.TableCategoryTransactional),
				ClassificationConfidence: 0.9,
				AccessPattern:            string(unifiedmodel.AccessPatternReadWrite),
			},
		},
		ColumnEnrichments: map[string]*pb.ColumnEnrichment{
			"users.email": {
				IsPrivilegedData:     true,
				DataCategory:         string(unifiedmodel.DataCategoryEmail),
				PrivilegedConfidence: 0.95,
				RiskLevel:            string(unifiedmodel.RiskLevelHigh),
			},
		},
	}

	targetEnrichment := &pb.UnifiedModelEnrichment{
		SchemaId:          "target_schema",
		EnrichmentVersion: "1.0.0",
		TableEnrichments: map[string]*pb.TableEnrichment{
			"user_accounts": {
				PrimaryCategory:          string(unifiedmodel.TableCategoryTransactional),
				ClassificationConfidence: 0.85,
				AccessPattern:            string(unifiedmodel.AccessPatternReadWrite),
			},
		},
		ColumnEnrichments: map[string]*pb.ColumnEnrichment{
			"user_accounts.email_address": {
				IsPrivilegedData:     true,
				DataCategory:         string(unifiedmodel.DataCategoryEmail),
				PrivilegedConfidence: 0.9,
				RiskLevel:            string(unifiedmodel.RiskLevelHigh),
			},
		},
	}

	// Create request with protobuf messages
	req := &pb.MatchUnifiedModelsEnrichedRequest{
		SourceUnifiedModel: sourceModel,
		SourceEnrichment:   sourceEnrichment,
		TargetUnifiedModel: targetModel,
		TargetEnrichment:   targetEnrichment,
		Options: &pb.MatchOptions{
			NameSimilarityThreshold:  0.3,
			PoorMatchThreshold:       0.4,
			NameWeight:               0.4,
			TypeWeight:               0.2,
			ClassificationWeight:     0.2,
			PrivilegedDataWeight:     0.15,
			TableStructureWeight:     0.05,
			EnableCrossTableMatching: true,
		},
	}

	// Call the endpoint
	resp, err := server.MatchUnifiedModelsEnriched(context.Background(), req)
	if err != nil {
		t.Fatalf("MatchUnifiedModelsEnriched failed: %v", err)
	}

	// Verify response
	if resp == nil {
		t.Fatal("Response should not be nil")
	}

	if len(resp.TableMatches) != 1 {
		t.Errorf("Expected 1 table match, got %d", len(resp.TableMatches))
	}

	tableMatch := resp.TableMatches[0]
	if tableMatch.SourceTable != "users" || tableMatch.TargetTable != "user_accounts" {
		t.Errorf("Expected users->user_accounts match, got %s->%s", tableMatch.SourceTable, tableMatch.TargetTable)
	}

	if tableMatch.ClassificationMatch != "transactional" {
		t.Errorf("Expected classification match 'transactional', got '%s'", tableMatch.ClassificationMatch)
	}

	if len(tableMatch.ColumnMatches) != 2 {
		t.Errorf("Expected 2 column matches, got %d", len(tableMatch.ColumnMatches))
	}

	// Check for email column match
	emailMatch := false
	for _, colMatch := range tableMatch.ColumnMatches {
		if colMatch.SourceColumn == "email" && colMatch.TargetColumn == "email_address" {
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

	if resp.OverallSimilarityScore <= 0.0 {
		t.Errorf("Expected positive overall similarity score, got %f", resp.OverallSimilarityScore)
	}
}

func TestServer_MatchUnifiedModelsEnriched_NilRequest(t *testing.T) {
	server := NewServer(NewEngine(nil))

	// Test nil source model
	req := &pb.MatchUnifiedModelsEnrichedRequest{
		SourceUnifiedModel: nil,
		TargetUnifiedModel: &pb.UnifiedModel{},
	}

	resp, err := server.MatchUnifiedModelsEnriched(context.Background(), req)
	if err == nil {
		t.Error("Expected error when source model is nil")
	}
	if resp != nil {
		t.Error("Expected nil response when source model is nil")
	}

	// Test nil target model
	req = &pb.MatchUnifiedModelsEnrichedRequest{
		SourceUnifiedModel: &pb.UnifiedModel{},
		TargetUnifiedModel: nil,
	}

	resp, err = server.MatchUnifiedModelsEnriched(context.Background(), req)
	if err == nil {
		t.Error("Expected error when target model is nil")
	}
	if resp != nil {
		t.Error("Expected nil response when target model is nil")
	}
}

func TestServer_MatchUnifiedModelsEnriched_EmptyModels(t *testing.T) {
	server := NewServer(NewEngine(nil))

	// Test with empty models (should not error, but return empty results)
	req := &pb.MatchUnifiedModelsEnrichedRequest{
		SourceUnifiedModel: &pb.UnifiedModel{
			DatabaseType: string(dbcapabilities.PostgreSQL),
			Tables:       make(map[string]*pb.Table),
		},
		TargetUnifiedModel: &pb.UnifiedModel{
			DatabaseType: string(dbcapabilities.PostgreSQL),
			Tables:       make(map[string]*pb.Table),
		},
	}

	resp, err := server.MatchUnifiedModelsEnriched(context.Background(), req)
	if err != nil {
		t.Errorf("Should not error with empty models: %v", err)
	}
	if resp == nil {
		t.Error("Expected response even with empty models")
	}
	if resp != nil && len(resp.TableMatches) != 0 {
		t.Error("Expected no table matches with empty models")
	}
}

func TestServer_ConvertMatchOptions(t *testing.T) {
	server := NewServer(NewEngine(nil))

	// Test with nil options
	options := server.convertMatchOptions(nil)
	if options == nil {
		t.Fatal("convertMatchOptions should not return nil")
	}

	// Should return default options
	if options.NameSimilarityThreshold != 0.3 {
		t.Errorf("Expected default NameSimilarityThreshold 0.3, got %f", options.NameSimilarityThreshold)
	}

	// Test with provided options
	protoOptions := &pb.MatchOptions{
		NameSimilarityThreshold:  0.5,
		PoorMatchThreshold:       0.6,
		NameWeight:               0.5,
		TypeWeight:               0.3,
		ClassificationWeight:     0.1,
		PrivilegedDataWeight:     0.1,
		TableStructureWeight:     0.0,
		EnableCrossTableMatching: false,
	}

	options = server.convertMatchOptions(protoOptions)
	if options.NameSimilarityThreshold != 0.5 {
		t.Errorf("Expected NameSimilarityThreshold 0.5, got %f", options.NameSimilarityThreshold)
	}
	if options.EnableCrossTableMatching {
		t.Error("Expected EnableCrossTableMatching to be false")
	}
}
