package engine

import (
	"context"
	"testing"

	pb "github.com/redbco/redb-open/api/proto/unifiedmodel/v1"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServer_CompareUnifiedModels(t *testing.T) {
	// Create a mock engine
	engine := &Engine{}
	server := NewServer(engine)

	t.Run("compare unified models successfully", func(t *testing.T) {
		// Create test UnifiedModel objects
		previousModel := &unifiedmodel.UnifiedModel{
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
					},
					Indexes:     make(map[string]unifiedmodel.Index),
					Constraints: make(map[string]unifiedmodel.Constraint),
				},
			},
			Schemas:    make(map[string]unifiedmodel.Schema),
			Types:      make(map[string]unifiedmodel.Type),
			Functions:  make(map[string]unifiedmodel.Function),
			Triggers:   make(map[string]unifiedmodel.Trigger),
			Sequences:  make(map[string]unifiedmodel.Sequence),
			Extensions: make(map[string]unifiedmodel.Extension),
		}

		currentModel := &unifiedmodel.UnifiedModel{
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
						"email": { // Added column
							Name:     "email",
							DataType: "varchar",
							Nullable: true,
						},
					},
					Indexes:     make(map[string]unifiedmodel.Index),
					Constraints: make(map[string]unifiedmodel.Constraint),
				},
			},
			Schemas:    make(map[string]unifiedmodel.Schema),
			Types:      make(map[string]unifiedmodel.Type),
			Functions:  make(map[string]unifiedmodel.Function),
			Triggers:   make(map[string]unifiedmodel.Trigger),
			Sequences:  make(map[string]unifiedmodel.Sequence),
			Extensions: make(map[string]unifiedmodel.Extension),
		}

		// Convert Go models to protobuf models
		previousProtoModel := server.convertUnifiedModelToProto(previousModel)
		currentProtoModel := server.convertUnifiedModelToProto(currentModel)

		// Create request
		req := &pb.CompareUnifiedModelsRequest{
			PreviousUnifiedModel: previousProtoModel,
			CurrentUnifiedModel:  currentProtoModel,
		}

		// Call the method
		resp, err := server.CompareUnifiedModels(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Verify results
		assert.True(t, resp.HasChanges)
		assert.Contains(t, resp.Changes, "Added column: users.email")
		assert.Empty(t, resp.Warnings)
	})

	t.Run("handle nil models", func(t *testing.T) {
		req := &pb.CompareUnifiedModelsRequest{
			PreviousUnifiedModel: nil,
			CurrentUnifiedModel:  nil,
		}

		resp, err := server.CompareUnifiedModels(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		assert.False(t, resp.HasChanges)
		assert.Empty(t, resp.Changes)
	})

	t.Run("handle empty models", func(t *testing.T) {
		req := &pb.CompareUnifiedModelsRequest{
			PreviousUnifiedModel: &pb.UnifiedModel{
				DatabaseType: string(dbcapabilities.PostgreSQL),
				Tables:       make(map[string]*pb.Table),
			},
			CurrentUnifiedModel: &pb.UnifiedModel{
				DatabaseType: string(dbcapabilities.PostgreSQL),
				Tables:       make(map[string]*pb.Table),
			},
		}

		resp, err := server.CompareUnifiedModels(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.False(t, resp.HasChanges)
		assert.Empty(t, resp.Changes)
	})
}

func TestServer_CompareSchemas_Deprecated(t *testing.T) {
	// Create a mock engine
	engine := &Engine{}
	server := NewServer(engine)

	t.Run("legacy method returns deprecation error", func(t *testing.T) {
		req := &pb.CompareRequest{
			SchemaType:     "postgres",
			PreviousSchema: &pb.UnifiedModel{},
			CurrentSchema:  &pb.UnifiedModel{},
		}

		resp, err := server.CompareSchemas(context.Background(), req)
		assert.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "legacy schema comparison is deprecated")
	})
}
