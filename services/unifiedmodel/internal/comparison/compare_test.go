package comparison

import (
	"testing"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnifiedSchemaComparator(t *testing.T) {
	comparator := NewUnifiedSchemaComparator()

	t.Run("compare identical models", func(t *testing.T) {
		model := &unifiedmodel.UnifiedModel{
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
						"name": {
							Name:     "name",
							DataType: "varchar",
							Nullable: false,
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

		result, err := comparator.CompareUnifiedModels(model, model)
		require.NoError(t, err)
		assert.False(t, result.HasChanges)
		assert.Empty(t, result.Changes)
	})

	t.Run("detect added table", func(t *testing.T) {
		prevModel := &unifiedmodel.UnifiedModel{
			DatabaseType: dbcapabilities.PostgreSQL,
			Tables:       make(map[string]unifiedmodel.Table),
			Schemas:      make(map[string]unifiedmodel.Schema),
			Types:        make(map[string]unifiedmodel.Type),
			Functions:    make(map[string]unifiedmodel.Function),
			Triggers:     make(map[string]unifiedmodel.Trigger),
			Sequences:    make(map[string]unifiedmodel.Sequence),
			Extensions:   make(map[string]unifiedmodel.Extension),
		}

		currModel := &unifiedmodel.UnifiedModel{
			DatabaseType: dbcapabilities.PostgreSQL,
			Tables: map[string]unifiedmodel.Table{
				"users": {
					Name:        "users",
					Columns:     make(map[string]unifiedmodel.Column),
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

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		require.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Added table: users")
	})

	t.Run("detect removed table", func(t *testing.T) {
		prevModel := &unifiedmodel.UnifiedModel{
			DatabaseType: dbcapabilities.PostgreSQL,
			Tables: map[string]unifiedmodel.Table{
				"users": {
					Name:        "users",
					Columns:     make(map[string]unifiedmodel.Column),
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

		currModel := &unifiedmodel.UnifiedModel{
			DatabaseType: dbcapabilities.PostgreSQL,
			Tables:       make(map[string]unifiedmodel.Table),
			Schemas:      make(map[string]unifiedmodel.Schema),
			Types:        make(map[string]unifiedmodel.Type),
			Functions:    make(map[string]unifiedmodel.Function),
			Triggers:     make(map[string]unifiedmodel.Trigger),
			Sequences:    make(map[string]unifiedmodel.Sequence),
			Extensions:   make(map[string]unifiedmodel.Extension),
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		require.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Removed table: users")
	})

	t.Run("detect column changes", func(t *testing.T) {
		prevModel := &unifiedmodel.UnifiedModel{
			DatabaseType: dbcapabilities.PostgreSQL,
			Tables: map[string]unifiedmodel.Table{
				"users": {
					Name: "users",
					Columns: map[string]unifiedmodel.Column{
						"id": {
							Name:     "id",
							DataType: "integer",
							Nullable: false,
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

		currModel := &unifiedmodel.UnifiedModel{
			DatabaseType: dbcapabilities.PostgreSQL,
			Tables: map[string]unifiedmodel.Table{
				"users": {
					Name: "users",
					Columns: map[string]unifiedmodel.Column{
						"id": {
							Name:     "id",
							DataType: "bigint", // Changed from integer to bigint
							Nullable: false,
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

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		require.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Column users.id data type changed: integer -> bigint")
	})

	t.Run("detect added extension", func(t *testing.T) {
		prevModel := &unifiedmodel.UnifiedModel{
			DatabaseType: dbcapabilities.PostgreSQL,
			Tables:       make(map[string]unifiedmodel.Table),
			Schemas:      make(map[string]unifiedmodel.Schema),
			Types:        make(map[string]unifiedmodel.Type),
			Functions:    make(map[string]unifiedmodel.Function),
			Triggers:     make(map[string]unifiedmodel.Trigger),
			Sequences:    make(map[string]unifiedmodel.Sequence),
			Extensions:   make(map[string]unifiedmodel.Extension),
		}

		currModel := &unifiedmodel.UnifiedModel{
			DatabaseType: dbcapabilities.PostgreSQL,
			Tables:       make(map[string]unifiedmodel.Table),
			Schemas:      make(map[string]unifiedmodel.Schema),
			Types:        make(map[string]unifiedmodel.Type),
			Functions:    make(map[string]unifiedmodel.Function),
			Triggers:     make(map[string]unifiedmodel.Trigger),
			Sequences:    make(map[string]unifiedmodel.Sequence),
			Extensions: map[string]unifiedmodel.Extension{
				"uuid-ossp": {
					Name:    "uuid-ossp",
					Version: "1.1",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		require.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Added extension: uuid-ossp")
	})

	t.Run("handle nil models", func(t *testing.T) {
		result, err := comparator.CompareUnifiedModels(nil, nil)
		require.NoError(t, err)
		assert.False(t, result.HasChanges)
		assert.Empty(t, result.Changes)
	})

	t.Run("detect_detailed_view_changes", func(t *testing.T) {
		// Create models with view changes
		prevModel := &unifiedmodel.UnifiedModel{
			Views: map[string]unifiedmodel.View{
				"test_view": {
					Name:       "test_view",
					Definition: "SELECT * FROM table1",
					Comment:    "Original comment",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Views: map[string]unifiedmodel.View{
				"test_view": {
					Name:       "test_view",
					Definition: "SELECT * FROM table1 WHERE active = true",
					Comment:    "Updated comment",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "View test_view definition changed")
		assert.Contains(t, result.Changes, "View test_view comment changed: Original comment -> Updated comment")
	})

	t.Run("detect_detailed_live_view_changes", func(t *testing.T) {
		// Create models with live view changes
		prevModel := &unifiedmodel.UnifiedModel{
			LiveViews: map[string]unifiedmodel.LiveView{
				"test_live_view": {
					Name:       "test_live_view",
					Definition: "SELECT * FROM events",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			LiveViews: map[string]unifiedmodel.LiveView{
				"test_live_view": {
					Name:       "test_live_view",
					Definition: "SELECT * FROM events WHERE timestamp > now() - interval '1 hour'",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Live view test_live_view definition changed")
	})

	t.Run("detect_detailed_type_changes", func(t *testing.T) {
		// Create models with type changes
		prevModel := &unifiedmodel.UnifiedModel{
			Types: map[string]unifiedmodel.Type{
				"custom_type": {
					Name:     "custom_type",
					Category: "enum",
					Definition: map[string]any{
						"values": []string{"active", "inactive"},
					},
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Types: map[string]unifiedmodel.Type{
				"custom_type": {
					Name:     "custom_type",
					Category: "composite",
					Definition: map[string]any{
						"fields": []string{"status", "timestamp"},
					},
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Type custom_type category changed: enum -> composite")
		assert.Contains(t, result.Changes, "Type custom_type definition changed")
	})

	t.Run("detect_detailed_sequence_changes", func(t *testing.T) {
		// Create models with sequence changes
		prevModel := &unifiedmodel.UnifiedModel{
			Sequences: map[string]unifiedmodel.Sequence{
				"user_id_seq": {
					Name:      "user_id_seq",
					Start:     1,
					Increment: 1,
					Cycle:     false,
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Sequences: map[string]unifiedmodel.Sequence{
				"user_id_seq": {
					Name:      "user_id_seq",
					Start:     100,
					Increment: 2,
					Cycle:     true,
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Sequence user_id_seq start changed: 1 -> 100")
		assert.Contains(t, result.Changes, "Sequence user_id_seq increment changed: 1 -> 2")
		assert.Contains(t, result.Changes, "Sequence user_id_seq cycle changed: false -> true")
	})

	t.Run("detect_detailed_trigger_changes", func(t *testing.T) {
		// Create models with trigger changes
		prevModel := &unifiedmodel.UnifiedModel{
			Triggers: map[string]unifiedmodel.Trigger{
				"audit_trigger": {
					Name:      "audit_trigger",
					Table:     "users",
					Timing:    "before",
					Events:    []string{"insert", "update"},
					Procedure: "audit_function",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Triggers: map[string]unifiedmodel.Trigger{
				"audit_trigger": {
					Name:      "audit_trigger",
					Table:     "users",
					Timing:    "after",
					Events:    []string{"insert", "update", "delete"},
					Procedure: "new_audit_function",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Trigger audit_trigger procedure changed")
		// Note: The current implementation may not detect all trigger changes - this tests what's currently implemented
	})

	t.Run("detect_detailed_extension_changes", func(t *testing.T) {
		// Create models with extension changes
		prevModel := &unifiedmodel.UnifiedModel{
			Extensions: map[string]unifiedmodel.Extension{
				"postgis": {
					Name:    "postgis",
					Version: "3.1.0",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Extensions: map[string]unifiedmodel.Extension{
				"postgis": {
					Name:    "postgis",
					Version: "3.2.0",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Extension postgis version changed: 3.1.0 -> 3.2.0")
	})

	t.Run("handle_nil_models_comprehensive", func(t *testing.T) {
		// Test that nil models are handled without panics and all map fields are initialized
		result, err := comparator.CompareUnifiedModels(nil, nil)
		require.NoError(t, err)
		assert.False(t, result.HasChanges)
		assert.Empty(t, result.Changes)
		assert.Empty(t, result.Warnings)

		// Test that comparison with nil previous model works
		currModel := &unifiedmodel.UnifiedModel{
			Tables: map[string]unifiedmodel.Table{
				"test_table": {Name: "test_table"},
			},
		}
		result, err = comparator.CompareUnifiedModels(nil, currModel)
		require.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Added table: test_table")

		// Test that comparison with nil current model works
		prevModel := &unifiedmodel.UnifiedModel{
			Tables: map[string]unifiedmodel.Table{
				"test_table": {Name: "test_table"},
			},
		}
		result, err = comparator.CompareUnifiedModels(prevModel, nil)
		require.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Removed table: test_table")
	})

	t.Run("detect_detailed_collection_changes", func(t *testing.T) {
		// Create models with collection changes
		prevModel := &unifiedmodel.UnifiedModel{
			Collections: map[string]unifiedmodel.Collection{
				"users": {
					Name:     "users",
					Owner:    "admin",
					Comment:  "User collection",
					ShardKey: []string{"user_id"},
					Fields:   map[string]unifiedmodel.Field{"name": {Name: "name"}},
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Collections: map[string]unifiedmodel.Collection{
				"users": {
					Name:     "users",
					Owner:    "system",
					Comment:  "Updated user collection",
					ShardKey: []string{"user_id", "region"},
					Fields:   map[string]unifiedmodel.Field{"name": {Name: "name"}, "email": {Name: "email"}},
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Collection users owner changed: admin -> system")
		assert.Contains(t, result.Changes, "Collection users comment changed: User collection -> Updated user collection")
		assert.Contains(t, result.Changes, "Collection users shard key changed")
		assert.Contains(t, result.Changes, "Collection users fields changed")
	})

	t.Run("detect_detailed_node_changes", func(t *testing.T) {
		// Create models with node changes
		prevModel := &unifiedmodel.UnifiedModel{
			Nodes: map[string]unifiedmodel.Node{
				"person": {
					Label:      "Person",
					Properties: map[string]unifiedmodel.Property{"name": {Name: "name"}},
					Indexes:    map[string]unifiedmodel.Index{"idx_name": {Name: "idx_name"}},
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Nodes: map[string]unifiedmodel.Node{
				"person": {
					Label:      "User",
					Properties: map[string]unifiedmodel.Property{"name": {Name: "name"}, "email": {Name: "email"}},
					Indexes:    map[string]unifiedmodel.Index{"idx_name": {Name: "idx_name"}},
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Node person label changed: Person -> User")
		assert.Contains(t, result.Changes, "Node person properties changed")
	})

	t.Run("detect_detailed_memory_table_changes", func(t *testing.T) {
		// Create models with memory table changes
		prevModel := &unifiedmodel.UnifiedModel{
			MemoryTables: map[string]unifiedmodel.MemoryTable{
				"cache_table": {
					Name: "cache_table",
					Columns: map[string]unifiedmodel.Column{
						"id": {Name: "id", DataType: "int"},
					},
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			MemoryTables: map[string]unifiedmodel.MemoryTable{
				"cache_table": {
					Name: "cache_table",
					Columns: map[string]unifiedmodel.Column{
						"id":   {Name: "id", DataType: "int"},
						"data": {Name: "data", DataType: "text"},
					},
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Added column: cache_table.data")
	})

	t.Run("detect_detailed_vector_changes", func(t *testing.T) {
		// Create models with vector changes
		prevModel := &unifiedmodel.UnifiedModel{
			Vectors: map[string]unifiedmodel.Vector{
				"embeddings_vec": {
					Name:      "embeddings_vec",
					Dimension: 512,
					Metric:    "cosine",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Vectors: map[string]unifiedmodel.Vector{
				"embeddings_vec": {
					Name:      "embeddings_vec",
					Dimension: 1024,
					Metric:    "euclidean",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Vector embeddings_vec dimension changed: 512 -> 1024")
		assert.Contains(t, result.Changes, "Vector embeddings_vec metric changed: cosine -> euclidean")
	})

	t.Run("detect_detailed_embedding_changes", func(t *testing.T) {
		// Create models with embedding changes
		prevModel := &unifiedmodel.UnifiedModel{
			Embeddings: map[string]unifiedmodel.Embedding{
				"text_embedding": {
					Name:  "text_embedding",
					Model: "bert-base-uncased",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Embeddings: map[string]unifiedmodel.Embedding{
				"text_embedding": {
					Name:  "text_embedding",
					Model: "bert-large-uncased",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Embedding text_embedding model changed: bert-base-uncased -> bert-large-uncased")
	})

	t.Run("detect_detailed_document_changes", func(t *testing.T) {
		// Create models with document changes
		prevModel := &unifiedmodel.UnifiedModel{
			Documents: map[string]unifiedmodel.Document{
				"user_doc": {
					Key:    "user_doc",
					Fields: map[string]any{"name": "John", "age": 30},
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Documents: map[string]unifiedmodel.Document{
				"user_doc": {
					Key:    "user_doc",
					Fields: map[string]any{"name": "John", "age": 30, "email": "john@example.com"},
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Document user_doc fields changed")
	})

	t.Run("detect_detailed_embedded_document_changes", func(t *testing.T) {
		// Create models with embedded document changes
		prevModel := &unifiedmodel.UnifiedModel{
			EmbeddedDocuments: map[string]unifiedmodel.EmbeddedDocument{
				"address": {
					Name:   "address",
					Fields: map[string]any{"street": "123 Main St", "city": "Anytown"},
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			EmbeddedDocuments: map[string]unifiedmodel.EmbeddedDocument{
				"address": {
					Name:   "address",
					Fields: map[string]any{"street": "123 Main St", "city": "Anytown", "country": "USA"},
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Embedded document address fields changed")
	})

	t.Run("detect_detailed_relationship_changes", func(t *testing.T) {
		// Create models with relationship changes
		prevModel := &unifiedmodel.UnifiedModel{
			Relationships: map[string]unifiedmodel.Relationship{
				"KNOWS": {
					Type:      "KNOWS",
					FromLabel: "Person",
					ToLabel:   "Person",
					Properties: map[string]unifiedmodel.Property{
						"since": {Name: "since"},
					},
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Relationships: map[string]unifiedmodel.Relationship{
				"KNOWS": {
					Type:      "FRIENDS_WITH",
					FromLabel: "User",
					ToLabel:   "User",
					Properties: map[string]unifiedmodel.Property{
						"since":    {Name: "since"},
						"strength": {Name: "strength"},
					},
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Relationship KNOWS type changed: KNOWS -> FRIENDS_WITH")
		assert.Contains(t, result.Changes, "Relationship KNOWS from label changed: Person -> User")
		assert.Contains(t, result.Changes, "Relationship KNOWS to label changed: Person -> User")
		assert.Contains(t, result.Changes, "Relationship KNOWS properties changed")
	})

	t.Run("detect_detailed_external_table_changes", func(t *testing.T) {
		// Create models with external table changes
		prevModel := &unifiedmodel.UnifiedModel{
			ExternalTables: map[string]unifiedmodel.ExternalTable{
				"s3_data": {
					Name:     "s3_data",
					Location: "s3://bucket/old-path/",
					Format:   "parquet",
					Columns: map[string]unifiedmodel.Column{
						"id": {Name: "id", DataType: "int"},
					},
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			ExternalTables: map[string]unifiedmodel.ExternalTable{
				"s3_data": {
					Name:     "s3_data",
					Location: "s3://bucket/new-path/",
					Format:   "json",
					Columns: map[string]unifiedmodel.Column{
						"id":   {Name: "id", DataType: "int"},
						"name": {Name: "name", DataType: "text"},
					},
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "External table s3_data location changed: s3://bucket/old-path/ -> s3://bucket/new-path/")
		assert.Contains(t, result.Changes, "External table s3_data format changed: parquet -> json")
		assert.Contains(t, result.Changes, "Added column: s3_data.name")
	})

	t.Run("detect_detailed_foreign_table_changes", func(t *testing.T) {
		// Create models with foreign table changes
		prevModel := &unifiedmodel.UnifiedModel{
			ForeignTables: map[string]unifiedmodel.ForeignTable{
				"remote_users": {
					Name:   "remote_users",
					Server: "old_server",
					Columns: map[string]unifiedmodel.Column{
						"id": {Name: "id", DataType: "int"},
					},
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			ForeignTables: map[string]unifiedmodel.ForeignTable{
				"remote_users": {
					Name:   "remote_users",
					Server: "new_server",
					Columns: map[string]unifiedmodel.Column{
						"id":    {Name: "id", DataType: "int"},
						"email": {Name: "email", DataType: "text"},
					},
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Foreign table remote_users server changed: old_server -> new_server")
		assert.Contains(t, result.Changes, "Added column: remote_users.email")
	})

	t.Run("detect_detailed_graph_changes", func(t *testing.T) {
		// Create models with graph changes
		prevModel := &unifiedmodel.UnifiedModel{
			Graphs: map[string]unifiedmodel.Graph{
				"social_graph": {
					Name: "social_graph",
					NodeLabels: map[string]unifiedmodel.Node{
						"Person": {Label: "Person"},
					},
					RelTypes: map[string]unifiedmodel.Relationship{
						"KNOWS": {Type: "KNOWS"},
					},
					Indexes: map[string]unifiedmodel.Index{
						"person_name_idx": {Name: "person_name_idx"},
					},
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Graphs: map[string]unifiedmodel.Graph{
				"social_graph": {
					Name: "social_graph",
					NodeLabels: map[string]unifiedmodel.Node{
						"Person": {Label: "Person"},
						"User":   {Label: "User"},
					},
					RelTypes: map[string]unifiedmodel.Relationship{
						"KNOWS":        {Type: "KNOWS"},
						"FRIENDS_WITH": {Type: "FRIENDS_WITH"},
					},
					Indexes: map[string]unifiedmodel.Index{
						"person_name_idx": {Name: "person_name_idx"},
						"user_email_idx":  {Name: "user_email_idx"},
					},
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Graph social_graph node labels changed")
		assert.Contains(t, result.Changes, "Graph social_graph relationship types changed")
		assert.Contains(t, result.Changes, "Added index: social_graph.user_email_idx")
	})

	t.Run("detect_detailed_vector_index_changes", func(t *testing.T) {
		// Create models with vector index changes
		prevModel := &unifiedmodel.UnifiedModel{
			VectorIndexes: map[string]unifiedmodel.VectorIndex{
				"embedding_idx": {
					Name:      "embedding_idx",
					On:        "documents",
					Fields:    []string{"content_vector"},
					Metric:    "cosine",
					Dimension: 512,
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			VectorIndexes: map[string]unifiedmodel.VectorIndex{
				"embedding_idx": {
					Name:      "embedding_idx",
					On:        "articles",
					Fields:    []string{"content_vector", "title_vector"},
					Metric:    "euclidean",
					Dimension: 1024,
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Vector index embedding_idx target changed: documents -> articles")
		assert.Contains(t, result.Changes, "Vector index embedding_idx metric changed: cosine -> euclidean")
		assert.Contains(t, result.Changes, "Vector index embedding_idx dimension changed: 512 -> 1024")
		assert.Contains(t, result.Changes, "Vector index embedding_idx fields changed")
	})

	t.Run("detect_detailed_search_index_changes", func(t *testing.T) {
		// Create models with search index changes
		prevModel := &unifiedmodel.UnifiedModel{
			SearchIndexes: map[string]unifiedmodel.SearchIndex{
				"text_search": {
					Name:     "text_search",
					On:       "documents",
					Fields:   []string{"title", "content"},
					Analyzer: "standard",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			SearchIndexes: map[string]unifiedmodel.SearchIndex{
				"text_search": {
					Name:     "text_search",
					On:       "articles",
					Fields:   []string{"title", "content", "summary"},
					Analyzer: "english",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Search index text_search target changed: documents -> articles")
		assert.Contains(t, result.Changes, "Search index text_search analyzer changed: standard -> english")
		assert.Contains(t, result.Changes, "Search index text_search fields changed")
	})

	t.Run("detect_detailed_path_changes", func(t *testing.T) {
		// Create models with path changes
		prevModel := &unifiedmodel.UnifiedModel{
			Paths: map[string]unifiedmodel.Path{
				"user_friends": {
					Name:     "user_friends",
					Sequence: []string{"User", "KNOWS", "User"},
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Paths: map[string]unifiedmodel.Path{
				"user_friends": {
					Name:     "user_friends",
					Sequence: []string{"Person", "FRIENDS_WITH", "Person"},
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Path user_friends sequence changed")
	})

	t.Run("detect_detailed_partition_changes", func(t *testing.T) {
		// Create models with partition changes
		prevModel := &unifiedmodel.UnifiedModel{
			Partitions: map[string]unifiedmodel.Partition{
				"sales_partition": {
					Name: "sales_partition",
					Type: "range",
					Key:  []string{"date"},
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Partitions: map[string]unifiedmodel.Partition{
				"sales_partition": {
					Name: "sales_partition",
					Type: "hash",
					Key:  []string{"customer_id", "date"},
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Partition sales_partition type changed: range -> hash")
		assert.Contains(t, result.Changes, "Partition sales_partition key changed")
	})

	t.Run("detect_detailed_sub_partition_changes", func(t *testing.T) {
		// Create models with sub-partition changes
		prevModel := &unifiedmodel.UnifiedModel{
			SubPartitions: map[string]unifiedmodel.SubPartition{
				"sales_sub": {
					Name: "sales_sub",
					Type: "list",
					Key:  []string{"region"},
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			SubPartitions: map[string]unifiedmodel.SubPartition{
				"sales_sub": {
					Name: "sales_sub",
					Type: "hash",
					Key:  []string{"region", "country"},
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Sub-partition sales_sub type changed: list -> hash")
		assert.Contains(t, result.Changes, "Sub-partition sales_sub key changed")
	})

	t.Run("detect_detailed_shard_changes", func(t *testing.T) {
		// Create models with shard changes
		prevModel := &unifiedmodel.UnifiedModel{
			Shards: map[string]unifiedmodel.Shard{
				"user_shard": {
					Name:     "user_shard",
					Strategy: "consistent_hash",
					Key:      []string{"user_id"},
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Shards: map[string]unifiedmodel.Shard{
				"user_shard": {
					Name:     "user_shard",
					Strategy: "range_based",
					Key:      []string{"user_id", "tenant_id"},
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Shard user_shard strategy changed: consistent_hash -> range_based")
		assert.Contains(t, result.Changes, "Shard user_shard key changed")
	})

	t.Run("detect_detailed_keyspace_changes", func(t *testing.T) {
		// Create models with keyspace changes
		prevModel := &unifiedmodel.UnifiedModel{
			Keyspaces: map[string]unifiedmodel.Keyspace{
				"app_keyspace": {
					Name:                "app_keyspace",
					ReplicationStrategy: "SimpleStrategy",
					ReplicationOptions:  map[string]string{"replication_factor": "3"},
					DurableWrites:       true,
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Keyspaces: map[string]unifiedmodel.Keyspace{
				"app_keyspace": {
					Name:                "app_keyspace",
					ReplicationStrategy: "NetworkTopologyStrategy",
					ReplicationOptions:  map[string]string{"datacenter1": "3", "datacenter2": "2"},
					DurableWrites:       false,
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Keyspace app_keyspace replication strategy changed: SimpleStrategy -> NetworkTopologyStrategy")
		assert.Contains(t, result.Changes, "Keyspace app_keyspace durable writes changed: true -> false")
		assert.Contains(t, result.Changes, "Keyspace app_keyspace replication options changed")
	})

	t.Run("detect_detailed_namespace_changes", func(t *testing.T) {
		// Create models with namespace changes
		prevModel := &unifiedmodel.UnifiedModel{
			Namespaces: map[string]unifiedmodel.Namespace{
				"production": {
					Name:   "production",
					Labels: map[string]string{"env": "prod", "team": "backend"},
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Namespaces: map[string]unifiedmodel.Namespace{
				"production": {
					Name:   "production",
					Labels: map[string]string{"env": "prod", "team": "backend", "region": "us-east-1"},
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Namespace production labels changed")
	})

	t.Run("detect_detailed_property_key_changes", func(t *testing.T) {
		// Create models with property key changes
		prevModel := &unifiedmodel.UnifiedModel{
			PropertyKeys: map[string]unifiedmodel.PropertyKey{
				"user_id": {
					Name: "user_id",
					Type: "integer",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			PropertyKeys: map[string]unifiedmodel.PropertyKey{
				"user_id": {
					Name: "user_id",
					Type: "string",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Property key user_id type changed: integer -> string")
	})

	t.Run("detect_detailed_identity_changes", func(t *testing.T) {
		// Create models with identity changes
		prevModel := &unifiedmodel.UnifiedModel{
			Identities: map[string]unifiedmodel.Identity{
				"user_seq": {
					Name:      "user_seq",
					Table:     "users",
					Column:    "id",
					Strategy:  "always",
					Start:     1,
					Increment: 1,
					Cycle:     false,
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Identities: map[string]unifiedmodel.Identity{
				"user_seq": {
					Name:      "user_seq",
					Table:     "accounts",
					Column:    "user_id",
					Strategy:  "by_default",
					Start:     100,
					Increment: 2,
					Cycle:     true,
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Identity user_seq table changed: users -> accounts")
		assert.Contains(t, result.Changes, "Identity user_seq column changed: id -> user_id")
		assert.Contains(t, result.Changes, "Identity user_seq strategy changed: always -> by_default")
		assert.Contains(t, result.Changes, "Identity user_seq start changed: 1 -> 100")
		assert.Contains(t, result.Changes, "Identity user_seq increment changed: 1 -> 2")
		assert.Contains(t, result.Changes, "Identity user_seq cycle changed: false -> true")
	})

	t.Run("detect_detailed_uuid_generator_changes", func(t *testing.T) {
		// Create models with UUID generator changes
		prevModel := &unifiedmodel.UnifiedModel{
			UUIDGenerators: map[string]unifiedmodel.UUIDGenerator{
				"uuid_gen": {
					Name:    "uuid_gen",
					Version: "v4",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			UUIDGenerators: map[string]unifiedmodel.UUIDGenerator{
				"uuid_gen": {
					Name:    "uuid_gen",
					Version: "v7",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "UUID generator uuid_gen version changed: v4 -> v7")
	})

	t.Run("detect_detailed_procedure_changes", func(t *testing.T) {
		// Create models with procedure changes
		prevModel := &unifiedmodel.UnifiedModel{
			Procedures: map[string]unifiedmodel.Procedure{
				"update_user": {
					Name:       "update_user",
					Language:   "plpgsql",
					Definition: "BEGIN UPDATE users SET name = p_name WHERE id = p_id; END;",
					Arguments:  []unifiedmodel.Argument{{Name: "p_id", Type: "integer"}},
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Procedures: map[string]unifiedmodel.Procedure{
				"update_user": {
					Name:       "update_user",
					Language:   "sql",
					Definition: "UPDATE users SET name = p_name, email = p_email WHERE id = p_id;",
					Arguments:  []unifiedmodel.Argument{{Name: "p_id", Type: "integer"}, {Name: "p_email", Type: "text"}},
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Procedure update_user language changed: plpgsql -> sql")
		assert.Contains(t, result.Changes, "Procedure update_user definition changed")
		assert.Contains(t, result.Changes, "Procedure update_user arguments changed")
	})

	t.Run("detect_detailed_method_changes", func(t *testing.T) {
		// Create models with method changes
		prevModel := &unifiedmodel.UnifiedModel{
			Methods: map[string]unifiedmodel.Method{
				"get_name": {
					Name:       "get_name",
					OfType:     "user_type",
					Language:   "plpgsql",
					Definition: "RETURN self.name;",
					Arguments:  []unifiedmodel.Argument{},
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Methods: map[string]unifiedmodel.Method{
				"get_name": {
					Name:       "get_name",
					OfType:     "person_type",
					Language:   "sql",
					Definition: "RETURN COALESCE(self.first_name || ' ' || self.last_name, self.username);",
					Arguments:  []unifiedmodel.Argument{{Name: "format", Type: "text"}},
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Method get_name object type changed: user_type -> person_type")
		assert.Contains(t, result.Changes, "Method get_name language changed: plpgsql -> sql")
		assert.Contains(t, result.Changes, "Method get_name definition changed")
		assert.Contains(t, result.Changes, "Method get_name arguments changed")
	})

	t.Run("detect_detailed_event_trigger_changes", func(t *testing.T) {
		// Create models with event trigger changes
		prevModel := &unifiedmodel.UnifiedModel{
			EventTriggers: map[string]unifiedmodel.EventTrigger{
				"audit_ddl": {
					Name:      "audit_ddl",
					Scope:     "database",
					Events:    []string{"ddl_command_start", "ddl_command_end"},
					Procedure: "audit_ddl_proc",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			EventTriggers: map[string]unifiedmodel.EventTrigger{
				"audit_ddl": {
					Name:      "audit_ddl",
					Scope:     "schema",
					Events:    []string{"ddl_command_start", "ddl_command_end", "table_rewrite"},
					Procedure: "enhanced_audit_proc",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Event trigger audit_ddl scope changed: database -> schema")
		assert.Contains(t, result.Changes, "Event trigger audit_ddl procedure changed: audit_ddl_proc -> enhanced_audit_proc")
		assert.Contains(t, result.Changes, "Event trigger audit_ddl events changed")
	})

	t.Run("detect_detailed_aggregate_changes", func(t *testing.T) {
		// Create models with aggregate changes
		prevModel := &unifiedmodel.UnifiedModel{
			Aggregates: map[string]unifiedmodel.Aggregate{
				"custom_avg": {
					Name:       "custom_avg",
					InputTypes: []string{"numeric"},
					StateType:  "internal",
					FinalType:  "numeric",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Aggregates: map[string]unifiedmodel.Aggregate{
				"custom_avg": {
					Name:       "custom_avg",
					InputTypes: []string{"numeric", "integer"},
					StateType:  "numeric",
					FinalType:  "double precision",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Aggregate custom_avg state type changed: internal -> numeric")
		assert.Contains(t, result.Changes, "Aggregate custom_avg final type changed: numeric -> double precision")
		assert.Contains(t, result.Changes, "Aggregate custom_avg input types changed")
	})

	t.Run("detect_detailed_operator_changes", func(t *testing.T) {
		// Create models with operator changes
		prevModel := &unifiedmodel.UnifiedModel{
			Operators: map[string]unifiedmodel.Operator{
				"@>": {
					Name:       "@>",
					LeftType:   "jsonb",
					RightType:  "jsonb",
					Returns:    "boolean",
					Definition: "jsonb_contains",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Operators: map[string]unifiedmodel.Operator{
				"@>": {
					Name:       "@>",
					LeftType:   "json",
					RightType:  "json",
					Returns:    "bool",
					Definition: "json_contains_enhanced",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Operator @> left type changed: jsonb -> json")
		assert.Contains(t, result.Changes, "Operator @> right type changed: jsonb -> json")
		assert.Contains(t, result.Changes, "Operator @> return type changed: boolean -> bool")
		assert.Contains(t, result.Changes, "Operator @> definition changed")
	})

	t.Run("detect_detailed_module_changes", func(t *testing.T) {
		// Create models with module changes
		prevModel := &unifiedmodel.UnifiedModel{
			Modules: map[string]unifiedmodel.Module{
				"auth_module": {
					Name:     "auth_module",
					Comment:  "Authentication module",
					Language: "python",
					Code:     "def authenticate(user): return True",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Modules: map[string]unifiedmodel.Module{
				"auth_module": {
					Name:     "auth_module",
					Comment:  "Enhanced authentication module",
					Language: "javascript",
					Code:     "function authenticate(user) { return user.isValid(); }",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Module auth_module comment changed")
		assert.Contains(t, result.Changes, "Module auth_module language changed: python -> javascript")
		assert.Contains(t, result.Changes, "Module auth_module code changed")
	})

	t.Run("detect_detailed_package_changes", func(t *testing.T) {
		// Create models with package changes
		prevModel := &unifiedmodel.UnifiedModel{
			Packages: map[string]unifiedmodel.Package{
				"utils_pkg": {
					Name: "utils_pkg",
					Spec: "PACKAGE utils_pkg IS PROCEDURE log(msg VARCHAR2); END;",
					Body: "PACKAGE BODY utils_pkg IS PROCEDURE log(msg VARCHAR2) IS BEGIN NULL; END; END;",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Packages: map[string]unifiedmodel.Package{
				"utils_pkg": {
					Name: "utils_pkg",
					Spec: "PACKAGE utils_pkg IS PROCEDURE log(msg VARCHAR2); FUNCTION format_date(d DATE) RETURN VARCHAR2; END;",
					Body: "PACKAGE BODY utils_pkg IS PROCEDURE log(msg VARCHAR2) IS BEGIN DBMS_OUTPUT.PUT_LINE(msg); END; FUNCTION format_date(d DATE) RETURN VARCHAR2 IS BEGIN RETURN TO_CHAR(d, 'YYYY-MM-DD'); END; END;",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Package utils_pkg specification changed")
		assert.Contains(t, result.Changes, "Package utils_pkg body changed")
	})

	t.Run("detect_detailed_package_body_changes", func(t *testing.T) {
		// Create models with package body changes
		prevModel := &unifiedmodel.UnifiedModel{
			PackageBodies: map[string]unifiedmodel.PackageBody{
				"math_pkg": {
					Name: "math_pkg",
					Body: "PACKAGE BODY math_pkg IS FUNCTION add(a NUMBER, b NUMBER) RETURN NUMBER IS BEGIN RETURN a + b; END; END;",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			PackageBodies: map[string]unifiedmodel.PackageBody{
				"math_pkg": {
					Name: "math_pkg",
					Body: "PACKAGE BODY math_pkg IS FUNCTION add(a NUMBER, b NUMBER) RETURN NUMBER IS BEGIN RETURN NVL(a,0) + NVL(b,0); END; END;",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Package body math_pkg implementation changed")
	})

	t.Run("detect_detailed_macro_changes", func(t *testing.T) {
		// Create models with macro changes
		prevModel := &unifiedmodel.UnifiedModel{
			Macros: map[string]unifiedmodel.Macro{
				"MAX_SIZE": {
					Name:       "MAX_SIZE",
					Definition: "1024",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Macros: map[string]unifiedmodel.Macro{
				"MAX_SIZE": {
					Name:       "MAX_SIZE",
					Definition: "2048",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Macro MAX_SIZE definition changed")
	})

	t.Run("detect_detailed_rule_changes", func(t *testing.T) {
		// Create models with rule changes
		prevModel := &unifiedmodel.UnifiedModel{
			Rules: map[string]unifiedmodel.Rule{
				"update_log": {
					Name:       "update_log",
					Target:     "users",
					Definition: "DO ALSO INSERT INTO audit_log (table_name, action) VALUES ('users', 'UPDATE');",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Rules: map[string]unifiedmodel.Rule{
				"update_log": {
					Name:       "update_log",
					Target:     "accounts",
					Definition: "DO ALSO INSERT INTO audit_log (table_name, action, timestamp) VALUES ('accounts', 'UPDATE', NOW());",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Rule update_log target changed: users -> accounts")
		assert.Contains(t, result.Changes, "Rule update_log definition changed")
	})

	t.Run("detect_detailed_window_func_changes", func(t *testing.T) {
		// Create models with window function changes
		prevModel := &unifiedmodel.UnifiedModel{
			WindowFuncs: map[string]unifiedmodel.WindowFunc{
				"custom_rank": {
					Name:       "custom_rank",
					Definition: "CREATE FUNCTION custom_rank() RETURNS INTEGER AS $$ SELECT ROW_NUMBER() OVER (ORDER BY id) $$;",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			WindowFuncs: map[string]unifiedmodel.WindowFunc{
				"custom_rank": {
					Name:       "custom_rank",
					Definition: "CREATE FUNCTION custom_rank() RETURNS INTEGER AS $$ SELECT DENSE_RANK() OVER (ORDER BY priority DESC, id) $$;",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Window function custom_rank definition changed")
	})

	t.Run("detect_detailed_user_changes", func(t *testing.T) {
		// Create models with user changes
		prevModel := &unifiedmodel.UnifiedModel{
			Users: map[string]unifiedmodel.DBUser{
				"john_doe": {
					Name:   "john_doe",
					Roles:  []string{"reader", "writer"},
					Labels: map[string]string{"department": "engineering", "level": "senior"},
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Users: map[string]unifiedmodel.DBUser{
				"john_doe": {
					Name:   "john_doe",
					Roles:  []string{"reader", "admin"},
					Labels: map[string]string{"department": "engineering", "level": "senior", "team": "backend"},
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "User john_doe roles changed")
		assert.Contains(t, result.Changes, "User john_doe labels changed")
	})

	t.Run("detect_detailed_role_changes", func(t *testing.T) {
		// Create models with role changes
		prevModel := &unifiedmodel.UnifiedModel{
			Roles: map[string]unifiedmodel.DBRole{
				"admin": {
					Name:        "admin",
					Members:     []string{"alice", "bob"},
					ParentRoles: []string{"user"},
					Labels:      map[string]string{"type": "system"},
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Roles: map[string]unifiedmodel.DBRole{
				"admin": {
					Name:        "admin",
					Members:     []string{"alice", "charlie"},
					ParentRoles: []string{"user", "moderator"},
					Labels:      map[string]string{"type": "system", "scope": "global"},
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Role admin members changed")
		assert.Contains(t, result.Changes, "Role admin parent roles changed")
		assert.Contains(t, result.Changes, "Role admin labels changed")
	})

	t.Run("detect_detailed_grant_changes", func(t *testing.T) {
		// Create models with grant changes
		prevModel := &unifiedmodel.UnifiedModel{
			Grants: map[string]unifiedmodel.Grant{
				"select_users": {
					Principal: "john_doe",
					Privilege: "SELECT",
					Scope:     "table",
					Object:    "users",
					Columns:   []string{"id", "name"},
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Grants: map[string]unifiedmodel.Grant{
				"select_users": {
					Principal: "jane_doe",
					Privilege: "SELECT, INSERT",
					Scope:     "table",
					Object:    "accounts",
					Columns:   []string{"id", "name", "email"},
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Grant select_users principal changed: john_doe -> jane_doe")
		assert.Contains(t, result.Changes, "Grant select_users privilege changed: SELECT -> SELECT, INSERT")
		assert.Contains(t, result.Changes, "Grant select_users object changed: users -> accounts")
		assert.Contains(t, result.Changes, "Grant select_users columns changed")
	})

	t.Run("detect_detailed_policy_changes", func(t *testing.T) {
		// Create models with policy changes
		prevModel := &unifiedmodel.UnifiedModel{
			Policies: map[string]unifiedmodel.Policy{
				"row_security": {
					Name:       "row_security",
					Type:       "RLS",
					Scope:      "table",
					Object:     "users",
					Definition: "user_id = current_user_id()",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Policies: map[string]unifiedmodel.Policy{
				"row_security": {
					Name:       "row_security",
					Type:       "PERMISSIVE",
					Scope:      "table",
					Object:     "accounts",
					Definition: "tenant_id = current_tenant_id() AND user_id = current_user_id()",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Policy row_security type changed: RLS -> PERMISSIVE")
		assert.Contains(t, result.Changes, "Policy row_security object changed: users -> accounts")
		assert.Contains(t, result.Changes, "Policy row_security definition changed")
	})

	t.Run("detect_detailed_extent_changes", func(t *testing.T) {
		// Create models with extent changes
		prevModel := &unifiedmodel.UnifiedModel{
			Extents: map[string]unifiedmodel.Extent{
				"data_extent_1": {
					Name: "data_extent_1",
					Size: 1048576, // 1MB
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Extents: map[string]unifiedmodel.Extent{
				"data_extent_1": {
					Name: "data_extent_1",
					Size: 2097152, // 2MB
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Extent data_extent_1 size changed: 1048576 -> 2097152 bytes")
	})

	t.Run("detect_detailed_page_changes", func(t *testing.T) {
		// Create models with page changes
		prevModel := &unifiedmodel.UnifiedModel{
			Pages: map[string]unifiedmodel.Page{
				"page_1": {
					Number: 1,
					Size:   8192, // 8KB
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Pages: map[string]unifiedmodel.Page{
				"page_1": {
					Number: 2,
					Size:   16384, // 16KB
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Page page_1 number changed: 1 -> 2")
		assert.Contains(t, result.Changes, "Page page_1 size changed: 8192 -> 16384 bytes")
	})

	t.Run("detect_detailed_datafile_changes", func(t *testing.T) {
		// Create models with datafile changes
		prevModel := &unifiedmodel.UnifiedModel{
			Datafiles: map[string]unifiedmodel.Datafile{
				"primary_data": {
					Name: "primary_data",
					Path: "/var/lib/db/primary.dbf",
					Size: 104857600, // 100MB
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Datafiles: map[string]unifiedmodel.Datafile{
				"primary_data": {
					Name: "primary_data",
					Path: "/var/lib/db/primary_new.dbf",
					Size: 209715200, // 200MB
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Datafile primary_data path changed: /var/lib/db/primary.dbf -> /var/lib/db/primary_new.dbf")
		assert.Contains(t, result.Changes, "Datafile primary_data size changed: 104857600 -> 209715200 bytes")
	})

	t.Run("detect_detailed_server_changes", func(t *testing.T) {
		// Create models with server changes
		prevModel := &unifiedmodel.UnifiedModel{
			Servers: map[string]unifiedmodel.Server{
				"db_server": {
					Name: "db_server",
					Type: "postgresql",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Servers: map[string]unifiedmodel.Server{
				"db_server": {
					Name: "db_server",
					Type: "mysql",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Server db_server type changed: postgresql -> mysql")
	})

	t.Run("detect_detailed_connection_changes", func(t *testing.T) {
		// Create models with connection changes
		prevModel := &unifiedmodel.UnifiedModel{
			Connections: map[string]unifiedmodel.Connection{
				"main_db": {
					Name:   "main_db",
					Driver: "postgres",
					DSN:    "postgres://user:pass@localhost:5432/db",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Connections: map[string]unifiedmodel.Connection{
				"main_db": {
					Name:   "main_db",
					Driver: "mysql",
					DSN:    "mysql://user:pass@localhost:3306/db",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Connection main_db driver changed: postgres -> mysql")
		assert.Contains(t, result.Changes, "Connection main_db DSN changed")
	})

	t.Run("detect_detailed_endpoint_changes", func(t *testing.T) {
		// Create models with endpoint changes
		prevModel := &unifiedmodel.UnifiedModel{
			Endpoints: map[string]unifiedmodel.Endpoint{
				"api_endpoint": {
					Name:   "api_endpoint",
					Scheme: "http",
					Host:   "localhost",
					Port:   8080,
					Path:   "/api/v1",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Endpoints: map[string]unifiedmodel.Endpoint{
				"api_endpoint": {
					Name:   "api_endpoint",
					Scheme: "https",
					Host:   "api.example.com",
					Port:   443,
					Path:   "/api/v2",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Endpoint api_endpoint scheme changed: http -> https")
		assert.Contains(t, result.Changes, "Endpoint api_endpoint host changed: localhost -> api.example.com")
		assert.Contains(t, result.Changes, "Endpoint api_endpoint port changed: 8080 -> 443")
		assert.Contains(t, result.Changes, "Endpoint api_endpoint path changed: /api/v1 -> /api/v2")
	})

	t.Run("detect_detailed_foreign_data_wrapper_changes", func(t *testing.T) {
		// Create models with foreign data wrapper changes
		prevModel := &unifiedmodel.UnifiedModel{
			ForeignDataWrappers: map[string]unifiedmodel.ForeignDataWrapper{
				"csv_fdw": {
					Name:    "csv_fdw",
					Handler: "csv_fdw_handler",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			ForeignDataWrappers: map[string]unifiedmodel.ForeignDataWrapper{
				"csv_fdw": {
					Name:    "csv_fdw",
					Handler: "enhanced_csv_fdw_handler",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Foreign data wrapper csv_fdw handler changed: csv_fdw_handler -> enhanced_csv_fdw_handler")
	})

	t.Run("detect_detailed_user_mapping_changes", func(t *testing.T) {
		// Create models with user mapping changes
		prevModel := &unifiedmodel.UnifiedModel{
			UserMappings: map[string]unifiedmodel.UserMapping{
				"remote_mapping": {
					User:   "local_user",
					Server: "remote_server",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			UserMappings: map[string]unifiedmodel.UserMapping{
				"remote_mapping": {
					User:   "admin_user",
					Server: "new_remote_server",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "User mapping remote_mapping user changed: local_user -> admin_user")
		assert.Contains(t, result.Changes, "User mapping remote_mapping server changed: remote_server -> new_remote_server")
	})

	t.Run("detect_detailed_federation_changes", func(t *testing.T) {
		// Create models with federation changes
		prevModel := &unifiedmodel.UnifiedModel{
			Federations: map[string]unifiedmodel.Federation{
				"db_federation": {
					Name:    "db_federation",
					Members: []string{"node1", "node2"},
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Federations: map[string]unifiedmodel.Federation{
				"db_federation": {
					Name:    "db_federation",
					Members: []string{"node1", "node3", "node4"},
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Federation db_federation members changed")
	})

	t.Run("detect_detailed_replica_changes", func(t *testing.T) {
		// Create models with replica changes
		prevModel := &unifiedmodel.UnifiedModel{
			Replicas: map[string]unifiedmodel.Replica{
				"read_replica": {
					Name: "read_replica",
					Mode: "async",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Replicas: map[string]unifiedmodel.Replica{
				"read_replica": {
					Name: "read_replica",
					Mode: "sync",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Replica read_replica mode changed: async -> sync")
	})

	t.Run("detect_detailed_cluster_changes", func(t *testing.T) {
		// Create models with cluster changes
		prevModel := &unifiedmodel.UnifiedModel{
			Clusters: map[string]unifiedmodel.Cluster{
				"main_cluster": {
					Name:  "main_cluster",
					Nodes: []string{"node1", "node2", "node3"},
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Clusters: map[string]unifiedmodel.Cluster{
				"main_cluster": {
					Name:  "main_cluster",
					Nodes: []string{"node1", "node2", "node4", "node5"},
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Cluster main_cluster nodes changed")
	})

	t.Run("detect_detailed_task_changes", func(t *testing.T) {
		// Create models with task changes
		prevModel := &unifiedmodel.UnifiedModel{
			Tasks: map[string]unifiedmodel.Task{
				"backup_task": {
					Name:       "backup_task",
					Definition: "BACKUP DATABASE TO '/backup/daily'",
					Schedule:   "0 2 * * *",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Tasks: map[string]unifiedmodel.Task{
				"backup_task": {
					Name:       "backup_task",
					Definition: "BACKUP DATABASE TO '/backup/hourly' WITH COMPRESSION",
					Schedule:   "0 * * * *",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Task backup_task definition changed")
		assert.Contains(t, result.Changes, "Task backup_task schedule changed: 0 2 * * * -> 0 * * * *")
	})

	t.Run("detect_detailed_job_changes", func(t *testing.T) {
		// Create models with job changes
		prevModel := &unifiedmodel.UnifiedModel{
			Jobs: map[string]unifiedmodel.Job{
				"etl_job": {
					Name:     "etl_job",
					Type:     "batch",
					Schedule: "daily",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Jobs: map[string]unifiedmodel.Job{
				"etl_job": {
					Name:     "etl_job",
					Type:     "streaming",
					Schedule: "continuous",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Job etl_job type changed: batch -> streaming")
		assert.Contains(t, result.Changes, "Job etl_job schedule changed: daily -> continuous")
	})

	t.Run("detect_detailed_schedule_changes", func(t *testing.T) {
		// Create models with schedule changes
		prevModel := &unifiedmodel.UnifiedModel{
			Schedules: map[string]unifiedmodel.Schedule{
				"maintenance_schedule": {
					Name: "maintenance_schedule",
					Cron: "0 3 * * 0",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Schedules: map[string]unifiedmodel.Schedule{
				"maintenance_schedule": {
					Name: "maintenance_schedule",
					Cron: "0 2 * * 6",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Schedule maintenance_schedule cron changed: 0 3 * * 0 -> 0 2 * * 6")
	})

	t.Run("detect_detailed_pipeline_changes", func(t *testing.T) {
		// Create models with pipeline changes
		prevModel := &unifiedmodel.UnifiedModel{
			Pipelines: map[string]unifiedmodel.Pipeline{
				"etl_pipeline": {
					Name:  "etl_pipeline",
					Steps: []string{"extract", "transform", "load"},
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Pipelines: map[string]unifiedmodel.Pipeline{
				"etl_pipeline": {
					Name:  "etl_pipeline",
					Steps: []string{"extract", "validate", "transform", "load"},
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Pipeline etl_pipeline steps changed")
	})

	t.Run("detect_detailed_stream_changes", func(t *testing.T) {
		// Create models with stream changes
		prevModel := &unifiedmodel.UnifiedModel{
			Streams: map[string]unifiedmodel.Stream{
				"user_events": {
					Name: "user_events",
					On:   "users_table",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Streams: map[string]unifiedmodel.Stream{
				"user_events": {
					Name: "user_events",
					On:   "accounts_table",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Stream user_events source changed: users_table -> accounts_table")
	})

	t.Run("detect_detailed_event_changes", func(t *testing.T) {
		// Create models with event changes
		prevModel := &unifiedmodel.UnifiedModel{
			Events: map[string]unifiedmodel.Event{
				"user_login": {
					Name:    "user_login",
					Source:  "auth_service",
					Payload: map[string]any{"user_id": "123", "timestamp": "2023-01-01"},
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Events: map[string]unifiedmodel.Event{
				"user_login": {
					Name:    "user_login",
					Source:  "identity_service",
					Payload: map[string]any{"user_id": "123", "timestamp": "2023-01-01", "ip_address": "192.168.1.1"},
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Event user_login source changed: auth_service -> identity_service")
		assert.Contains(t, result.Changes, "Event user_login payload changed")
	})

	t.Run("detect_detailed_notification_changes", func(t *testing.T) {
		// Create models with notification changes
		prevModel := &unifiedmodel.UnifiedModel{
			Notifications: map[string]unifiedmodel.Notification{
				"alert_notification": {
					Name:    "alert_notification",
					Channel: "email",
					Message: "Database connection failed",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Notifications: map[string]unifiedmodel.Notification{
				"alert_notification": {
					Name:    "alert_notification",
					Channel: "slack",
					Message: "Database connection timeout detected",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Notification alert_notification channel changed: email -> slack")
		assert.Contains(t, result.Changes, "Notification alert_notification message changed")
	})

	t.Run("detect_detailed_alert_changes", func(t *testing.T) {
		// Create models with alert changes
		prevModel := &unifiedmodel.UnifiedModel{
			Alerts: map[string]unifiedmodel.Alert{
				"high_cpu": {
					Name:      "high_cpu",
					Condition: "cpu_usage > 80",
					Severity:  "warning",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Alerts: map[string]unifiedmodel.Alert{
				"high_cpu": {
					Name:      "high_cpu",
					Condition: "cpu_usage > 90",
					Severity:  "critical",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Alert high_cpu condition changed")
		assert.Contains(t, result.Changes, "Alert high_cpu severity changed: warning -> critical")
	})

	t.Run("detect_detailed_statistic_changes", func(t *testing.T) {
		// Create models with statistic changes
		prevModel := &unifiedmodel.UnifiedModel{
			Statistics: map[string]unifiedmodel.Statistic{
				"query_count": {
					Name:   "query_count",
					Value:  1000,
					Labels: map[string]string{"database": "prod", "type": "select"},
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Statistics: map[string]unifiedmodel.Statistic{
				"query_count": {
					Name:   "query_count",
					Value:  1500,
					Labels: map[string]string{"database": "prod", "type": "select", "region": "us-east-1"},
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Statistic query_count value changed")
		assert.Contains(t, result.Changes, "Statistic query_count labels changed")
	})

	t.Run("detect_detailed_histogram_changes", func(t *testing.T) {
		// Create models with histogram changes
		prevModel := &unifiedmodel.UnifiedModel{
			Histograms: map[string]unifiedmodel.Histogram{
				"response_time": {
					Name:    "response_time",
					Buckets: map[string]float64{"0.1": 10, "0.5": 50, "1.0": 100},
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Histograms: map[string]unifiedmodel.Histogram{
				"response_time": {
					Name:    "response_time",
					Buckets: map[string]float64{"0.1": 10, "0.5": 50, "1.0": 100, "2.0": 200},
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Histogram response_time buckets changed")
	})

	t.Run("detect_detailed_monitor_changes", func(t *testing.T) {
		// Create models with monitor changes
		prevModel := &unifiedmodel.UnifiedModel{
			Monitors: map[string]unifiedmodel.Monitor{
				"db_monitor": {
					Name:  "db_monitor",
					Scope: "database",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Monitors: map[string]unifiedmodel.Monitor{
				"db_monitor": {
					Name:  "db_monitor",
					Scope: "cluster",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Monitor db_monitor scope changed: database -> cluster")
	})

	t.Run("detect_detailed_monitor_metric_changes", func(t *testing.T) {
		// Create models with monitor metric changes
		prevModel := &unifiedmodel.UnifiedModel{
			MonitorMetrics: map[string]unifiedmodel.MonitorMetric{
				"cpu_usage": {
					Name:   "cpu_usage",
					Unit:   "percent",
					Labels: map[string]string{"host": "server1"},
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			MonitorMetrics: map[string]unifiedmodel.MonitorMetric{
				"cpu_usage": {
					Name:   "cpu_usage",
					Unit:   "ratio",
					Labels: map[string]string{"host": "server1", "region": "us-west-2"},
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Monitor metric cpu_usage unit changed: percent -> ratio")
		assert.Contains(t, result.Changes, "Monitor metric cpu_usage labels changed")
	})

	t.Run("detect_detailed_threshold_changes", func(t *testing.T) {
		// Create models with threshold changes
		prevModel := &unifiedmodel.UnifiedModel{
			Thresholds: map[string]unifiedmodel.Threshold{
				"cpu_threshold": {
					Name:     "cpu_threshold",
					Metric:   "cpu_usage",
					Operator: ">",
					Value:    80,
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Thresholds: map[string]unifiedmodel.Threshold{
				"cpu_threshold": {
					Name:     "cpu_threshold",
					Metric:   "cpu_utilization",
					Operator: ">=",
					Value:    90,
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Threshold cpu_threshold metric changed: cpu_usage -> cpu_utilization")
		assert.Contains(t, result.Changes, "Threshold cpu_threshold operator changed: > -> >=")
		assert.Contains(t, result.Changes, "Threshold cpu_threshold value changed")
	})

	t.Run("detect_detailed_text_search_component_changes", func(t *testing.T) {
		// Create models with text search component changes
		prevModel := &unifiedmodel.UnifiedModel{
			TextSearchComponents: map[string]unifiedmodel.TextSearchComponent{
				"english_config": {
					Name:         "english_config",
					Type:         "configuration",
					Parser:       "default",
					Dictionaries: []string{"english_stem", "simple"},
					Chain:        []string{"standard", "lowercase"},
					Comment:      "English text search configuration",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			TextSearchComponents: map[string]unifiedmodel.TextSearchComponent{
				"english_config": {
					Name:         "english_config",
					Type:         "analyzer",
					Parser:       "enhanced",
					Dictionaries: []string{"english_stem", "simple", "thesaurus"},
					Chain:        []string{"standard", "lowercase", "stop"},
					Comment:      "Enhanced English text search configuration",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Text search component english_config type changed: configuration -> analyzer")
		assert.Contains(t, result.Changes, "Text search component english_config parser changed: default -> enhanced")
		assert.Contains(t, result.Changes, "Text search component english_config dictionaries changed")
		assert.Contains(t, result.Changes, "Text search component english_config chain changed")
		assert.Contains(t, result.Changes, "Text search component english_config comment changed")
	})

	t.Run("detect_detailed_comment_changes", func(t *testing.T) {
		// Create models with comment changes
		prevModel := &unifiedmodel.UnifiedModel{
			Comments: map[string]unifiedmodel.Comment{
				"table_comment": {
					On:      "users",
					Comment: "User information table",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Comments: map[string]unifiedmodel.Comment{
				"table_comment": {
					On:      "accounts",
					Comment: "Account information table with enhanced security",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Comment table_comment target changed: users -> accounts")
		assert.Contains(t, result.Changes, "Comment table_comment text changed")
	})

	t.Run("detect_detailed_annotation_changes", func(t *testing.T) {
		// Create models with annotation changes
		prevModel := &unifiedmodel.UnifiedModel{
			Annotations: map[string]unifiedmodel.Annotation{
				"version_annotation": {
					On:    "users",
					Key:   "version",
					Value: "1.0",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Annotations: map[string]unifiedmodel.Annotation{
				"version_annotation": {
					On:    "accounts",
					Key:   "api_version",
					Value: "2.0",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Annotation version_annotation target changed: users -> accounts")
		assert.Contains(t, result.Changes, "Annotation version_annotation key changed: version -> api_version")
		assert.Contains(t, result.Changes, "Annotation version_annotation value changed")
	})

	t.Run("detect_detailed_tag_changes", func(t *testing.T) {
		// Create models with tag changes
		prevModel := &unifiedmodel.UnifiedModel{
			Tags: map[string]unifiedmodel.Tag{
				"production_tag": {
					On:   "users",
					Name: "production",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Tags: map[string]unifiedmodel.Tag{
				"production_tag": {
					On:   "accounts",
					Name: "prod",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Tag production_tag target changed: users -> accounts")
		assert.Contains(t, result.Changes, "Tag production_tag name changed: production -> prod")
	})

	t.Run("detect_detailed_alias_changes", func(t *testing.T) {
		// Create models with alias changes
		prevModel := &unifiedmodel.UnifiedModel{
			Aliases: map[string]unifiedmodel.Alias{
				"user_alias": {
					On:    "users",
					Alias: "u",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Aliases: map[string]unifiedmodel.Alias{
				"user_alias": {
					On:    "accounts",
					Alias: "acc",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Alias user_alias target changed: users -> accounts")
		assert.Contains(t, result.Changes, "Alias user_alias value changed: u -> acc")
	})

	t.Run("detect_detailed_synonym_changes", func(t *testing.T) {
		// Create models with synonym changes
		prevModel := &unifiedmodel.UnifiedModel{
			Synonyms: map[string]unifiedmodel.Synonym{
				"user_synonym": {
					On:   "users",
					Name: "people",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Synonyms: map[string]unifiedmodel.Synonym{
				"user_synonym": {
					On:   "accounts",
					Name: "customers",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Synonym user_synonym target changed: users -> accounts")
		assert.Contains(t, result.Changes, "Synonym user_synonym name changed: people -> customers")
	})

	t.Run("detect_detailed_label_changes", func(t *testing.T) {
		// Create models with label changes
		prevModel := &unifiedmodel.UnifiedModel{
			Labels: map[string]unifiedmodel.Label{
				"user_label": {
					On:    "users",
					Name:  "User",
					Props: map[string]string{"category": "person", "type": "entity"},
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Labels: map[string]unifiedmodel.Label{
				"user_label": {
					On:    "accounts",
					Name:  "Account",
					Props: map[string]string{"category": "person", "type": "entity", "scope": "global"},
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Label user_label target changed: users -> accounts")
		assert.Contains(t, result.Changes, "Label user_label name changed: User -> Account")
		assert.Contains(t, result.Changes, "Label user_label properties changed")
	})

	t.Run("detect_detailed_snapshot_changes", func(t *testing.T) {
		// Create models with snapshot changes
		prevModel := &unifiedmodel.UnifiedModel{
			Snapshots: map[string]unifiedmodel.Snapshot{
				"daily_snapshot": {
					Name:  "daily_snapshot",
					Scope: "database",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Snapshots: map[string]unifiedmodel.Snapshot{
				"daily_snapshot": {
					Name:  "daily_snapshot",
					Scope: "instance",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Snapshot daily_snapshot scope changed: database -> instance")
	})

	t.Run("detect_detailed_backup_changes", func(t *testing.T) {
		// Create models with backup changes
		prevModel := &unifiedmodel.UnifiedModel{
			Backups: map[string]unifiedmodel.Backup{
				"weekly_backup": {
					Name:   "weekly_backup",
					Method: "full",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Backups: map[string]unifiedmodel.Backup{
				"weekly_backup": {
					Name:   "weekly_backup",
					Method: "incremental",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Backup weekly_backup method changed: full -> incremental")
	})

	t.Run("detect_detailed_archive_changes", func(t *testing.T) {
		// Create models with archive changes
		prevModel := &unifiedmodel.UnifiedModel{
			Archives: map[string]unifiedmodel.Archive{
				"monthly_archive": {
					Name:   "monthly_archive",
					Format: "tar.gz",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Archives: map[string]unifiedmodel.Archive{
				"monthly_archive": {
					Name:   "monthly_archive",
					Format: "zip",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Archive monthly_archive format changed: tar.gz -> zip")
	})

	t.Run("detect_detailed_recovery_point_changes", func(t *testing.T) {
		// Create models with recovery point changes
		prevModel := &unifiedmodel.UnifiedModel{
			RecoveryPoints: map[string]unifiedmodel.RecoveryPoint{
				"checkpoint_1": {
					Name:  "checkpoint_1",
					Point: "LSN:1000",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			RecoveryPoints: map[string]unifiedmodel.RecoveryPoint{
				"checkpoint_1": {
					Name:  "checkpoint_1",
					Point: "LSN:2000",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Recovery point checkpoint_1 location changed: LSN:1000 -> LSN:2000")
	})

	t.Run("detect_detailed_version_changes", func(t *testing.T) {
		// Create models with version changes
		prevModel := &unifiedmodel.UnifiedModel{
			Versions: map[string]unifiedmodel.VersionNode{
				"v1.0": {
					ID:      "abc123",
					Parents: []string{"def456"},
					Message: "Initial version",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Versions: map[string]unifiedmodel.VersionNode{
				"v1.0": {
					ID:      "xyz789",
					Parents: []string{"def456", "ghi012"},
					Message: "Updated initial version with fixes",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Version v1.0 ID changed: abc123 -> xyz789")
		assert.Contains(t, result.Changes, "Version v1.0 parents changed")
		assert.Contains(t, result.Changes, "Version v1.0 message changed")
	})

	t.Run("detect_detailed_migration_changes", func(t *testing.T) {
		// Create models with migration changes
		prevModel := &unifiedmodel.UnifiedModel{
			Migrations: map[string]unifiedmodel.Migration{
				"001_create_users": {
					ID:          "001",
					Description: "Create users table",
					Script:      "CREATE TABLE users (id INT PRIMARY KEY);",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Migrations: map[string]unifiedmodel.Migration{
				"001_create_users": {
					ID:          "001_v2",
					Description: "Create users table with enhanced schema",
					Script:      "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255) UNIQUE);",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Migration 001_create_users ID changed: 001 -> 001_v2")
		assert.Contains(t, result.Changes, "Migration 001_create_users description changed")
		assert.Contains(t, result.Changes, "Migration 001_create_users script changed")
	})

	t.Run("detect_detailed_branch_changes", func(t *testing.T) {
		// Create models with branch changes
		prevModel := &unifiedmodel.UnifiedModel{
			Branches: map[string]unifiedmodel.Branch{
				"feature_branch": {
					Name: "feature/user-auth",
					From: "main",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Branches: map[string]unifiedmodel.Branch{
				"feature_branch": {
					Name: "feature/enhanced-auth",
					From: "develop",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Branch feature_branch name changed: feature/user-auth -> feature/enhanced-auth")
		assert.Contains(t, result.Changes, "Branch feature_branch source changed: main -> develop")
	})

	t.Run("detect_detailed_time_travel_changes", func(t *testing.T) {
		// Create models with time travel changes
		prevModel := &unifiedmodel.UnifiedModel{
			TimeTravel: map[string]unifiedmodel.TimeTravel{
				"users_snapshot": {
					Object: "users",
					AsOf:   "2023-01-01T00:00:00Z",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			TimeTravel: map[string]unifiedmodel.TimeTravel{
				"users_snapshot": {
					Object: "accounts",
					AsOf:   "2023-06-01T00:00:00Z",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Time travel users_snapshot object changed: users -> accounts")
		assert.Contains(t, result.Changes, "Time travel users_snapshot timestamp changed: 2023-01-01T00:00:00Z -> 2023-06-01T00:00:00Z")
	})

	t.Run("detect_detailed_plugin_changes", func(t *testing.T) {
		// Create models with plugin changes
		prevModel := &unifiedmodel.UnifiedModel{
			Plugins: map[string]unifiedmodel.Plugin{
				"auth_plugin": {
					Name:    "authentication",
					Version: "1.0.0",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Plugins: map[string]unifiedmodel.Plugin{
				"auth_plugin": {
					Name:    "enhanced_authentication",
					Version: "2.1.0",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Plugin auth_plugin name changed: authentication -> enhanced_authentication")
		assert.Contains(t, result.Changes, "Plugin auth_plugin version changed: 1.0.0 -> 2.1.0")
	})

	t.Run("detect_detailed_module_extension_changes", func(t *testing.T) {
		// Create models with module extension changes
		prevModel := &unifiedmodel.UnifiedModel{
			ModuleExtensions: map[string]unifiedmodel.ModuleExtension{
				"crypto_ext": {
					Name:    "crypto_functions",
					Module:  "security",
					Version: "1.5.0",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			ModuleExtensions: map[string]unifiedmodel.ModuleExtension{
				"crypto_ext": {
					Name:    "advanced_crypto_functions",
					Module:  "enhanced_security",
					Version: "2.0.0",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Module extension crypto_ext name changed: crypto_functions -> advanced_crypto_functions")
		assert.Contains(t, result.Changes, "Module extension crypto_ext module changed: security -> enhanced_security")
		assert.Contains(t, result.Changes, "Module extension crypto_ext version changed: 1.5.0 -> 2.0.0")
	})

	t.Run("detect_detailed_ttl_setting_changes", func(t *testing.T) {
		// Create models with TTL setting changes
		prevModel := &unifiedmodel.UnifiedModel{
			TTLSettings: map[string]unifiedmodel.TTLSetting{
				"session_ttl": {
					Name:   "session_expiry",
					Scope:  "table",
					Policy: "delete_after_30_days",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			TTLSettings: map[string]unifiedmodel.TTLSetting{
				"session_ttl": {
					Name:   "enhanced_session_expiry",
					Scope:  "collection",
					Policy: "delete_after_7_days",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "TTL setting session_ttl name changed: session_expiry -> enhanced_session_expiry")
		assert.Contains(t, result.Changes, "TTL setting session_ttl scope changed: table -> collection")
		assert.Contains(t, result.Changes, "TTL setting session_ttl policy changed: delete_after_30_days -> delete_after_7_days")
	})

	t.Run("detect_detailed_dimension_changes", func(t *testing.T) {
		// Create models with dimension changes
		prevModel := &unifiedmodel.UnifiedModel{
			Dimensions: map[string]unifiedmodel.DimensionSpec{
				"embedding_dim": {
					Name: "text_embedding",
					Size: 768,
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Dimensions: map[string]unifiedmodel.DimensionSpec{
				"embedding_dim": {
					Name: "enhanced_text_embedding",
					Size: 1024,
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Dimension embedding_dim name changed: text_embedding -> enhanced_text_embedding")
		assert.Contains(t, result.Changes, "Dimension embedding_dim size changed: 768 -> 1024")
	})

	t.Run("detect_detailed_distance_metric_changes", func(t *testing.T) {
		// Create models with distance metric changes
		prevModel := &unifiedmodel.UnifiedModel{
			DistanceMetrics: map[string]unifiedmodel.DistanceMetricSpec{
				"similarity_metric": {
					Name:   "cosine_similarity",
					Method: "cosine",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			DistanceMetrics: map[string]unifiedmodel.DistanceMetricSpec{
				"similarity_metric": {
					Name:   "euclidean_distance",
					Method: "l2",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Distance metric similarity_metric name changed: cosine_similarity -> euclidean_distance")
		assert.Contains(t, result.Changes, "Distance metric similarity_metric method changed: cosine -> l2")
	})

	t.Run("detect_detailed_projection_changes", func(t *testing.T) {
		// Create models with projection changes
		prevModel := &unifiedmodel.UnifiedModel{
			Projections: map[string]unifiedmodel.Projection{
				"user_summary": {
					Name:       "user_summary_view",
					Definition: "SELECT id, name, email FROM users",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Projections: map[string]unifiedmodel.Projection{
				"user_summary": {
					Name:       "enhanced_user_summary_view",
					Definition: "SELECT id, name, email, created_at FROM users WHERE active = true",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Projection user_summary name changed: user_summary_view -> enhanced_user_summary_view")
		assert.Contains(t, result.Changes, "Projection user_summary definition changed")
	})

	t.Run("detect_detailed_analytics_agg_changes", func(t *testing.T) {
		// Create models with analytics aggregation changes
		prevModel := &unifiedmodel.UnifiedModel{
			AnalyticsAggs: map[string]unifiedmodel.AggregationOp{
				"daily_stats": {
					Name:       "daily_user_stats",
					Definition: "COUNT(*) GROUP BY DATE(created_at)",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			AnalyticsAggs: map[string]unifiedmodel.AggregationOp{
				"daily_stats": {
					Name:       "enhanced_daily_user_stats",
					Definition: "COUNT(*), AVG(age) GROUP BY DATE(created_at), region",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Analytics aggregation daily_stats name changed: daily_user_stats -> enhanced_daily_user_stats")
		assert.Contains(t, result.Changes, "Analytics aggregation daily_stats definition changed")
	})

	t.Run("detect_detailed_temporary_table_changes", func(t *testing.T) {
		// Create models with temporary table changes
		prevModel := &unifiedmodel.UnifiedModel{
			TemporaryTables: map[string]unifiedmodel.TemporaryTable{
				"temp_users": {
					Name:  "temp_users",
					Scope: "session",
					Columns: map[string]unifiedmodel.Column{
						"id": {
							Name:     "id",
							DataType: "INT",
							Nullable: false,
						},
					},
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			TemporaryTables: map[string]unifiedmodel.TemporaryTable{
				"temp_users": {
					Name:  "temp_user_data",
					Scope: "transaction",
					Columns: map[string]unifiedmodel.Column{
						"id": {
							Name:     "id",
							DataType: "BIGINT",
							Nullable: false,
						},
						"email": {
							Name:     "email",
							DataType: "VARCHAR(255)",
							Nullable: true,
						},
					},
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Temporary table temp_users name changed: temp_users -> temp_user_data")
		assert.Contains(t, result.Changes, "Temporary table temp_users scope changed: session -> transaction")
		assert.Contains(t, result.Changes, "Column temp_users.id data type changed: INT -> BIGINT")
		assert.Contains(t, result.Changes, "Added column: temp_users.email")
	})

	t.Run("detect_detailed_transient_table_changes", func(t *testing.T) {
		// Create models with transient table changes
		prevModel := &unifiedmodel.UnifiedModel{
			TransientTables: map[string]unifiedmodel.TransientTable{
				"transient_logs": {
					Name: "transient_logs",
					Columns: map[string]unifiedmodel.Column{
						"timestamp": {
							Name:     "timestamp",
							DataType: "TIMESTAMP",
							Nullable: false,
						},
					},
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			TransientTables: map[string]unifiedmodel.TransientTable{
				"transient_logs": {
					Name: "enhanced_transient_logs",
					Columns: map[string]unifiedmodel.Column{
						"timestamp": {
							Name:     "timestamp",
							DataType: "TIMESTAMPTZ",
							Nullable: false,
						},
						"level": {
							Name:     "level",
							DataType: "VARCHAR(10)",
							Nullable: false,
						},
					},
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Transient table transient_logs name changed: transient_logs -> enhanced_transient_logs")
		assert.Contains(t, result.Changes, "Column transient_logs.timestamp data type changed: TIMESTAMP -> TIMESTAMPTZ")
		assert.Contains(t, result.Changes, "Added column: transient_logs.level")
	})

	t.Run("detect_detailed_cache_changes", func(t *testing.T) {
		// Create models with cache changes
		prevModel := &unifiedmodel.UnifiedModel{
			Caches: map[string]unifiedmodel.Cache{
				"query_cache": {
					Name:  "query_cache",
					Scope: "session",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			Caches: map[string]unifiedmodel.Cache{
				"query_cache": {
					Name:  "enhanced_query_cache",
					Scope: "global",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Cache query_cache name changed: query_cache -> enhanced_query_cache")
		assert.Contains(t, result.Changes, "Cache query_cache scope changed: session -> global")
	})

	t.Run("detect_detailed_window_view_changes", func(t *testing.T) {
		// Create models with window view changes
		prevModel := &unifiedmodel.UnifiedModel{
			WindowViews: map[string]unifiedmodel.WindowView{
				"user_activity_window": {
					Name:       "user_activity_window",
					Definition: "SELECT user_id, COUNT(*) FROM events",
					WindowSpec: "PARTITION BY user_id ORDER BY timestamp ROWS BETWEEN 10 PRECEDING AND CURRENT ROW",
				},
			},
		}

		currModel := &unifiedmodel.UnifiedModel{
			WindowViews: map[string]unifiedmodel.WindowView{
				"user_activity_window": {
					Name:       "enhanced_user_activity_window",
					Definition: "SELECT user_id, COUNT(*), AVG(duration) FROM events",
					WindowSpec: "PARTITION BY user_id, region ORDER BY timestamp ROWS BETWEEN 50 PRECEDING AND CURRENT ROW",
				},
			},
		}

		result, err := comparator.CompareUnifiedModels(prevModel, currModel)
		assert.NoError(t, err)
		assert.True(t, result.HasChanges)
		assert.Contains(t, result.Changes, "Window view user_activity_window name changed: user_activity_window -> enhanced_user_activity_window")
		assert.Contains(t, result.Changes, "Window view user_activity_window definition changed")
		assert.Contains(t, result.Changes, "Window view user_activity_window window specification changed: PARTITION BY user_id ORDER BY timestamp ROWS BETWEEN 10 PRECEDING AND CURRENT ROW -> PARTITION BY user_id, region ORDER BY timestamp ROWS BETWEEN 50 PRECEDING AND CURRENT ROW")
	})
}
