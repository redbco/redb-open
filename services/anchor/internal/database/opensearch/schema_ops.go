package opensearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// SchemaOps implements adapter.SchemaOperator for OpenSearch.
type SchemaOps struct {
	conn *Connection
}

// DiscoverSchema discovers the unified schema for the OpenSearch index.
func (s *SchemaOps) DiscoverSchema(ctx context.Context) (*unifiedmodel.UnifiedModel, error) {
	if !s.conn.IsConnected() {
		return nil, adapter.ErrConnectionClosed
	}

	model := &unifiedmodel.UnifiedModel{
		Tables:          make(map[string]unifiedmodel.Table),
		SearchIndexes:   make(map[string]unifiedmodel.SearchIndex),
		SearchDocuments: make(map[string]unifiedmodel.SearchDocument),
	}

	// Get mapping for the index
	res, err := s.conn.client.Indices.GetMapping(
		s.conn.client.Indices.GetMapping.WithContext(ctx),
		s.conn.client.Indices.GetMapping.WithIndex(s.conn.indexName),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get mapping: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("failed to get mapping: %s", res.Status())
	}

	// Parse mapping response
	var mappingResp map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&mappingResp); err != nil {
		return nil, err
	}

	// Extract properties from mapping
	indexMapping, ok := mappingResp[s.conn.indexName].(map[string]interface{})
	if !ok {
		return model, nil
	}

	mappings, ok := indexMapping["mappings"].(map[string]interface{})
	if !ok {
		return model, nil
	}

	properties, ok := mappings["properties"].(map[string]interface{})
	if !ok {
		return model, nil
	}

	// Convert properties to columns
	columnsMap := make(map[string]unifiedmodel.Column)
	for fieldName, fieldDef := range properties {
		fieldDefMap, ok := fieldDef.(map[string]interface{})
		if !ok {
			continue
		}

		dataType, _ := fieldDefMap["type"].(string)

		column := unifiedmodel.Column{
			Name:     fieldName,
			DataType: dataType,
			Nullable: true, // OpenSearch fields are nullable by default
		}

		columnsMap[fieldName] = column
	}

	// Create a single "table" representing the index
	table := unifiedmodel.Table{
		Name:    s.conn.indexName,
		Columns: columnsMap,
		Indexes: make(map[string]unifiedmodel.Index),
		Options: map[string]interface{}{
			"index_type": "opensearch",
		},
	}

	model.Tables[s.conn.indexName] = table

	// Also create SearchIndex and SearchDocument (primary containers for search engines)
	searchIndex := unifiedmodel.SearchIndex{
		Name:   s.conn.indexName,
		Fields: []string{},
	}

	searchDoc := unifiedmodel.SearchDocument{
		Name:       s.conn.indexName,
		DocumentID: s.conn.indexName,
		Index:      s.conn.indexName,
		Fields:     make(map[string]unifiedmodel.Field),
		Type:       "opensearch",
	}

	for fieldName, column := range columnsMap {
		searchIndex.Fields = append(searchIndex.Fields, fieldName)
		searchDoc.Fields[fieldName] = unifiedmodel.Field{
			Name:     fieldName,
			Type:     column.DataType,
			Required: !column.Nullable,
		}
	}

	model.SearchIndexes[s.conn.indexName] = searchIndex
	model.SearchDocuments[s.conn.indexName] = searchDoc

	return model, nil
}

// GetTableSchema retrieves schema information for the index (treated as a table).
func (s *SchemaOps) GetTableSchema(ctx context.Context, name string) (*unifiedmodel.Table, error) {
	if !s.conn.IsConnected() {
		return nil, adapter.ErrConnectionClosed
	}

	// In OpenSearch, we only have one "table" (the index)
	if name != s.conn.indexName {
		return nil, fmt.Errorf("index not found: %s", name)
	}

	model, err := s.DiscoverSchema(ctx)
	if err != nil {
		return nil, err
	}

	if table, ok := model.Tables[name]; ok {
		return &table, nil
	}

	return nil, fmt.Errorf("index not found: %s", name)
}

// CreateStructure creates mappings from a UnifiedModel.
func (s *SchemaOps) CreateStructure(ctx context.Context, model *unifiedmodel.UnifiedModel) error {
	if !s.conn.IsConnected() {
		return adapter.ErrConnectionClosed
	}

	// Create tables (indices) from model
	for _, table := range model.Tables {
		if err := s.CreateTable(ctx, &table); err != nil {
			return fmt.Errorf("failed to create index %s: %w", table.Name, err)
		}
	}

	return nil
}

// ListTables lists all "tables" (in OpenSearch, just the index name).
func (s *SchemaOps) ListTables(ctx context.Context) ([]string, error) {
	if !s.conn.IsConnected() {
		return nil, adapter.ErrConnectionClosed
	}

	// In OpenSearch, there's only one "table" per connection (the index)
	return []string{s.conn.indexName}, nil
}

// CreateTable creates a new mapping in the OpenSearch index.
func (s *SchemaOps) CreateTable(ctx context.Context, table *unifiedmodel.Table) error {
	if !s.conn.IsConnected() {
		return adapter.ErrConnectionClosed
	}

	// Build mapping from table definition
	properties := make(map[string]interface{})

	for _, col := range table.Columns {
		fieldDef := map[string]interface{}{
			"type": col.DataType,
		}

		properties[col.Name] = fieldDef
	}

	mapping := map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": properties,
		},
	}

	// Create index with mapping
	mappingJSON, err := json.Marshal(mapping)
	if err != nil {
		return err
	}

	res, err := s.conn.client.Indices.Create(
		table.Name,
		s.conn.client.Indices.Create.WithContext(ctx),
		s.conn.client.Indices.Create.WithBody(bytes.NewReader(mappingJSON)),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("failed to create index: %s", res.Status())
	}

	return nil
}

// DropTable drops the OpenSearch index.
func (s *SchemaOps) DropTable(ctx context.Context, name string) error {
	if !s.conn.IsConnected() {
		return adapter.ErrConnectionClosed
	}

	res, err := s.conn.client.Indices.Delete(
		[]string{name},
		s.conn.client.Indices.Delete.WithContext(ctx),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("failed to delete index: %s", res.Status())
	}

	return nil
}
