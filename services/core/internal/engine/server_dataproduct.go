package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

// ListDataProducts lists all data products in a workspace
func (s *Server) ListDataProducts(ctx context.Context, req *corev1.ListDataProductsRequest) (*corev1.ListDataProductsResponse, error) {
	defer s.trackOperation()()

	if req.TenantId == "" || req.WorkspaceName == "" {
		return &corev1.ListDataProductsResponse{
			Message: "tenant_id and workspace_name are required",
			Success: false,
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	// Get workspace_id from workspace_name
	var workspaceID string
	err := s.engine.db.Pool().QueryRow(ctx, `
		SELECT workspace_id 
		FROM workspaces 
		WHERE tenant_id = $1 AND workspace_name = $2
	`, req.TenantId, req.WorkspaceName).Scan(&workspaceID)

	if err != nil {
		if err == sql.ErrNoRows {
			return &corev1.ListDataProductsResponse{
				Message: "Workspace not found",
				Success: false,
				Status:  commonv1.Status_STATUS_ERROR,
			}, nil
		}
		return &corev1.ListDataProductsResponse{
			Message: fmt.Sprintf("Failed to get workspace: %v", err),
			Success: false,
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	// Query data products with their resource items
	rows, err := s.engine.db.Pool().Query(ctx, `
		SELECT 
			dp.product_id,
			dp.tenant_id,
			dp.workspace_id,
			dp.product_name,
			dp.product_description,
			dp.metadata,
			dp.owner_id,
			dp.status::text,
			dp.created,
			dp.updated
		FROM data_products dp
		WHERE dp.workspace_id = $1
		ORDER BY dp.created DESC
	`, workspaceID)

	if err != nil {
		return &corev1.ListDataProductsResponse{
			Message: fmt.Sprintf("Failed to list data products: %v", err),
			Success: false,
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}
	defer rows.Close()

	var products []*corev1.DataProduct
	for rows.Next() {
		var product corev1.DataProduct
		var metadataJSON []byte
		var created, updated time.Time

		err := rows.Scan(
			&product.ProductId,
			&product.TenantId,
			&product.WorkspaceId,
			&product.ProductName,
			&product.ProductDescription,
			&metadataJSON,
			&product.OwnerId,
			&product.Status,
			&created,
			&updated,
		)
		if err != nil {
			continue
		}

		product.Created = created.Format(time.RFC3339)
		product.Updated = updated.Format(time.RFC3339)

		// Parse metadata
		if len(metadataJSON) > 0 {
			var metadataMap map[string]interface{}
			if err := json.Unmarshal(metadataJSON, &metadataMap); err == nil {
				product.Metadata, _ = structpb.NewStruct(metadataMap)
			}
		}

		// Get resource items for this product
		product.ResourceItems, _ = s.getResourceItemsForProduct(ctx, product.ProductId)

		products = append(products, &product)
	}

	return &corev1.ListDataProductsResponse{
		DataProducts: products,
		Message:      fmt.Sprintf("Found %d data products", len(products)),
		Success:      true,
		Status:       commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// GetDataProduct gets a specific data product by name
func (s *Server) GetDataProduct(ctx context.Context, req *corev1.GetDataProductRequest) (*corev1.GetDataProductResponse, error) {
	defer s.trackOperation()()

	if req.TenantId == "" || req.WorkspaceName == "" || req.ProductName == "" {
		return &corev1.GetDataProductResponse{
			Message: "tenant_id, workspace_name, and product_name are required",
			Success: false,
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	// Get workspace_id
	var workspaceID string
	err := s.engine.db.Pool().QueryRow(ctx, `
		SELECT workspace_id 
		FROM workspaces 
		WHERE tenant_id = $1 AND workspace_name = $2
	`, req.TenantId, req.WorkspaceName).Scan(&workspaceID)

	if err != nil {
		return &corev1.GetDataProductResponse{
			Message: "Workspace not found",
			Success: false,
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	// Query the data product
	var product corev1.DataProduct
	var metadataJSON []byte
	var created, updated time.Time

	err = s.engine.db.Pool().QueryRow(ctx, `
		SELECT 
			product_id,
			tenant_id,
			workspace_id,
			product_name,
			product_description,
			metadata,
			owner_id,
			status::text,
			created,
			updated
		FROM data_products
		WHERE workspace_id = $1 AND product_name = $2
	`, workspaceID, req.ProductName).Scan(
		&product.ProductId,
		&product.TenantId,
		&product.WorkspaceId,
		&product.ProductName,
		&product.ProductDescription,
		&metadataJSON,
		&product.OwnerId,
		&product.Status,
		&created,
		&updated,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return &corev1.GetDataProductResponse{
				Message: "Data product not found",
				Success: false,
				Status:  commonv1.Status_STATUS_ERROR,
			}, nil
		}
		return &corev1.GetDataProductResponse{
			Message: fmt.Sprintf("Failed to get data product: %v", err),
			Success: false,
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	product.Created = created.Format(time.RFC3339)
	product.Updated = updated.Format(time.RFC3339)

	// Parse metadata
	if len(metadataJSON) > 0 {
		var metadataMap map[string]interface{}
		if err := json.Unmarshal(metadataJSON, &metadataMap); err == nil {
			product.Metadata, _ = structpb.NewStruct(metadataMap)
		}
	}

	// Get resource items
	product.ResourceItems, _ = s.getResourceItemsForProduct(ctx, product.ProductId)

	return &corev1.GetDataProductResponse{
		DataProduct: &product,
		Message:     "Data product retrieved successfully",
		Success:     true,
		Status:      commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// CreateDataProduct creates a new data product
func (s *Server) CreateDataProduct(ctx context.Context, req *corev1.CreateDataProductRequest) (*corev1.CreateDataProductResponse, error) {
	defer s.trackOperation()()

	if req.TenantId == "" || req.WorkspaceName == "" || req.ProductName == "" {
		return &corev1.CreateDataProductResponse{
			Message: "tenant_id, workspace_name, and product_name are required",
			Success: false,
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	if len(req.ResourceItemIds) == 0 {
		return &corev1.CreateDataProductResponse{
			Message: "At least one resource item is required",
			Success: false,
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	// Get workspace_id
	var workspaceID string
	err := s.engine.db.Pool().QueryRow(ctx, `
		SELECT workspace_id 
		FROM workspaces 
		WHERE tenant_id = $1 AND workspace_name = $2
	`, req.TenantId, req.WorkspaceName).Scan(&workspaceID)

	if err != nil {
		return &corev1.CreateDataProductResponse{
			Message: "Workspace not found",
			Success: false,
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	// Convert metadata to JSON
	var metadataJSON []byte
	if req.Metadata != nil {
		metadataJSON, _ = json.Marshal(req.Metadata.AsMap())
	} else {
		metadataJSON = []byte("{}")
	}

	// Start transaction
	tx, err := s.engine.db.Pool().Begin(ctx)
	if err != nil {
		return &corev1.CreateDataProductResponse{
			Message: fmt.Sprintf("Failed to start transaction: %v", err),
			Success: false,
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}
	defer tx.Rollback(ctx)

	// Create the data product
	var product corev1.DataProduct
	var created, updated time.Time

	err = tx.QueryRow(ctx, `
		INSERT INTO data_products (
			tenant_id,
			workspace_id,
			product_name,
			product_description,
			metadata,
			owner_id,
			status
		) VALUES ($1, $2, $3, $4, $5, $6, 'STATUS_CREATED')
		RETURNING product_id, tenant_id, workspace_id, product_name, product_description, 
				  metadata, owner_id, status::text, created, updated
	`, req.TenantId, workspaceID, req.ProductName, req.ProductDescription, metadataJSON, req.OwnerId).Scan(
		&product.ProductId,
		&product.TenantId,
		&product.WorkspaceId,
		&product.ProductName,
		&product.ProductDescription,
		&metadataJSON,
		&product.OwnerId,
		&product.Status,
		&created,
		&updated,
	)

	if err != nil {
		return &corev1.CreateDataProductResponse{
			Message: fmt.Sprintf("Failed to create data product: %v", err),
			Success: false,
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	product.Created = created.Format(time.RFC3339)
	product.Updated = updated.Format(time.RFC3339)

	// Parse metadata
	if len(metadataJSON) > 0 {
		var metadataMap map[string]interface{}
		if err := json.Unmarshal(metadataJSON, &metadataMap); err == nil {
			product.Metadata, _ = structpb.NewStruct(metadataMap)
		}
	}

	// Add resource items
	for i, itemID := range req.ResourceItemIds {
		_, err := tx.Exec(ctx, `
			INSERT INTO data_product_items (product_id, resource_item_id, item_order)
			VALUES ($1, $2, $3)
		`, product.ProductId, itemID, i)

		if err != nil {
			return &corev1.CreateDataProductResponse{
				Message: fmt.Sprintf("Failed to add resource item: %v", err),
				Success: false,
				Status:  commonv1.Status_STATUS_ERROR,
			}, nil
		}
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return &corev1.CreateDataProductResponse{
			Message: fmt.Sprintf("Failed to commit transaction: %v", err),
			Success: false,
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	// Get full product with items
	product.ResourceItems, _ = s.getResourceItemsForProduct(ctx, product.ProductId)

	return &corev1.CreateDataProductResponse{
		DataProduct: &product,
		Message:     "Data product created successfully",
		Success:     true,
		Status:      commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// ModifyDataProduct modifies an existing data product
func (s *Server) ModifyDataProduct(ctx context.Context, req *corev1.ModifyDataProductRequest) (*corev1.ModifyDataProductResponse, error) {
	defer s.trackOperation()()

	if req.TenantId == "" || req.WorkspaceName == "" || req.ProductName == "" {
		return &corev1.ModifyDataProductResponse{
			Message: "tenant_id, workspace_name, and product_name are required",
			Success: false,
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	// Get workspace_id
	var workspaceID string
	err := s.engine.db.Pool().QueryRow(ctx, `
		SELECT workspace_id 
		FROM workspaces 
		WHERE tenant_id = $1 AND workspace_name = $2
	`, req.TenantId, req.WorkspaceName).Scan(&workspaceID)

	if err != nil {
		return &corev1.ModifyDataProductResponse{
			Message: "Workspace not found",
			Success: false,
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	// Get product_id
	var productID string
	err = s.engine.db.Pool().QueryRow(ctx, `
		SELECT product_id FROM data_products
		WHERE workspace_id = $1 AND product_name = $2
	`, workspaceID, req.ProductName).Scan(&productID)

	if err != nil {
		return &corev1.ModifyDataProductResponse{
			Message: "Data product not found",
			Success: false,
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	// Start transaction
	tx, err := s.engine.db.Pool().Begin(ctx)
	if err != nil {
		return &corev1.ModifyDataProductResponse{
			Message: fmt.Sprintf("Failed to start transaction: %v", err),
			Success: false,
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}
	defer tx.Rollback(ctx)

	// Build update query dynamically based on provided fields
	updates := []string{}
	args := []interface{}{productID}
	argIdx := 2

	if req.ProductDescription != "" {
		updates = append(updates, fmt.Sprintf("product_description = $%d", argIdx))
		args = append(args, req.ProductDescription)
		argIdx++
	}

	if req.Status != "" {
		updates = append(updates, fmt.Sprintf("status = $%d::status_enum", argIdx))
		args = append(args, req.Status)
		argIdx++
	}

	if req.Metadata != nil {
		metadataJSON, _ := json.Marshal(req.Metadata.AsMap())
		updates = append(updates, fmt.Sprintf("metadata = $%d", argIdx))
		args = append(args, metadataJSON)
		argIdx++
	}

	// Always update the updated timestamp
	updates = append(updates, "updated = CURRENT_TIMESTAMP")

	if len(updates) > 0 {
		query := fmt.Sprintf(`
			UPDATE data_products
			SET %s
			WHERE product_id = $1
		`, joinStrings(updates, ", "))

		_, err = tx.Exec(ctx, query, args...)
		if err != nil {
			return &corev1.ModifyDataProductResponse{
				Message: fmt.Sprintf("Failed to update data product: %v", err),
				Success: false,
				Status:  commonv1.Status_STATUS_ERROR,
			}, nil
		}
	}

	// Update resource items if provided
	if len(req.ResourceItemIds) > 0 {
		// Delete existing items
		_, err = tx.Exec(ctx, `DELETE FROM data_product_items WHERE product_id = $1`, productID)
		if err != nil {
			return &corev1.ModifyDataProductResponse{
				Message: fmt.Sprintf("Failed to remove old resource items: %v", err),
				Success: false,
				Status:  commonv1.Status_STATUS_ERROR,
			}, nil
		}

		// Add new items
		for i, itemID := range req.ResourceItemIds {
			_, err := tx.Exec(ctx, `
				INSERT INTO data_product_items (product_id, resource_item_id, item_order)
				VALUES ($1, $2, $3)
			`, productID, itemID, i)

			if err != nil {
				return &corev1.ModifyDataProductResponse{
					Message: fmt.Sprintf("Failed to add resource item: %v", err),
					Success: false,
					Status:  commonv1.Status_STATUS_ERROR,
				}, nil
			}
		}
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return &corev1.ModifyDataProductResponse{
			Message: fmt.Sprintf("Failed to commit transaction: %v", err),
			Success: false,
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	// Get updated product
	var product corev1.DataProduct
	var metadataJSON []byte
	var created, updated time.Time

	err = s.engine.db.Pool().QueryRow(ctx, `
		SELECT 
			product_id, tenant_id, workspace_id, product_name, product_description,
			metadata, owner_id, status::text, created, updated
		FROM data_products
		WHERE product_id = $1
	`, productID).Scan(
		&product.ProductId,
		&product.TenantId,
		&product.WorkspaceId,
		&product.ProductName,
		&product.ProductDescription,
		&metadataJSON,
		&product.OwnerId,
		&product.Status,
		&created,
		&updated,
	)

	if err != nil {
		return &corev1.ModifyDataProductResponse{
			Message: fmt.Sprintf("Failed to retrieve updated product: %v", err),
			Success: false,
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	product.Created = created.Format(time.RFC3339)
	product.Updated = updated.Format(time.RFC3339)

	// Parse metadata
	if len(metadataJSON) > 0 {
		var metadataMap map[string]interface{}
		if err := json.Unmarshal(metadataJSON, &metadataMap); err == nil {
			product.Metadata, _ = structpb.NewStruct(metadataMap)
		}
	}

	// Get resource items
	product.ResourceItems, _ = s.getResourceItemsForProduct(ctx, product.ProductId)

	return &corev1.ModifyDataProductResponse{
		DataProduct: &product,
		Message:     "Data product modified successfully",
		Success:     true,
		Status:      commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// DeleteDataProduct deletes a data product
func (s *Server) DeleteDataProduct(ctx context.Context, req *corev1.DeleteDataProductRequest) (*corev1.DeleteDataProductResponse, error) {
	defer s.trackOperation()()

	if req.TenantId == "" || req.WorkspaceName == "" || req.ProductName == "" {
		return &corev1.DeleteDataProductResponse{
			Message: "tenant_id, workspace_name, and product_name are required",
			Success: false,
		}, nil
	}

	// Get workspace_id
	var workspaceID string
	err := s.engine.db.Pool().QueryRow(ctx, `
		SELECT workspace_id 
		FROM workspaces 
		WHERE tenant_id = $1 AND workspace_name = $2
	`, req.TenantId, req.WorkspaceName).Scan(&workspaceID)

	if err != nil {
		return &corev1.DeleteDataProductResponse{
			Message: "Workspace not found",
			Success: false,
		}, nil
	}

	// Delete the data product (cascade will handle items)
	result, err := s.engine.db.Pool().Exec(ctx, `
		DELETE FROM data_products
		WHERE workspace_id = $1 AND product_name = $2
	`, workspaceID, req.ProductName)

	if err != nil {
		return &corev1.DeleteDataProductResponse{
			Message: fmt.Sprintf("Failed to delete data product: %v", err),
			Success: false,
		}, nil
	}

	rowsAffected := result.RowsAffected()
	if rowsAffected == 0 {
		return &corev1.DeleteDataProductResponse{
			Message: "Data product not found",
			Success: false,
		}, nil
	}

	return &corev1.DeleteDataProductResponse{
		Message: "Data product deleted successfully",
		Success: true,
	}, nil
}

// Helper function to get resource items for a product
func (s *Server) getResourceItemsForProduct(ctx context.Context, productID string) ([]*corev1.ResourceItem, error) {
	rows, err := s.engine.db.Pool().Query(ctx, `
		SELECT 
			ri.item_id,
			ri.container_id,
			ri.tenant_id,
			ri.workspace_id,
			ri.resource_uri,
			ri.protocol,
			ri.scope,
			ri.item_type,
			ri.item_name,
			ri.item_path,
			ri.data_type,
			COALESCE(ri.unified_data_type, ''),
			ri.is_nullable,
			ri.is_primary_key,
			ri.is_unique,
			ri.is_indexed,
			ri.is_required,
			ri.is_array,
			ri.array_dimensions,
			COALESCE(ri.default_value, ''),
			COALESCE(ri.max_length, 0),
			COALESCE(ri.precision, 0),
			COALESCE(ri.scale, 0),
			ri.is_privileged,
			COALESCE(ri.privileged_classification, ''),
			COALESCE(ri.detection_confidence, 0),
			COALESCE(ri.detection_method, ''),
			ri.created,
			ri.updated
		FROM resource_items ri
		JOIN data_product_items dpi ON ri.item_id = dpi.resource_item_id
		WHERE dpi.product_id = $1
		ORDER BY dpi.item_order
	`, productID)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []*corev1.ResourceItem
	for rows.Next() {
		var item corev1.ResourceItem
		var created, updated time.Time

		err := rows.Scan(
			&item.ItemId,
			&item.ContainerId,
			&item.TenantId,
			&item.WorkspaceId,
			&item.ResourceUri,
			&item.Protocol,
			&item.Scope,
			&item.ItemType,
			&item.ItemName,
			&item.ItemPath,
			&item.DataType,
			&item.UnifiedDataType,
			&item.IsNullable,
			&item.IsPrimaryKey,
			&item.IsUnique,
			&item.IsIndexed,
			&item.IsRequired,
			&item.IsArray,
			&item.ArrayDimensions,
			&item.DefaultValue,
			&item.MaxLength,
			&item.Precision,
			&item.Scale,
			&item.IsPrivileged,
			&item.PrivilegedClassification,
			&item.DetectionConfidence,
			&item.DetectionMethod,
			&created,
			&updated,
		)
		if err != nil {
			continue
		}

		item.Created = created.Format(time.RFC3339)
		item.Updated = updated.Format(time.RFC3339)

		items = append(items, &item)
	}

	return items, nil
}

// Helper function to join strings
func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	result := strs[0]
	for i := 1; i < len(strs); i++ {
		result += sep + strs[i]
	}
	return result
}

