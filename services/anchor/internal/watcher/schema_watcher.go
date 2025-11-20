package watcher

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"encoding/json"

	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	pb "github.com/redbco/redb-open/api/proto/unifiedmodel/v1"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/anchor/internal/resources"
	"github.com/redbco/redb-open/services/anchor/internal/state"
	"google.golang.org/grpc"
)

type SchemaWatcher struct {
	state        *state.GlobalState
	db           *database.PostgreSQL
	umClient     pb.UnifiedModelServiceClient
	repoClient   corev1.RepoServiceClient
	branchClient corev1.BranchServiceClient
	commitClient corev1.CommitServiceClient
	resourceRepo *resources.Repository
	logger       *logger.Logger
}

func NewSchemaWatcher(db *database.PostgreSQL, umConn *grpc.ClientConn, coreConn *grpc.ClientConn, supervisorAddr string, logger *logger.Logger) *SchemaWatcher {
	return &SchemaWatcher{
		state:        state.GetInstance(),
		db:           db,
		umClient:     pb.NewUnifiedModelServiceClient(umConn),
		repoClient:   corev1.NewRepoServiceClient(coreConn),
		branchClient: corev1.NewBranchServiceClient(coreConn),
		commitClient: corev1.NewCommitServiceClient(coreConn),
		resourceRepo: resources.NewRepository(db.Pool()),
		logger:       logger,
	}
}

// ensureRepoBranchCommit ensures that a repository, branch, and commit exist for storing schema changes.
// It returns the branch ID and commit ID if successful.
func (w *SchemaWatcher) ensureRepoBranchCommit(ctx context.Context, workspaceID, databaseID, schemaType string, schemaStructure []byte, commitMessage string) (string, string, error) {
	// Get database name from the database client
	registry := w.state.GetConnectionRegistry()
	client, err := registry.GetDatabaseClient(databaseID)
	if err != nil {
		w.logError("Failed to get database client %s: %v", databaseID, err)
		return "", "", fmt.Errorf("failed to get database client: %w", err)
	}

	// Use the database name from the config, fallback to database ID if not available
	databaseName := client.Config.Name
	if databaseName == "" {
		databaseName = databaseID
		w.logWarn("Database name not found in config for %s, using database ID", databaseID)
	}

	// Create a human-readable repo name based on the database name
	// Sanitize the database name to make it suitable for a repo name
	repoName := w.generateUniqueRepoName(ctx, workspaceID, databaseName)
	repoDesc := fmt.Sprintf("Schema repository for database %s", databaseName)

	// Try to find existing repo
	repo, err := w.branchClient.FindRepoAndBranchByDatabaseID(ctx, &corev1.FindRepoAndBranchByDatabaseIDRequest{
		DatabaseId: databaseID,
	})
	if err != nil {
		w.logError("Failed to list repos: %v", err)
		return "", "", fmt.Errorf("failed to list repos: %w", err)
	}

	var repoID, branchID string
	repoID = repo.RepoId
	branchID = repo.BranchId

	// Create repo if it doesn't exist
	if repoID == "" {
		repoResp, err := w.repoClient.CreateRepoByAnchor(ctx, &corev1.CreateRepoByAnchorRequest{
			DatabaseId:      databaseID,
			RepoName:        repoName,
			RepoDescription: repoDesc,
		})
		if err != nil {
			w.logError("Failed to create repo: %v", err)
			return "", "", fmt.Errorf("failed to create repo: %w", err)
		}
		repoID = repoResp.RepoId
		branchID = repoResp.BranchId
		w.logInfo("Created repo: %s with name: %s", repoID, repoName)
	}

	// Create a new commit with the schema structure
	commitResp, err := w.commitClient.CreateCommitByAnchor(ctx, &corev1.CreateCommitByAnchorRequest{
		BranchId:        branchID,
		CommitMessage:   commitMessage,
		IsHead:          true,
		SchemaType:      schemaType,
		SchemaStructure: string(schemaStructure),
	})
	if err != nil {
		w.logError("Failed to create commit: %v", err)
		return "", "", fmt.Errorf("failed to create commit: %w", err)
	}

	// Also store the new schema in the database
	_, err = w.db.Pool().Exec(ctx, "UPDATE databases SET database_schema = $1 WHERE database_id = $2", string(schemaStructure), databaseID)
	if err != nil {
		w.logError("Failed to store schema in database: %v", err)
		return "", "", fmt.Errorf("failed to store schema in database: %w", err)
	}

	// Request enriched analysis from the unified model service
	w.logInfo("Requesting enriched analysis for database %s", databaseID)

	// Convert JSON bytes back to UnifiedModel for the enriched analysis
	var um unifiedmodel.UnifiedModel
	err = json.Unmarshal(schemaStructure, &um)
	if err != nil {
		w.logError("Failed to unmarshal schema for enriched analysis: %v", err)
	} else {
		enrichedResp, err := w.umClient.AnalyzeSchemaEnriched(ctx, &pb.AnalyzeSchemaEnrichedRequest{
			SchemaType:   schemaType,
			UnifiedModel: um.ToProto(),
		})
		if err != nil {
			w.logError("Failed to get enriched analysis: %v", err)
			// Don't fail the entire operation if enriched analysis fails
			// Just log the error and continue
		} else {
			// Marshal the enriched analysis results to JSON
			enrichedBytes, err := json.Marshal(enrichedResp)
			if err != nil {
				w.logError("Failed to marshal enriched analysis: %v", err)
			} else {
				// Store the enriched analysis in the database_tables column
				_, err = w.db.Pool().Exec(ctx, "UPDATE databases SET database_tables = $1 WHERE database_id = $2", string(enrichedBytes), databaseID)
				if err != nil {
					w.logError("Failed to store enriched analysis in database: %v", err)
				} else {
					w.logInfo("Successfully stored enriched analysis for database %s", databaseID)
				}
			}

			// Log any warnings from the enriched analysis
			for _, warning := range enrichedResp.Warnings {
				w.logWarn("Enriched analysis warning for %s: %s", databaseID, warning)
			}

			// Populate resource registry tables with containers and items (pass enrichedResp)
			w.logInfo("Populating resource registry for database %s", databaseID)
			err = w.populateResourceRegistry(ctx, &um, databaseID, enrichedResp)
			if err != nil {
				w.logError("Failed to populate resource registry for database %s: %v", databaseID, err)
				// Don't fail the entire operation if resource registry population fails
			} else {
				w.logInfo("Successfully populated resource registry for database %s", databaseID)
			}
		}
	}

	return branchID, commitResp.CommitId, nil
}

// generateUniqueRepoName generates a unique repository name based on the database name
// It ensures uniqueness by appending a suffix if the name already exists
func (w *SchemaWatcher) generateUniqueRepoName(ctx context.Context, workspaceID, databaseName string) string {
	// Sanitize the database name to make it suitable for a repo name
	// Remove or replace characters that might cause issues in repo names
	baseName := w.sanitizeRepoName(databaseName)

	// Start with the base name
	repoName := baseName

	// Check if this name already exists in the workspace
	// We'll try up to 100 times to find a unique name
	for i := 0; i < 100; i++ {
		exists, err := w.checkRepoNameExists(ctx, workspaceID, repoName)
		if err != nil {
			w.logError("Failed to check repo name existence: %v", err)
			// If we can't check, use a timestamp-based fallback
			return fmt.Sprintf("%s-%d", baseName, time.Now().Unix())
		}

		if !exists {
			return repoName
		}

		// Name exists, try with a suffix
		if i == 0 {
			repoName = fmt.Sprintf("%s-schema", baseName)
		} else {
			repoName = fmt.Sprintf("%s-schema-%d", baseName, i)
		}
	}

	// If we've tried 100 times and still can't find a unique name, use timestamp
	return fmt.Sprintf("%s-schema-%d", baseName, time.Now().Unix())
}

// sanitizeRepoName sanitizes a database name to make it suitable for a repository name
func (w *SchemaWatcher) sanitizeRepoName(name string) string {
	// Convert to lowercase
	name = strings.ToLower(name)

	// Replace spaces and common separators with hyphens
	replacer := strings.NewReplacer(
		" ", "-",
		"_", "-",
		".", "-",
		"/", "-",
		"\\", "-",
		":", "-",
		";", "-",
		",", "-",
		"(", "",
		")", "",
		"[", "",
		"]", "",
		"{", "",
		"}", "",
		"<", "",
		">", "",
		"|", "-",
		"*", "",
		"?", "",
		"!", "",
		"@", "",
		"#", "",
		"$", "",
		"%", "",
		"^", "",
		"&", "-",
		"+", "-",
		"=", "-",
		"~", "-",
		"`", "",
		"'", "",
		"\"", "",
	)
	name = replacer.Replace(name)

	// Remove multiple consecutive hyphens
	for strings.Contains(name, "--") {
		name = strings.ReplaceAll(name, "--", "-")
	}

	// Remove leading and trailing hyphens
	name = strings.Trim(name, "-")

	// Ensure the name is not empty
	if name == "" {
		name = "database"
	}

	// Limit length to 50 characters to leave room for suffixes
	if len(name) > 50 {
		name = name[:50]
		// Remove trailing hyphens if we cut in the middle of a word
		name = strings.Trim(name, "-")
	}

	return name
}

// checkRepoNameExists checks if a repository name already exists in the workspace
func (w *SchemaWatcher) checkRepoNameExists(ctx context.Context, workspaceID, repoName string) (bool, error) {
	// Query the database to check if a repo with this name exists in the workspace
	var exists bool
	err := w.db.Pool().QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM repos WHERE workspace_id = $1 AND repo_name = $2)",
		workspaceID, repoName).Scan(&exists)

	if err != nil {
		return false, fmt.Errorf("failed to check repo name existence: %w", err)
	}

	return exists, nil
}

func (w *SchemaWatcher) Start(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	if w.logger != nil {
		w.logger.Info("Schema watcher starting...")
		defer w.logger.Info("Schema watcher shutdown complete")
	}

	for {
		select {
		case <-ctx.Done():
			if w.logger != nil {
				w.logger.Info("Schema watcher received shutdown signal")
			}
			return
		case <-ticker.C:
			// Check if context is cancelled before starting work
			if ctx.Err() != nil {
				if w.logger != nil {
					w.logger.Info("Schema watcher shutting down, skipping work")
				}
				return
			}

			if w.logger != nil {
				w.logger.Info("Schema watcher checking for changes")
			}
			// Use a separate context for the checkSchemaChanges call to prevent it from affecting the main loop
			checkCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			err := w.checkSchemaChanges(checkCtx)
			cancel() // Cancel the context to free resources

			if err != nil {
				// Don't log context cancellation errors as they're expected during shutdown
				if ctx.Err() == nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					if w.logger != nil {
						w.logger.Errorf("Failed to check schema changes: %v", err)
					}
				}
				// Continue the loop even if there's an error, unless context is cancelled
			}
		}
	}
}

// Helper method to safely log with nil check
func (w *SchemaWatcher) logDebug(msg string, args ...interface{}) {
	if w.logger != nil {
		if len(args) > 0 {
			w.logger.Debugf(msg, args...)
		} else {
			w.logger.Debug("%s", msg)
		}
	}
}

func (w *SchemaWatcher) logInfo(msg string, args ...interface{}) {
	if w.logger != nil {
		if len(args) > 0 {
			w.logger.Infof(msg, args...)
		} else {
			w.logger.Info("%s", msg)
		}
	}
}

func (w *SchemaWatcher) logError(msg string, args ...interface{}) {
	if w.logger != nil {
		if len(args) > 0 {
			w.logger.Errorf(msg, args...)
		} else {
			w.logger.Error("%s", msg)
		}
	}
}

func (w *SchemaWatcher) logWarn(msg string, args ...interface{}) {
	if w.logger != nil {
		if len(args) > 0 {
			w.logger.Warnf(msg, args...)
		} else {
			w.logger.Warn("%s", msg)
		}
	}
}

func (w *SchemaWatcher) checkSchemaChanges(ctx context.Context) error {
	// Check if context is cancelled before starting
	if ctx.Err() != nil {
		return ctx.Err()
	}

	registry := w.state.GetConnectionRegistry()

	// Get all connected database clients
	for _, clientID := range registry.GetAllDatabaseClientIDs() {
		// Check if context is cancelled before processing each database
		if ctx.Err() != nil {
			return ctx.Err()
		}

		w.logInfo("Checking schema for database: %s", clientID)

		client, err := registry.GetDatabaseClient(clientID)
		if err != nil {
			w.logError("Failed to get database client %s: %v", clientID, err)
			continue
		}

		// Get current schema structure as UnifiedModel via adapter
		conn := client.AdapterConnection.(adapter.Connection)
		currentUM, err := conn.SchemaOperations().DiscoverSchema(ctx)
		if err != nil {
			w.logError("Failed to get schema for database %s: %v", clientID, err)
			continue
		}

		// Marshal the current schema to JSON for storage (still needed for database storage)
		currentBytes, err := json.Marshal(currentUM)
		if err != nil {
			w.logError("Failed to marshal current schema: %v", err)
			continue
		}

		// If we have a previous schema to compare against
		if client.LastSchema != nil {
			// Ensure previous schema is also a UnifiedModel
			previousUM, ok := client.LastSchema.(*unifiedmodel.UnifiedModel)
			if !ok {
				w.logError("Previous schema is not a UnifiedModel for database %s", clientID)
				continue
			}

			// Call UnifiedModel service to compare schemas using UnifiedModel objects
			w.logDebug("Comparing schemas for database %s", clientID)
			compareResp, err := w.umClient.CompareUnifiedModels(ctx, &pb.CompareUnifiedModelsRequest{
				PreviousUnifiedModel: previousUM.ToProto(),
				CurrentUnifiedModel:  currentUM.ToProto(),
			})
			if err != nil {
				w.logError("Failed to compare schemas: %v", err)
				continue
			}

			if compareResp.HasChanges {
				w.logInfo("Schema changes detected for database %s", clientID)
				var commitMessage string
				for _, change := range compareResp.Changes {
					w.logInfo("Schema change: %s", change)
					commitMessage += change + "\n"
				}

				// Store the schema changes in the internal database
				_, _, err := w.ensureRepoBranchCommit(ctx, client.Config.WorkspaceID, client.Config.DatabaseID, client.Config.ConnectionType, currentBytes, commitMessage)
				if err != nil {
					w.logError("Failed to store schema changes: %v", err)
					continue
				}

				// Invalidate mappings that target tables in this database
				w.invalidateMappingsForDatabase(ctx, client.Config.WorkspaceID, client.Config.DatabaseID)
			} else {
				w.logDebug("No schema changes detected for database %s", clientID)
			}

			// Log any warnings
			for _, warning := range compareResp.Warnings {
				w.logWarn("Schema comparison warning for %s: %s", clientID, warning)
			}
		} else {
			w.logDebug("No previous schema found for database %s, checking for existing commits", clientID)

			// Try to get the latest commit for this database
			w.logDebug("Fetching latest commit for database %s", clientID)

			// Create a timeout context for the GetLatestCommitForDatabase call
			timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			latestCommit, err := w.state.GetConfigRepository().GetLatestStoredDatabaseSchema(timeoutCtx, client.Config.DatabaseID)
			cancel() // Cancel the timeout context

			if err != nil {
				// Log the error but continue processing
				w.logError("Error fetching latest commit for database %s: %v", clientID, err)
				w.logInfo("Proceeding with initial schema storage for database %s", clientID)

				// Store the initial schema in the internal database
				_, _, err := w.ensureRepoBranchCommit(ctx, client.Config.WorkspaceID, client.Config.DatabaseID, client.Config.ConnectionType, currentBytes, "Discovered schema")
				if err != nil {
					w.logError("Failed to store initial schema: %v", err)
					continue
				}
			} else {
				w.logInfo("Found existing commit for database %s, comparing with current schema", clientID)

				// The stored schema might be JSON, so we need to handle conversion
				var previousUM *unifiedmodel.UnifiedModel

				// Try to convert stored schema to UnifiedModel
				if storedUM, ok := latestCommit.Schema.(*unifiedmodel.UnifiedModel); ok {
					// Already a UnifiedModel
					previousUM = storedUM
				} else {
					// Stored as JSON, need to unmarshal
					var jsonBytes []byte
					if str, ok := latestCommit.Schema.(string); ok {
						jsonBytes = []byte(str)
					} else if bytes, ok := latestCommit.Schema.([]byte); ok {
						jsonBytes = bytes
					} else {
						// Try to marshal it first (in case it's a map or other structure)
						jsonBytes, err = json.Marshal(latestCommit.Schema)
						if err != nil {
							w.logError("Failed to marshal stored schema: %v", err)
							continue
						}
					}

					previousUM = &unifiedmodel.UnifiedModel{}
					err = json.Unmarshal(jsonBytes, previousUM)
					if err != nil {
						w.logError("Failed to unmarshal stored schema: %v", err)
						continue
					}
				}

				// Call UnifiedModel service to compare schemas using UnifiedModel objects
				compareResp, err := w.umClient.CompareUnifiedModels(ctx, &pb.CompareUnifiedModelsRequest{
					PreviousUnifiedModel: previousUM.ToProto(),
					CurrentUnifiedModel:  currentUM.ToProto(),
				})
				if err != nil {
					w.logError("Failed to compare schemas: %v", err)
					continue
				}

				if compareResp.HasChanges {
					w.logInfo("Schema changes detected for database %s", clientID)
					var commitMessage string
					for _, change := range compareResp.Changes {
						w.logInfo("Schema change: %s", change)
						commitMessage += change + "\n"
					}

					// Store the schema changes in the internal database
					_, _, err := w.ensureRepoBranchCommit(ctx, client.Config.WorkspaceID, client.Config.DatabaseID, client.Config.ConnectionType, currentBytes, commitMessage)
					if err != nil {
						w.logError("Failed to store schema changes: %v", err)
						continue
					}

					// Invalidate mappings that target tables in this database
					w.invalidateMappingsForDatabase(ctx, client.Config.WorkspaceID, client.Config.DatabaseID)
				} else {
					w.logDebug("No schema changes detected for database %s", clientID)
				}

				// Log any warnings
				for _, warning := range compareResp.Warnings {
					w.logWarn("Schema comparison warning for %s: %s", clientID, warning)
				}
			}
		}

		// Update last known schema
		client.LastSchema = currentUM
	}

	return nil
}

// invalidateMappingsForDatabase invalidates all mappings that target any table in the specified database
func (w *SchemaWatcher) invalidateMappingsForDatabase(ctx context.Context, workspaceID, databaseID string) {
	w.logInfo("Invalidating mappings that target database %s", databaseID)

	// Query to invalidate all mappings that have rules targeting this database
	query := `
		UPDATE mappings m
		SET validated = false,
		    validated_at = NULL,
		    validation_errors = '[]',
		    validation_warnings = '[]',
		    updated = CURRENT_TIMESTAMP
		WHERE m.workspace_id = $1
		AND m.mapping_id IN (
			SELECT DISTINCT mrm.mapping_id
			FROM mapping_rule_mappings mrm
			JOIN mapping_rules mr ON mrm.mapping_rule_id = mr.mapping_rule_id
			WHERE mr.mapping_rule_metadata->>'target_resource_uri' LIKE $2
		)
	`

	// The target URI format is: redb://database_id/dbname/table/table_name/column/column_name
	// We want to match any target in this database
	targetPattern := fmt.Sprintf("redb://%s/%%", databaseID)

	result, err := w.db.Pool().Exec(ctx, query, workspaceID, targetPattern)
	if err != nil {
		w.logError("Failed to invalidate mappings for database %s: %v", databaseID, err)
		return
	}

	rowsAffected := result.RowsAffected()
	if rowsAffected > 0 {
		w.logInfo("Invalidated %d mapping(s) targeting database %s", rowsAffected, databaseID)
	} else {
		w.logDebug("No mappings found targeting database %s", databaseID)
	}
}

// populateResourceRegistry populates the resource_containers and resource_items tables
func (w *SchemaWatcher) populateResourceRegistry(ctx context.Context, um *unifiedmodel.UnifiedModel, databaseID string, enrichedResp *pb.AnalyzeSchemaEnrichedResponse) error {
	// Get database info from the database
	var tenantID, workspaceID, ownerID, nodeID, databaseName string
	err := w.db.Pool().QueryRow(ctx,
		`SELECT tenant_id, workspace_id, owner_id, connected_to_node_id, database_name 
		 FROM databases WHERE database_id = $1`,
		databaseID).Scan(&tenantID, &workspaceID, &ownerID, &nodeID, &databaseName)
	if err != nil {
		return fmt.Errorf("failed to get database info: %w", err)
	}

	// Generate containers and items from the UnifiedModel (now passing enrichedResp)
	containers, items, err := unifiedmodel.PopulateResourcesFromUnifiedModel(um, databaseID, nodeID, tenantID, workspaceID, ownerID, databaseName, enrichedResp)
	if err != nil {
		return fmt.Errorf("failed to generate resources from UnifiedModel: %w", err)
	}

	w.logInfo("Generated %d containers and %d items for database %s", len(containers), len(items), databaseID)

	// Instead of deleting existing containers (which would cascade delete mappings),
	// we'll upsert them and delete only containers that no longer exist in the new schema

	// Get existing container IDs for this database
	existingContainerURIs := make(map[string]string) // URI -> ID
	existingRows, err := w.db.Pool().Query(ctx,
		"SELECT container_id, resource_uri FROM resource_containers WHERE database_id = $1",
		databaseID)
	if err != nil {
		return fmt.Errorf("failed to query existing containers: %w", err)
	}
	for existingRows.Next() {
		var containerID, resourceURI string
		if err := existingRows.Scan(&containerID, &resourceURI); err != nil {
			w.logError("Failed to scan existing container: %v", err)
			continue
		}
		existingContainerURIs[resourceURI] = containerID
	}
	existingRows.Close()

	// Track new container URIs from the discovery
	newContainerURIs := make(map[string]bool)
	for _, container := range containers {
		newContainerURIs[container.ResourceURI] = true
	}

	// Delete containers that no longer exist in the schema
	for uri, containerID := range existingContainerURIs {
		if !newContainerURIs[uri] {
			_, err := w.db.Pool().Exec(ctx,
				"DELETE FROM resource_containers WHERE container_id = $1",
				containerID)
			if err != nil {
				w.logError("Failed to delete obsolete container %s: %v", uri, err)
			} else {
				w.logInfo("Deleted obsolete container: %s", uri)
			}
		}
	}

	// Get existing item IDs for this database
	// Note: resource_items doesn't have database_id, so we need to join through containers
	existingItemURIs := make(map[string]string) // URI -> ID
	existingItemRows, err := w.db.Pool().Query(ctx,
		`SELECT ri.item_id, ri.resource_uri 
		 FROM resource_items ri
		 JOIN resource_containers rc ON ri.container_id = rc.container_id
		 WHERE rc.database_id = $1`,
		databaseID)
	if err != nil {
		return fmt.Errorf("failed to query existing items: %w", err)
	}
	for existingItemRows.Next() {
		var itemID, resourceURI string
		if err := existingItemRows.Scan(&itemID, &resourceURI); err != nil {
			w.logError("Failed to scan existing item: %v", err)
			continue
		}
		existingItemURIs[resourceURI] = itemID
	}
	existingItemRows.Close()

	// Track new item URIs from the discovery
	newItemURIs := make(map[string]bool)
	for _, item := range items {
		newItemURIs[item.ResourceURI] = true
	}

	// Delete items that no longer exist in the schema
	for uri, itemID := range existingItemURIs {
		if !newItemURIs[uri] {
			_, err := w.db.Pool().Exec(ctx,
				"DELETE FROM resource_items WHERE item_id = $1",
				itemID)
			if err != nil {
				w.logError("Failed to delete obsolete item %s: %v", uri, err)
			} else {
				w.logInfo("Deleted obsolete item: %s", uri)
			}
		}
	}

	// Create a map to associate container URIs with their IDs after insertion/update
	containerURIToID := make(map[string]string)

	// Upsert containers (update existing, insert new)
	containersCreated := 0
	for _, container := range containers {
		if existingID, exists := existingContainerURIs[container.ResourceURI]; exists {
			// Update existing container
			updates := map[string]interface{}{
				"object_type":        container.ObjectType,
				"object_name":        container.ObjectName,
				"container_metadata": container.ContainerMetadata,
				"enriched_metadata":  container.EnrichedMetadata,
				"item_count":         container.ItemCount,
				"size_bytes":         container.SizeBytes,
			}
			err = w.resourceRepo.UpdateContainer(ctx, existingID, updates)
			if err != nil {
				w.logError("Failed to update container %s: %v", container.ResourceURI, err)
				continue
			}
			containerURIToID[container.ResourceURI] = existingID
		} else {
			// Create new container
			err = w.resourceRepo.CreateContainer(ctx, container)
			if err != nil {
				w.logError("Failed to create container %s: %v", container.ResourceURI, err)
				continue
			}
			containerURIToID[container.ResourceURI] = container.ContainerID
			containersCreated++
		}
	}

	// Upsert items (update existing, insert new)
	itemsCreated := 0
	for _, item := range items {
		// Extract the container URI from the item URI
		containerURI := extractContainerURIFromItemURI(item.ResourceURI)

		// Set the container_id based on the mapping
		if containerID, ok := containerURIToID[containerURI]; ok {
			item.ContainerID = containerID
		} else {
			w.logError("Failed to find container ID for item %s (container URI: %s)", item.ResourceURI, containerURI)
			continue
		}

		// Check if item already exists
		if existingItemID, exists := existingItemURIs[item.ResourceURI]; exists {
			// Update existing item
			updates := map[string]interface{}{
				"item_name":                 item.ItemName,
				"item_display_name":         item.ItemDisplayName,
				"data_type":                 item.DataType,
				"unified_data_type":         item.UnifiedDataType,
				"is_nullable":               item.IsNullable,
				"is_primary_key":            item.IsPrimaryKey,
				"is_unique":                 item.IsUnique,
				"is_indexed":                item.IsIndexed,
				"is_required":               item.IsRequired,
				"is_array":                  item.IsArray,
				"default_value":             item.DefaultValue,
				"constraints":               item.Constraints,
				"is_privileged":             item.IsPrivileged,
				"privileged_classification": item.PrivilegedClassification,
				"detection_confidence":      item.DetectionConfidence,
				"detection_method":          item.DetectionMethod,
				"ordinal_position":          item.OrdinalPosition,
				"max_length":                item.MaxLength,
				"precision":                 item.Precision,
				"scale":                     item.Scale,
				"item_comment":              item.ItemComment,
				"array_dimensions":          item.ArrayDimensions,
			}
			err = w.resourceRepo.UpdateItem(ctx, existingItemID, updates)
			if err != nil {
				w.logError("Failed to update item %s: %v", item.ResourceURI, err)
				continue
			}
		} else {
			// Create new item
			err = w.resourceRepo.CreateItem(ctx, item)
			if err != nil {
				w.logError("Failed to create item %s: %v", item.ResourceURI, err)
				continue
			}
			itemsCreated++
		}
	}

	w.logInfo("Successfully populated %d containers and %d items for database %s", containersCreated, itemsCreated, databaseID)
	return nil
}

// RefreshResourceRegistry triggers an immediate refresh of the resource registry for a specific database
// Returns the number of containers and items created
func (w *SchemaWatcher) RefreshResourceRegistry(ctx context.Context, databaseID string) (int, int, error) {
	w.logInfo("Manually refreshing resource registry for database: %s", databaseID)

	// Get database structure via adapter
	registry := w.state.GetConnectionRegistry()
	client, err := registry.GetDatabaseClient(databaseID)
	if err != nil {
		return 0, 0, fmt.Errorf("database not found: %w", err)
	}

	conn := client.AdapterConnection.(adapter.Connection)
	um, err := conn.SchemaOperations().DiscoverSchema(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to discover database schema: %w", err)
	}

	// Marshal the discovered schema to JSON for storage
	schemaBytes, err := json.Marshal(um)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to marshal schema: %w", err)
	}

	// Update the database record with the fresh schema
	_, err = w.db.Pool().Exec(ctx, "UPDATE databases SET database_schema = $1 WHERE database_id = $2", string(schemaBytes), databaseID)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to update database schema: %w", err)
	}

	w.logInfo("Updated database schema for database %s", databaseID)

	// Get database info from the database
	var tenantID, workspaceID, ownerID, nodeID, databaseName string
	err = w.db.Pool().QueryRow(ctx,
		`SELECT tenant_id, workspace_id, owner_id, connected_to_node_id, database_name 
		 FROM databases WHERE database_id = $1`,
		databaseID).Scan(&tenantID, &workspaceID, &ownerID, &nodeID, &databaseName)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get database info: %w", err)
	}

	// Call unified model service for enrichment
	var enrichResp *pb.AnalyzeSchemaEnrichedResponse
	if w.umClient != nil {
		umProto := um.ToProto()
		enrichReq := &pb.AnalyzeSchemaEnrichedRequest{
			SchemaType:   string(um.DatabaseType),
			UnifiedModel: umProto,
		}

		enrichResp, err = w.umClient.AnalyzeSchemaEnriched(ctx, enrichReq)
		if err != nil {
			w.logWarn("Failed to enrich schema for database %s: %v (continuing without enrichment)", databaseID, err)
			enrichResp = nil
		} else {
			// Update the database record with the fresh enrichment data
			enrichedBytes, err := json.Marshal(enrichResp)
			if err != nil {
				w.logError("Failed to marshal enriched analysis: %v", err)
			} else {
				_, err = w.db.Pool().Exec(ctx, "UPDATE databases SET database_tables = $1 WHERE database_id = $2", string(enrichedBytes), databaseID)
				if err != nil {
					w.logError("Failed to store enriched analysis in database: %v", err)
				} else {
					w.logInfo("Updated enriched analysis for database %s", databaseID)
				}
			}
		}
	}

	// Generate containers and items from the UnifiedModel
	containers, items, err := unifiedmodel.PopulateResourcesFromUnifiedModel(um, databaseID, nodeID, tenantID, workspaceID, ownerID, databaseName, enrichResp)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to generate resources from UnifiedModel: %w", err)
	}

	w.logInfo("Generated %d containers and %d items for database %s", len(containers), len(items), databaseID)

	// Instead of deleting existing containers (which would cascade delete mappings),
	// we'll upsert them and delete only containers that no longer exist in the new schema

	// Get existing container IDs for this database
	existingContainerURIs := make(map[string]string) // URI -> ID
	existingRows, err := w.db.Pool().Query(ctx,
		"SELECT container_id, resource_uri FROM resource_containers WHERE database_id = $1",
		databaseID)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to query existing containers: %w", err)
	}
	for existingRows.Next() {
		var containerID, resourceURI string
		if err := existingRows.Scan(&containerID, &resourceURI); err != nil {
			w.logError("Failed to scan existing container: %v", err)
			continue
		}
		existingContainerURIs[resourceURI] = containerID
	}
	existingRows.Close()

	// Track new container URIs from the discovery
	newContainerURIs := make(map[string]bool)
	for _, container := range containers {
		newContainerURIs[container.ResourceURI] = true
	}

	// Delete containers that no longer exist in the schema
	for uri, containerID := range existingContainerURIs {
		if !newContainerURIs[uri] {
			_, err := w.db.Pool().Exec(ctx,
				"DELETE FROM resource_containers WHERE container_id = $1",
				containerID)
			if err != nil {
				w.logError("Failed to delete obsolete container %s: %v", uri, err)
			} else {
				w.logInfo("Deleted obsolete container: %s", uri)
			}
		}
	}

	// Get existing item IDs for this database
	// Note: resource_items doesn't have database_id, so we need to join through containers
	existingItemURIs := make(map[string]string) // URI -> ID
	existingItemRows, err := w.db.Pool().Query(ctx,
		`SELECT ri.item_id, ri.resource_uri 
		 FROM resource_items ri
		 JOIN resource_containers rc ON ri.container_id = rc.container_id
		 WHERE rc.database_id = $1`,
		databaseID)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to query existing items: %w", err)
	}
	for existingItemRows.Next() {
		var itemID, resourceURI string
		if err := existingItemRows.Scan(&itemID, &resourceURI); err != nil {
			w.logError("Failed to scan existing item: %v", err)
			continue
		}
		existingItemURIs[resourceURI] = itemID
	}
	existingItemRows.Close()

	// Track new item URIs from the discovery
	newItemURIs := make(map[string]bool)
	for _, item := range items {
		newItemURIs[item.ResourceURI] = true
	}

	// Delete items that no longer exist in the schema
	for uri, itemID := range existingItemURIs {
		if !newItemURIs[uri] {
			_, err := w.db.Pool().Exec(ctx,
				"DELETE FROM resource_items WHERE item_id = $1",
				itemID)
			if err != nil {
				w.logError("Failed to delete obsolete item %s: %v", uri, err)
			}
		}
	}

	// Create a map to associate container URIs with their IDs after insertion/update
	containerURIToID := make(map[string]string)

	// Upsert containers
	containersCreated := 0
	for _, container := range containers {
		// Check if container already exists
		if existingID, exists := existingContainerURIs[container.ResourceURI]; exists {
			// Update existing container
			updates := map[string]interface{}{
				"object_type":                         container.ObjectType,
				"object_name":                         container.ObjectName,
				"container_metadata":                  container.ContainerMetadata,
				"enriched_metadata":                   container.EnrichedMetadata,
				"database_type":                       container.DatabaseType,
				"vendor":                              container.Vendor,
				"item_count":                          container.ItemCount,
				"size_bytes":                          container.SizeBytes,
				"container_classification":            container.ContainerClassification,
				"container_classification_confidence": container.ContainerClassificationConfidence,
				"container_classification_source":     container.ContainerClassificationSource,
				"status":                              container.Status,
				"status_message":                      container.StatusMessage,
				"last_seen":                           container.LastSeen,
				"online":                              container.Online,
			}
			err = w.resourceRepo.UpdateContainer(ctx, existingID, updates)
			if err != nil {
				w.logError("Failed to update container %s: %v", container.ResourceURI, err)
				continue
			}
			containerURIToID[container.ResourceURI] = existingID
		} else {
			// Create new container
			err = w.resourceRepo.CreateContainer(ctx, container)
			if err != nil {
				w.logError("Failed to create container %s: %v", container.ResourceURI, err)
				continue
			}
			containerURIToID[container.ResourceURI] = container.ContainerID
			containersCreated++
		}
	}

	// Insert items - now with container_id set
	itemsCreated := 0
	for _, item := range items {
		// Extract the container URI from the item URI
		containerURI := extractContainerURIFromItemURI(item.ResourceURI)

		// Set the container_id based on the mapping
		if containerID, ok := containerURIToID[containerURI]; ok {
			item.ContainerID = containerID
		} else {
			w.logError("Failed to find container ID for item %s (container URI: %s)", item.ResourceURI, containerURI)
			continue
		}

		// Check if item already exists
		if existingItemID, exists := existingItemURIs[item.ResourceURI]; exists {
			// Update existing item
			updates := map[string]interface{}{
				"item_name":                 item.ItemName,
				"item_display_name":         item.ItemDisplayName,
				"data_type":                 item.DataType,
				"unified_data_type":         item.UnifiedDataType,
				"is_nullable":               item.IsNullable,
				"is_primary_key":            item.IsPrimaryKey,
				"is_unique":                 item.IsUnique,
				"is_indexed":                item.IsIndexed,
				"is_required":               item.IsRequired,
				"is_array":                  item.IsArray,
				"default_value":             item.DefaultValue,
				"constraints":               item.Constraints,
				"is_privileged":             item.IsPrivileged,
				"privileged_classification": item.PrivilegedClassification,
				"detection_confidence":      item.DetectionConfidence,
				"detection_method":          item.DetectionMethod,
				"ordinal_position":          item.OrdinalPosition,
				"max_length":                item.MaxLength,
				"precision":                 item.Precision,
				"scale":                     item.Scale,
				"item_comment":              item.ItemComment,
				"container_id":              item.ContainerID,
			}
			err = w.resourceRepo.UpdateItem(ctx, existingItemID, updates)
			if err != nil {
				w.logError("Failed to update item %s: %v", item.ResourceURI, err)
				continue
			}
		} else {
			// Create new item
			err = w.resourceRepo.CreateItem(ctx, item)
			if err != nil {
				w.logError("Failed to create item %s: %v", item.ResourceURI, err)
				continue
			}
			itemsCreated++
		}
	}

	w.logInfo("Successfully populated %d containers and %d items for database %s", containersCreated, itemsCreated, databaseID)
	return containersCreated, itemsCreated, nil
}

// extractContainerURIFromItemURI extracts the container URI from an item URI
// Item URI format examples:
//   - redb://data/database/{dbID}/table/{tableName}/column/{columnName}
//   - redb://data/database/{dbID}/collection/{collectionName}/field/{fieldName}
//   - redb://data/database/{dbID}/view/{viewName}/column/{columnName}
//
// Container URI format:
//   - redb://data/database/{dbID}/table/{tableName}
//   - redb://data/database/{dbID}/collection/{collectionName}
//   - redb://data/database/{dbID}/view/{viewName}
func extractContainerURIFromItemURI(itemURI string) string {
	// Find the last occurrence of a container type (table, collection, view, etc.)
	// and extract everything up to and including its name

	// Split by '/' to get segments
	parts := strings.Split(itemURI, "/")

	// We need to find patterns like: .../table/{name}/column/... or .../collection/{name}/field/...
	// The container URI is everything up to and including the container name
	for i := 0; i < len(parts)-2; i++ {
		segment := parts[i]
		// Check if this is a container type
		if segment == "table" || segment == "collection" || segment == "view" ||
			segment == "materialized_view" || segment == "graph_node" ||
			segment == "graph_edge" || segment == "topic" || segment == "stream" {
			// Container URI is everything up to and including the next segment (container name)
			if i+1 < len(parts) {
				return strings.Join(parts[:i+2], "/")
			}
		}
	}

	// Fallback: return the URI as-is (shouldn't happen with valid URIs)
	return itemURI
}
