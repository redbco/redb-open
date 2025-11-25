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
	"github.com/redbco/redb-open/pkg/models"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/anchor/internal/resources"
	"github.com/redbco/redb-open/services/anchor/internal/state"
	"google.golang.org/grpc"
)

type SchemaWatcher struct {
	state         *state.GlobalState
	db            *database.PostgreSQL
	umClient      pb.UnifiedModelServiceClient
	repoClient    corev1.RepoServiceClient
	branchClient  corev1.BranchServiceClient
	commitClient  corev1.CommitServiceClient
	mappingClient corev1.MappingServiceClient
	resourceRepo  *resources.Repository
	logger        *logger.Logger
}

// CommitSchemaStructure represents the new format for storing schema in commits
// It contains snapshots of resource_containers and resource_items
type CommitSchemaStructure struct {
	Containers []ResourceContainerSnapshot `json:"containers"`
	Items      []ResourceItemSnapshot      `json:"items"`
}

// ResourceContainerSnapshot is a snapshot of a resource container for commit storage
// It excludes database-specific fields like ContainerID, Created, Updated
type ResourceContainerSnapshot struct {
	TenantID                          string                 `json:"tenant_id"`
	WorkspaceID                       string                 `json:"workspace_id"`
	ResourceURI                       string                 `json:"resource_uri"`
	Protocol                          string                 `json:"protocol"`
	Scope                             string                 `json:"scope"`
	ObjectType                        string                 `json:"object_type"`
	ObjectName                        string                 `json:"object_name"`
	DatabaseID                        *string                `json:"database_id,omitempty"`
	ChangeStatus                      string                 `json:"change_status"` // STATUS_CREATED, STATUS_UPDATED, STATUS_DELETED, STATUS_UNCHANGED
	ContainerMetadata                 map[string]interface{} `json:"container_metadata"`
	EnrichedMetadata                  map[string]interface{} `json:"enriched_metadata"`
	DatabaseType                      *string                `json:"database_type,omitempty"`
	Vendor                            *string                `json:"vendor,omitempty"`
	ItemCount                         int                    `json:"item_count"`
	SizeBytes                         int64                  `json:"size_bytes,omitempty"`
	ContainerClassification           *string                `json:"container_classification,omitempty"`
	ContainerClassificationConfidence *float64               `json:"container_classification_confidence,omitempty"`
	ContainerClassificationSource     string                 `json:"container_classification_source"`
}

// ResourceItemSnapshot is a snapshot of a resource item for commit storage
// It excludes database-specific fields like ItemID, and uses URIs for linking
type ResourceItemSnapshot struct {
	ResourceURI              string                   `json:"resource_uri"`
	ContainerURI             string                   `json:"container_uri"` // Reference to parent container
	Protocol                 string                   `json:"protocol"`
	Scope                    string                   `json:"scope"`
	ItemType                 string                   `json:"item_type"`
	ItemName                 string                   `json:"item_name"`
	ItemDisplayName          string                   `json:"item_display_name"`
	ItemPath                 []string                 `json:"item_path,omitempty"`
	DataType                 string                   `json:"data_type"`
	UnifiedDataType          *string                  `json:"unified_data_type,omitempty"`
	IsNullable               bool                     `json:"is_nullable"`
	IsPrimaryKey             bool                     `json:"is_primary_key"`
	IsUnique                 bool                     `json:"is_unique"`
	IsIndexed                bool                     `json:"is_indexed"`
	IsRequired               bool                     `json:"is_required"`
	IsArray                  bool                     `json:"is_array"`
	ArrayDimensions          int                      `json:"array_dimensions,omitempty"`
	DefaultValue             *string                  `json:"default_value,omitempty"`
	Constraints              []map[string]interface{} `json:"constraints,omitempty"`
	IsPrivileged             bool                     `json:"is_privileged"`
	PrivilegedClassification *string                  `json:"privileged_classification,omitempty"`
	DetectionConfidence      *float64                 `json:"detection_confidence,omitempty"`
	DetectionMethod          *string                  `json:"detection_method,omitempty"`
	OrdinalPosition          *int                     `json:"ordinal_position,omitempty"`
	MaxLength                *int                     `json:"max_length,omitempty"`
	Precision                *int                     `json:"precision,omitempty"`
	Scale                    *int                     `json:"scale,omitempty"`
	ItemComment              *string                  `json:"item_comment,omitempty"`
	ChangeStatus             string                   `json:"change_status"` // STATUS_CREATED, STATUS_UPDATED, STATUS_DELETED, STATUS_UNCHANGED
}

func NewSchemaWatcher(db *database.PostgreSQL, umConn *grpc.ClientConn, coreConn *grpc.ClientConn, supervisorAddr string, logger *logger.Logger) *SchemaWatcher {
	return &SchemaWatcher{
		state:         state.GetInstance(),
		db:            db,
		umClient:      pb.NewUnifiedModelServiceClient(umConn),
		repoClient:    corev1.NewRepoServiceClient(coreConn),
		branchClient:  corev1.NewBranchServiceClient(coreConn),
		commitClient:  corev1.NewCommitServiceClient(coreConn),
		mappingClient: corev1.NewMappingServiceClient(coreConn),
		resourceRepo:  resources.NewRepository(db.Pool()),
		logger:        logger,
	}
}

// ensureRepoBranchCommit ensures that a repository, branch, and commit exist for storing schema changes.
// It queries the resource_containers and resource_items tables for the database, compares with previous commit,
// determines change status for each container/item, and stores them in the commit with appropriate status.
// It also updates the status of actual resource_containers and resource_items in the database.
// It returns the branch ID and commit ID if successful.
func (w *SchemaWatcher) ensureRepoBranchCommit(ctx context.Context, workspaceID, databaseID, commitMessage string) (string, string, error) {
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

	// Get previous commit to compare schemas
	var previousContainers map[string]ResourceContainerSnapshot
	var previousItems map[string]ResourceItemSnapshot

	// Try to get the latest commit for this database
	latestCommit, err := w.state.GetConfigRepository().GetLatestStoredDatabaseSchema(ctx, databaseID)
	if err == nil && latestCommit != nil && latestCommit.Schema != nil {
		w.logInfo("Found latest commit for database %s, attempting to parse previous schema", databaseID)
		// Parse the previous schema structure
		var schemaBytes []byte
		if str, ok := latestCommit.Schema.(string); ok {
			schemaBytes = []byte(str)
			w.logInfo("Previous schema is a string, length: %d bytes", len(schemaBytes))
		} else if bytes, ok := latestCommit.Schema.([]byte); ok {
			schemaBytes = bytes
			w.logInfo("Previous schema is []byte, length: %d bytes", len(schemaBytes))
		} else {
			// Try to marshal it first
			schemaBytes, _ = json.Marshal(latestCommit.Schema)
			w.logInfo("Previous schema is %T, marshaled to %d bytes", latestCommit.Schema, len(schemaBytes))
		}

		if len(schemaBytes) > 0 {
			var prevCommitSchema CommitSchemaStructure
			err = json.Unmarshal(schemaBytes, &prevCommitSchema)
			if err == nil {
				// Build maps for easy lookup
				previousContainers = make(map[string]ResourceContainerSnapshot)
				for _, c := range prevCommitSchema.Containers {
					previousContainers[c.ResourceURI] = c
				}
				previousItems = make(map[string]ResourceItemSnapshot)
				for _, item := range prevCommitSchema.Items {
					previousItems[item.ResourceURI] = item
				}
				w.logInfo("Successfully loaded previous commit with %d containers and %d items", len(previousContainers), len(previousItems))
			} else {
				w.logWarn("Failed to unmarshal previous commit schema: %v (schema bytes preview: %s...)", err, string(schemaBytes[:min(200, len(schemaBytes))]))
			}
		} else {
			w.logWarn("Previous schema bytes are empty for database %s", databaseID)
		}
	} else {
		if err != nil {
			w.logInfo("No previous commit found for database %s (error: %v)", databaseID, err)
		} else if latestCommit == nil {
			w.logInfo("No previous commit found for database %s (latestCommit is nil)", databaseID)
		} else if latestCommit.Schema == nil {
			w.logInfo("No previous commit schema found for database %s (schema is nil)", databaseID)
		}
	}

	// Query ALL resource_containers for this database from the resource registry
	w.logInfo("Querying resource containers for database %s to create commit", databaseID)
	currentContainers, err := w.resourceRepo.ListContainersByDatabase(ctx, databaseID)
	if err != nil {
		w.logError("Failed to query containers: %v", err)
		return "", "", fmt.Errorf("failed to query containers: %w", err)
	}

	w.logInfo("Found %d current containers for database %s", len(currentContainers), databaseID)

	// Query ALL resource_items for this database
	currentItems := []*models.ResourceItem{}
	for _, container := range currentContainers {
		items, err := w.resourceRepo.ListItemsByContainer(ctx, container.ContainerID)
		if err != nil {
			w.logWarn("Failed to query items for container %s: %v", container.ContainerID, err)
			continue
		}
		currentItems = append(currentItems, items...)
	}

	w.logInfo("Found %d current items across all containers for database %s", len(currentItems), databaseID)

	// Build maps of current containers and items
	currentContainerMap := make(map[string]*models.ResourceContainer)
	for _, c := range currentContainers {
		currentContainerMap[c.ResourceURI] = c
	}

	currentItemMap := make(map[string]*models.ResourceItem)
	for _, item := range currentItems {
		currentItemMap[item.ResourceURI] = item
	}

	// Create snapshots with change status
	var containerSnapshots []ResourceContainerSnapshot
	var itemSnapshots []ResourceItemSnapshot

	// Track counts for logging
	containerCounts := make(map[string]int)
	itemCounts := make(map[string]int)

	// Process current containers (created, updated, unchanged)
	for _, c := range currentContainers {
		snapshot := convertToContainerSnapshot(c)

		if prevContainer, existedBefore := previousContainers[c.ResourceURI]; existedBefore {
			// Check if the previous version was deleted - if so, treat current as new
			if prevContainer.ChangeStatus == "STATUS_DELETED" {
				// Was deleted before, now it's being recreated - mark as CREATED
				w.logInfo("Container %s marked as CREATED (was previously deleted, now recreated)", c.ResourceURI)
				snapshot.ChangeStatus = "STATUS_CREATED"
				// Update status in database
				w.resourceRepo.UpdateContainer(ctx, c.ContainerID, map[string]interface{}{"status": "STATUS_CREATED"})
				containerCounts["created"]++
			} else {
				// Exists in previous and was not deleted - check if changed
				if w.containerHasChanged(prevContainer, snapshot) {
					snapshot.ChangeStatus = "STATUS_UPDATED"
					// Update status in database
					w.resourceRepo.UpdateContainer(ctx, c.ContainerID, map[string]interface{}{"status": "STATUS_UPDATED"})
					containerCounts["updated"]++
					w.logDebug("Container %s marked as UPDATED", c.ResourceURI)
				} else {
					snapshot.ChangeStatus = "STATUS_UNCHANGED"
					containerCounts["unchanged"]++
					w.logDebug("Container %s marked as UNCHANGED", c.ResourceURI)
				}
			}
		} else {
			// New container - log details
			w.logInfo("Container %s marked as CREATED (not found in previous commit with %d containers)", c.ResourceURI, len(previousContainers))
			if len(previousContainers) > 0 && len(previousContainers) < 10 {
				// Log the URIs in the previous commit for debugging (only if small number)
				w.logDebug("Previous commit container URIs: %v", getMapKeys(previousContainers))
			}
			snapshot.ChangeStatus = "STATUS_CREATED"
			// Update status in database
			w.resourceRepo.UpdateContainer(ctx, c.ContainerID, map[string]interface{}{"status": "STATUS_CREATED"})
			containerCounts["created"]++
		}

		containerSnapshots = append(containerSnapshots, snapshot)
	}

	// Add deleted containers from previous commit
	// Only include containers that were NOT already marked as deleted in the previous commit
	for uri, prevContainer := range previousContainers {
		if _, stillExists := currentContainerMap[uri]; !stillExists {
			// Only include if it wasn't already deleted in the previous commit
			if prevContainer.ChangeStatus != "STATUS_DELETED" {
				prevContainer.ChangeStatus = "STATUS_DELETED"
				containerSnapshots = append(containerSnapshots, prevContainer)
				containerCounts["deleted"]++
			}
		}
	}

	// Process current items (created, updated, unchanged)
	for _, item := range currentItems {
		snapshot := convertToItemSnapshot(item)

		if prevItem, existedBefore := previousItems[item.ResourceURI]; existedBefore {
			// Check if the previous version was deleted - if so, treat current as new
			if prevItem.ChangeStatus == "STATUS_DELETED" {
				// Was deleted before, now it's being recreated - mark as CREATED
				w.logDebug("Item %s marked as CREATED (was previously deleted, now recreated)", item.ResourceURI)
				snapshot.ChangeStatus = "STATUS_CREATED"
				// Update status in database
				w.resourceRepo.UpdateItem(ctx, item.ItemID, map[string]interface{}{"status": "STATUS_CREATED"})
				itemCounts["created"]++
			} else {
				// Exists in previous and was not deleted - check if changed
				if w.itemHasChanged(prevItem, snapshot) {
					snapshot.ChangeStatus = "STATUS_UPDATED"
					// Update status in database
					w.resourceRepo.UpdateItem(ctx, item.ItemID, map[string]interface{}{"status": "STATUS_UPDATED"})
					itemCounts["updated"]++
				} else {
					snapshot.ChangeStatus = "STATUS_UNCHANGED"
					itemCounts["unchanged"]++
				}
			}
		} else {
			// New item
			snapshot.ChangeStatus = "STATUS_CREATED"
			// Update status in database
			w.resourceRepo.UpdateItem(ctx, item.ItemID, map[string]interface{}{"status": "STATUS_CREATED"})
			itemCounts["created"]++
		}

		itemSnapshots = append(itemSnapshots, snapshot)
	}

	// Add deleted items from previous commit
	// Only include items that were NOT already marked as deleted in the previous commit
	for uri, prevItem := range previousItems {
		if _, stillExists := currentItemMap[uri]; !stillExists {
			// Only include if it wasn't already deleted in the previous commit
			if prevItem.ChangeStatus != "STATUS_DELETED" {
				prevItem.ChangeStatus = "STATUS_DELETED"
				itemSnapshots = append(itemSnapshots, prevItem)
				itemCounts["deleted"]++
			}
		}
	}

	// Log change summary
	w.logInfo("Commit change summary for database %s:", databaseID)
	w.logInfo("  Containers: created=%d, updated=%d, deleted=%d, unchanged=%d",
		containerCounts["created"], containerCounts["updated"], containerCounts["deleted"], containerCounts["unchanged"])
	w.logInfo("  Items: created=%d, updated=%d, deleted=%d, unchanged=%d",
		itemCounts["created"], itemCounts["updated"], itemCounts["deleted"], itemCounts["unchanged"])

	// Create commit schema structure with resource containers and items
	commitSchema := CommitSchemaStructure{
		Containers: containerSnapshots,
		Items:      itemSnapshots,
	}

	// Marshal to JSON
	schemaStructure, err := json.Marshal(commitSchema)
	if err != nil {
		w.logError("Failed to marshal commit schema: %v", err)
		return "", "", fmt.Errorf("failed to marshal commit schema: %w", err)
	}

	w.logInfo("Marshaled commit schema: %d bytes for database %s", len(schemaStructure), databaseID)

	// Create a new commit with the resource-based schema structure
	commitResp, err := w.commitClient.CreateCommitByAnchor(ctx, &corev1.CreateCommitByAnchorRequest{
		BranchId:        branchID,
		CommitMessage:   commitMessage,
		IsHead:          true,
		SchemaType:      "resource_registry", // New schema type to distinguish from legacy format
		SchemaStructure: string(schemaStructure),
	})
	if err != nil {
		w.logError("Failed to create commit: %v", err)
		return "", "", fmt.Errorf("failed to create commit: %w", err)
	}

	w.logInfo("Successfully created commit %s for database %s with resource-based schema", commitResp.CommitId, databaseID)

	return branchID, commitResp.CommitId, nil
}

// storeSchemaAndCreateCommit handles the complete flow of storing a schema change:
// 1. Stores UnifiedModel in database_schema (for backward compatibility)
// 2. Calls enriched analysis
// 3. Populates resource registry with containers and items
// 4. Creates a commit with the resource-based schema structure
func (w *SchemaWatcher) storeSchemaAndCreateCommit(ctx context.Context, workspaceID, databaseID, schemaType string, currentUM *unifiedmodel.UnifiedModel, commitMessage string) error {
	// Marshal the UnifiedModel to JSON for legacy storage
	currentBytes, err := json.Marshal(currentUM)
	if err != nil {
		return fmt.Errorf("failed to marshal schema: %w", err)
	}

	// 1. Store UnifiedModel in database_schema for backward compatibility
	w.logInfo("Storing UnifiedModel schema for database %s (legacy compatibility)", databaseID)
	_, err = w.db.Pool().Exec(ctx, "UPDATE databases SET database_schema = $1 WHERE database_id = $2", string(currentBytes), databaseID)
	if err != nil {
		w.logError("Failed to store schema in database: %v", err)
		return fmt.Errorf("failed to store schema in database: %w", err)
	}

	// 2. Request enriched analysis from the unified model service
	w.logInfo("Requesting enriched analysis for database %s", databaseID)
	var enrichedResp *pb.AnalyzeSchemaEnrichedResponse
	enrichedResp, err = w.umClient.AnalyzeSchemaEnriched(ctx, &pb.AnalyzeSchemaEnrichedRequest{
		SchemaType:   schemaType,
		UnifiedModel: currentUM.ToProto(),
	})
	if err != nil {
		w.logError("Failed to get enriched analysis: %v", err)
		// Don't fail the entire operation if enriched analysis fails
		enrichedResp = nil
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
	}

	// 3. Populate resource registry tables with containers and items
	w.logInfo("Populating resource registry for database %s", databaseID)
	err = w.populateResourceRegistry(ctx, currentUM, databaseID, enrichedResp)
	if err != nil {
		w.logError("Failed to populate resource registry for database %s: %v", databaseID, err)
		return fmt.Errorf("failed to populate resource registry: %w", err)
	}
	w.logInfo("Successfully populated resource registry for database %s", databaseID)

	// 4. Create commit with resource-based schema structure
	w.logInfo("Creating commit with resource-based schema for database %s", databaseID)
	_, _, err = w.ensureRepoBranchCommit(ctx, workspaceID, databaseID, commitMessage)
	if err != nil {
		w.logError("Failed to create commit: %v", err)
		return fmt.Errorf("failed to create commit: %w", err)
	}

	return nil
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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func getMapKeys(m map[string]ResourceContainerSnapshot) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
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

		// Log schema discovery summary
		collectionCount := len(currentUM.Collections)
		tableCount := len(currentUM.Tables)
		w.logInfo("Discovered schema for database %s: %d collections, %d tables", clientID, collectionCount, tableCount)

		// Marshal the current schema to JSON for storage (still needed for database storage)
		currentBytes, err := json.Marshal(currentUM)
		if err != nil {
			w.logError("Failed to marshal current schema: %v", err)
			continue
		}

		// Validate the marshaled JSON is not empty or just empty object
		schemaSize := len(currentBytes)
		w.logInfo("Marshaled schema for database %s: %d bytes", clientID, schemaSize)

		if schemaSize < 10 || string(currentBytes) == "{}" {
			w.logError("WARNING: Marshaled schema for database %s is empty or nearly empty (%d bytes): %s",
				clientID, schemaSize, string(currentBytes))
			w.logError("Schema had %d collections and %d tables before marshaling", collectionCount, tableCount)
		}

		// Verify schema integrity by unmarshaling and checking
		var verifyUM unifiedmodel.UnifiedModel
		if err := json.Unmarshal(currentBytes, &verifyUM); err == nil {
			verifyCollectionCount := len(verifyUM.Collections)
			verifyTableCount := len(verifyUM.Tables)
			if verifyCollectionCount != collectionCount || verifyTableCount != tableCount {
				w.logWarn("Schema count mismatch after JSON round-trip for database %s: collections %d->%d, tables %d->%d",
					clientID, collectionCount, verifyCollectionCount, tableCount, verifyTableCount)
			} else {
				w.logDebug("Schema integrity verified for database %s after JSON round-trip", clientID)
			}
		}

		// If we have a previous schema to compare against
		if client.LastSchema != nil {
			// Ensure previous schema is also a UnifiedModel
			previousUM, ok := client.LastSchema.(*unifiedmodel.UnifiedModel)
			if !ok {
				w.logError("Previous schema is not a UnifiedModel for database %s", clientID)
				continue
			}

			// Check if the stored database schema is empty or invalid (force re-storage if needed)
			var storedSchema string
			err := w.db.Pool().QueryRow(ctx, "SELECT database_schema FROM databases WHERE database_id = $1", client.Config.DatabaseID).Scan(&storedSchema)
			forceStore := false
			if err != nil {
				w.logWarn("Failed to check stored schema for database %s: %v (will force storage)", clientID, err)
				forceStore = true
			} else if storedSchema == "" || storedSchema == "{}" || len(storedSchema) < 10 {
				w.logInfo("Stored schema for database %s is empty or invalid (%d bytes), forcing re-storage", clientID, len(storedSchema))
				forceStore = true
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

			if compareResp.HasChanges || forceStore {
				if compareResp.HasChanges {
					w.logInfo("Schema changes detected for database %s", clientID)
				} else {
					w.logInfo("Forcing schema storage for database %s (stored schema was empty/invalid)", clientID)
				}

				var commitMessage string
				if compareResp.HasChanges {
					for _, change := range compareResp.Changes {
						w.logInfo("Schema change: %s", change)
						commitMessage += change + "\n"
					}
				} else {
					commitMessage = "Re-storing valid schema (previous storage was empty/invalid)"
				}

				// Store schema and create commit with resource-based structure
				err = w.storeSchemaAndCreateCommit(ctx, client.Config.WorkspaceID, client.Config.DatabaseID, client.Config.ConnectionType, currentUM, commitMessage)
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
			w.logDebug("No previous schema found for database %s (first discovery after startup), checking for existing commits", clientID)

			// Try to get the latest commit for this database
			w.logDebug("Fetching latest commit for database %s", clientID)

			// Create a timeout context for the GetLatestCommitForDatabase call
			timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			latestCommit, err := w.state.GetConfigRepository().GetLatestStoredDatabaseSchema(timeoutCtx, client.Config.DatabaseID)
			cancel() // Cancel the timeout context

			if err != nil || latestCommit == nil || !latestCommit.CommitExists {
				// No existing commit found - this is truly the first time we're seeing this database
				w.logInfo("No existing commit found for database %s, creating initial commit", clientID)

				// Store the initial schema in the internal database
				err = w.storeSchemaAndCreateCommit(ctx, client.Config.WorkspaceID, client.Config.DatabaseID, client.Config.ConnectionType, currentUM, "Discovered schema")
				if err != nil {
					w.logError("Failed to store initial schema: %v", err)
					continue
				}
			} else {
				// A commit exists - this means we're just starting up and haven't loaded LastSchema yet
				// Instead of comparing (which would fail because the commit is in the new format),
				// just set LastSchema to current and skip this cycle.
				// The next cycle will properly compare and detect any real changes.
				w.logInfo("Found existing commit for database %s on startup - setting current schema as baseline for future comparisons", clientID)
				w.logDebug("Skipping commit creation on startup to avoid false 'added' detections")

				// Update the database_schema column to ensure we have the latest UnifiedModel stored
				// This is for legacy compatibility and doesn't create a commit
				currentBytes, err := json.Marshal(currentUM)
				if err != nil {
					w.logError("Failed to marshal current schema for database_schema update: %v", err)
				} else {
					_, err = w.db.Pool().Exec(ctx, "UPDATE databases SET database_schema = $1 WHERE database_id = $2", string(currentBytes), client.Config.DatabaseID)
					if err != nil {
						w.logError("Failed to update database_schema on startup: %v", err)
					}
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

	w.logInfo("Successfully populated resource registry for database %s: %d containers + %d items",
		databaseID, len(containers), len(items))

	// Reconcile virtual resources with newly discovered schema
	err = w.reconcileVirtualResources(ctx, databaseID, containers, items)
	if err != nil {
		// Log but don't fail - reconciliation is best-effort
		w.logWarn("Failed to reconcile virtual resources for database %s: %v", databaseID, err)
	}

	return containersCreated, itemsCreated, nil
}

// convertToContainerSnapshot converts a ResourceContainer to a ResourceContainerSnapshot for commit storage
func convertToContainerSnapshot(c *models.ResourceContainer) ResourceContainerSnapshot {
	return ResourceContainerSnapshot{
		TenantID:                          c.TenantID,
		WorkspaceID:                       c.WorkspaceID,
		ResourceURI:                       c.ResourceURI,
		Protocol:                          c.Protocol,
		Scope:                             c.Scope,
		ObjectType:                        c.ObjectType,
		ObjectName:                        c.ObjectName,
		DatabaseID:                        c.DatabaseID,
		ChangeStatus:                      "", // Will be set by caller
		ContainerMetadata:                 c.ContainerMetadata,
		EnrichedMetadata:                  c.EnrichedMetadata,
		DatabaseType:                      c.DatabaseType,
		Vendor:                            c.Vendor,
		ItemCount:                         c.ItemCount,
		SizeBytes:                         c.SizeBytes,
		ContainerClassification:           c.ContainerClassification,
		ContainerClassificationConfidence: c.ContainerClassificationConfidence,
		ContainerClassificationSource:     c.ContainerClassificationSource,
	}
}

// convertToItemSnapshot converts a ResourceItem to a ResourceItemSnapshot for commit storage
func convertToItemSnapshot(item *models.ResourceItem) ResourceItemSnapshot {
	// Extract container URI from item URI
	containerURI := extractContainerURIFromItemURI(item.ResourceURI)

	return ResourceItemSnapshot{
		ResourceURI:              item.ResourceURI,
		ContainerURI:             containerURI,
		Protocol:                 item.Protocol,
		Scope:                    item.Scope,
		ItemType:                 item.ItemType,
		ItemName:                 item.ItemName,
		ItemDisplayName:          item.ItemDisplayName,
		ItemPath:                 item.ItemPath,
		DataType:                 item.DataType,
		UnifiedDataType:          item.UnifiedDataType,
		IsNullable:               item.IsNullable,
		IsPrimaryKey:             item.IsPrimaryKey,
		IsUnique:                 item.IsUnique,
		IsIndexed:                item.IsIndexed,
		IsRequired:               item.IsRequired,
		IsArray:                  item.IsArray,
		ArrayDimensions:          item.ArrayDimensions,
		DefaultValue:             item.DefaultValue,
		Constraints:              item.Constraints,
		IsPrivileged:             item.IsPrivileged,
		PrivilegedClassification: item.PrivilegedClassification,
		DetectionConfidence:      item.DetectionConfidence,
		DetectionMethod:          item.DetectionMethod,
		OrdinalPosition:          item.OrdinalPosition,
		MaxLength:                item.MaxLength,
		Precision:                item.Precision,
		Scale:                    item.Scale,
		ItemComment:              item.ItemComment,
		ChangeStatus:             "", // Will be set by caller
	}
}

// containerHasChanged checks if a container has any significant changes
func (w *SchemaWatcher) containerHasChanged(prev, curr ResourceContainerSnapshot) bool {
	// Compare key fields that would indicate a meaningful change
	if prev.ObjectType != curr.ObjectType ||
		prev.ObjectName != curr.ObjectName ||
		prev.ItemCount != curr.ItemCount {
		return true
	}

	// Compare classification fields
	if (prev.ContainerClassification == nil && curr.ContainerClassification != nil) ||
		(prev.ContainerClassification != nil && curr.ContainerClassification == nil) ||
		(prev.ContainerClassification != nil && curr.ContainerClassification != nil && *prev.ContainerClassification != *curr.ContainerClassification) {
		return true
	}

	// Compare metadata (simplified check - could be more sophisticated)
	prevMetaJSON, _ := json.Marshal(prev.ContainerMetadata)
	currMetaJSON, _ := json.Marshal(curr.ContainerMetadata)
	if string(prevMetaJSON) != string(currMetaJSON) {
		return true
	}

	return false
}

// itemHasChanged checks if an item has any significant changes
func (w *SchemaWatcher) itemHasChanged(prev, curr ResourceItemSnapshot) bool {
	// Compare key fields that would indicate a meaningful change
	if prev.ItemName != curr.ItemName ||
		prev.DataType != curr.DataType ||
		prev.IsNullable != curr.IsNullable ||
		prev.IsPrimaryKey != curr.IsPrimaryKey ||
		prev.IsUnique != curr.IsUnique ||
		prev.IsRequired != curr.IsRequired ||
		prev.IsArray != curr.IsArray {
		return true
	}

	// Compare optional fields
	if (prev.UnifiedDataType == nil && curr.UnifiedDataType != nil) ||
		(prev.UnifiedDataType != nil && curr.UnifiedDataType == nil) ||
		(prev.UnifiedDataType != nil && curr.UnifiedDataType != nil && *prev.UnifiedDataType != *curr.UnifiedDataType) {
		return true
	}

	if (prev.DefaultValue == nil && curr.DefaultValue != nil) ||
		(prev.DefaultValue != nil && curr.DefaultValue == nil) ||
		(prev.DefaultValue != nil && curr.DefaultValue != nil && *prev.DefaultValue != *curr.DefaultValue) {
		return true
	}

	// Compare privilege classification
	if prev.IsPrivileged != curr.IsPrivileged {
		return true
	}

	if (prev.PrivilegedClassification == nil && curr.PrivilegedClassification != nil) ||
		(prev.PrivilegedClassification != nil && curr.PrivilegedClassification == nil) ||
		(prev.PrivilegedClassification != nil && curr.PrivilegedClassification != nil && *prev.PrivilegedClassification != *curr.PrivilegedClassification) {
		return true
	}

	return false
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
