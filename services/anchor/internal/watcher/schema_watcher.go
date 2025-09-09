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
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
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
		logger:       logger,
	}
}

// ensureRepoBranchCommit ensures that a repository, branch, and commit exist for storing schema changes.
// It returns the branch ID and commit ID if successful.
func (w *SchemaWatcher) ensureRepoBranchCommit(ctx context.Context, workspaceID, databaseID, schemaType string, schemaStructure []byte, commitMessage string) (string, string, error) {
	// Get database name from the database client
	dbManager := w.state.GetDatabaseManager()
	client, err := dbManager.GetDatabaseClient(databaseID)
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
func (w *SchemaWatcher) logInfo(msg string, args ...interface{}) {
	if w.logger != nil {
		if len(args) > 0 {
			w.logger.Infof(msg, args...)
		} else {
			w.logger.Info(msg)
		}
	}
}

func (w *SchemaWatcher) logError(msg string, args ...interface{}) {
	if w.logger != nil {
		if len(args) > 0 {
			w.logger.Errorf(msg, args...)
		} else {
			w.logger.Error(msg)
		}
	}
}

func (w *SchemaWatcher) logWarn(msg string, args ...interface{}) {
	if w.logger != nil {
		if len(args) > 0 {
			w.logger.Warnf(msg, args...)
		} else {
			w.logger.Warn(msg)
		}
	}
}

func (w *SchemaWatcher) checkSchemaChanges(ctx context.Context) error {
	// Check if context is cancelled before starting
	if ctx.Err() != nil {
		return ctx.Err()
	}

	dbManager := w.state.GetDatabaseManager()

	// Get all connected database clients
	for _, clientID := range dbManager.GetAllDatabaseClientIDs() {
		// Check if context is cancelled before processing each database
		if ctx.Err() != nil {
			return ctx.Err()
		}

		w.logInfo("Checking schema for database: %s", clientID)

		client, err := dbManager.GetDatabaseClient(clientID)
		if err != nil {
			w.logError("Failed to get database client %s: %v", clientID, err)
			continue
		}

		// Get current schema structure as UnifiedModel
		structure, err := dbManager.GetDatabaseStructure(clientID)
		if err != nil {
			w.logError("Failed to get schema for database %s: %v", clientID, err)
			continue
		}

		// Ensure we have a UnifiedModel
		currentUM, ok := structure.(*unifiedmodel.UnifiedModel)
		if !ok {
			w.logError("Database structure is not a UnifiedModel for database %s", clientID)
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
			w.logInfo("Comparing schemas for database %s", clientID)
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
					commitMessage += change
				}

				// Store the schema changes in the internal database
				_, _, err := w.ensureRepoBranchCommit(ctx, client.Config.WorkspaceID, client.Config.DatabaseID, client.Config.ConnectionType, currentBytes, commitMessage)
				if err != nil {
					w.logError("Failed to store schema changes: %v", err)
					continue
				}
			} else {
				w.logInfo("No schema changes detected for database %s", clientID)
			}

			// Log any warnings
			for _, warning := range compareResp.Warnings {
				w.logWarn("Schema comparison warning for %s: %s", clientID, warning)
			}
		} else {
			w.logInfo("No previous schema found for database %s, checking for existing commits", clientID)

			// Try to get the latest commit for this database
			w.logInfo("Fetching latest commit for database %s", clientID)

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
						commitMessage += change
					}

					// Store the schema changes in the internal database
					_, _, err := w.ensureRepoBranchCommit(ctx, client.Config.WorkspaceID, client.Config.DatabaseID, client.Config.ConnectionType, currentBytes, commitMessage)
					if err != nil {
						w.logError("Failed to store schema changes: %v", err)
						continue
					}
				} else {
					w.logInfo("No schema changes detected for database %s", clientID)
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
