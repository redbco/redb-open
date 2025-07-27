package database

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/anchor/internal/database/postgres"
)

// ReplicationManager handles replication-related operations
type ReplicationManager struct {
	dm     *DatabaseManager
	logger *logger.Logger
}

// NewReplicationManager creates a new ReplicationManager instance
func NewReplicationManager(dm *DatabaseManager) *ReplicationManager {
	return &ReplicationManager{
		dm: dm,
	}
}

// SetLogger sets the logger for the replication manager
func (rm *ReplicationManager) SetLogger(logger *logger.Logger) {
	rm.logger = logger
}

// safeLog safely logs a message if logger is available
func (rm *ReplicationManager) safeLog(level string, format string, args ...interface{}) {
	if rm.logger != nil {
		switch level {
		case "info":
			rm.logger.Info(format, args...)
		case "error":
			rm.logger.Error(format, args...)
		case "warn":
			rm.logger.Warn(format, args...)
		case "debug":
			rm.logger.Debug(format, args...)
		}
	}
}

// CreateReplicationSource creates a replication source for a database
func (rm *ReplicationManager) CreateReplicationSource(databaseID string, tableName string, eventHandler func(map[string]interface{})) (*postgres.PostgresReplicationSourceDetails, error) {
	rm.safeLog("info", "Creating replication source for database %s, table %s", databaseID, tableName)

	client, err := rm.dm.GetDatabaseClient(databaseID)
	if err != nil {
		return nil, fmt.Errorf("failed to get database client: %w", err)
	}

	if client.DatabaseType != "postgres" {
		return nil, fmt.Errorf("replication source creation is only supported for PostgreSQL databases")
	}

	// Type assertion to convert interface{} to *pgxpool.Pool
	pool, ok := client.DB.(*pgxpool.Pool)
	if !ok {
		return nil, fmt.Errorf("invalid database connection type for PostgreSQL")
	}

	// Get the actual database name from the client config
	databaseName := client.Config.DatabaseName
	if databaseName == "" {
		return nil, fmt.Errorf("database name not found in client config")
	}

	details, err := postgres.CreateReplicationSource(pool, []string{tableName}, databaseID, databaseName, eventHandler, rm.logger)
	if err != nil {
		rm.safeLog("error", "Failed to create replication source: %v", err)
		return nil, fmt.Errorf("failed to create replication source: %w", err)
	}

	rm.safeLog("info", "Successfully created replication source for database %s, table %s", databaseID, tableName)
	return details, nil
}

// GetReplicationStatus gets the replication status for a database
func (rm *ReplicationManager) GetReplicationStatus(databaseID string) (map[string]interface{}, error) {
	rm.safeLog("info", "Getting replication status for database %s", databaseID)

	client, err := rm.dm.GetDatabaseClient(databaseID)
	if err != nil {
		return nil, fmt.Errorf("failed to get database client: %w", err)
	}

	if client.DatabaseType != "postgres" {
		return nil, fmt.Errorf("replication status is only supported for PostgreSQL databases")
	}

	// Type assertion to convert interface{} to *pgxpool.Pool
	pool, ok := client.DB.(*pgxpool.Pool)
	if !ok {
		return nil, fmt.Errorf("invalid database connection type for PostgreSQL")
	}

	status, err := postgres.GetReplicationStatus(pool, databaseID)
	if err != nil {
		rm.safeLog("error", "Failed to get replication status: %v", err)
		return nil, fmt.Errorf("failed to get replication status: %w", err)
	}

	rm.safeLog("info", "Retrieved replication status for database %s", databaseID)
	return status, nil
}

// ListReplicationSlots lists all replication slots for a database
func (rm *ReplicationManager) ListReplicationSlots(databaseID string) ([]map[string]interface{}, error) {
	rm.safeLog("info", "Listing replication slots for database %s", databaseID)

	client, err := rm.dm.GetDatabaseClient(databaseID)
	if err != nil {
		return nil, fmt.Errorf("failed to get database client: %w", err)
	}

	if client.DatabaseType != "postgres" {
		return nil, fmt.Errorf("replication slots listing is only supported for PostgreSQL databases")
	}

	// Type assertion to convert interface{} to *pgxpool.Pool
	pool, ok := client.DB.(*pgxpool.Pool)
	if !ok {
		return nil, fmt.Errorf("invalid database connection type for PostgreSQL")
	}

	slots, err := postgres.ListReplicationSlots(pool, databaseID)
	if err != nil {
		rm.safeLog("error", "Failed to list replication slots: %v", err)
		return nil, fmt.Errorf("failed to list replication slots: %w", err)
	}

	rm.safeLog("info", "Retrieved %d replication slots for database %s", len(slots), databaseID)
	return slots, nil
}

// DropReplicationSlot drops a replication slot
func (rm *ReplicationManager) DropReplicationSlot(databaseID string, slotName string) error {
	rm.safeLog("info", "Dropping replication slot %s for database %s", slotName, databaseID)

	client, err := rm.dm.GetDatabaseClient(databaseID)
	if err != nil {
		return fmt.Errorf("failed to get database client: %w", err)
	}

	if client.DatabaseType != "postgres" {
		return fmt.Errorf("replication slot dropping is only supported for PostgreSQL databases")
	}

	// Type assertion to convert interface{} to *pgxpool.Pool
	pool, ok := client.DB.(*pgxpool.Pool)
	if !ok {
		return fmt.Errorf("invalid database connection type for PostgreSQL")
	}

	err = postgres.DropReplicationSlot(pool, slotName)
	if err != nil {
		rm.safeLog("error", "Failed to drop replication slot: %v", err)
		return fmt.Errorf("failed to drop replication slot: %w", err)
	}

	rm.safeLog("info", "Successfully dropped replication slot %s for database %s", slotName, databaseID)
	return nil
}

// ListPublications lists all publications for a database
func (rm *ReplicationManager) ListPublications(databaseID string) ([]map[string]interface{}, error) {
	rm.safeLog("info", "Listing publications for database %s", databaseID)

	client, err := rm.dm.GetDatabaseClient(databaseID)
	if err != nil {
		return nil, fmt.Errorf("failed to get database client: %w", err)
	}

	if client.DatabaseType != "postgres" {
		return nil, fmt.Errorf("publications listing is only supported for PostgreSQL databases")
	}

	// Type assertion to convert interface{} to *pgxpool.Pool
	pool, ok := client.DB.(*pgxpool.Pool)
	if !ok {
		return nil, fmt.Errorf("invalid database connection type for PostgreSQL")
	}

	publications, err := postgres.ListPublications(pool)
	if err != nil {
		rm.safeLog("error", "Failed to list publications: %v", err)
		return nil, fmt.Errorf("failed to list publications: %w", err)
	}

	rm.safeLog("info", "Retrieved %d publications for database %s", len(publications), databaseID)
	return publications, nil
}

// DropPublication drops a publication
func (rm *ReplicationManager) DropPublication(databaseID string, publicationName string) error {
	rm.safeLog("info", "Dropping publication %s for database %s", publicationName, databaseID)

	client, err := rm.dm.GetDatabaseClient(databaseID)
	if err != nil {
		return fmt.Errorf("failed to get database client: %w", err)
	}

	if client.DatabaseType != "postgres" {
		return fmt.Errorf("publication dropping is only supported for PostgreSQL databases")
	}

	// Type assertion to convert interface{} to *pgxpool.Pool
	pool, ok := client.DB.(*pgxpool.Pool)
	if !ok {
		return fmt.Errorf("invalid database connection type for PostgreSQL")
	}

	err = postgres.DropPublication(pool, publicationName)
	if err != nil {
		rm.safeLog("error", "Failed to drop publication: %v", err)
		return fmt.Errorf("failed to drop publication: %w", err)
	}

	rm.safeLog("info", "Successfully dropped publication %s for database %s", publicationName, databaseID)
	return nil
}

// GetReplicationLag gets the replication lag for a database
func (rm *ReplicationManager) GetReplicationLag(databaseID string) (map[string]interface{}, error) {
	rm.safeLog("info", "Getting replication lag for database %s", databaseID)

	client, err := rm.dm.GetDatabaseClient(databaseID)
	if err != nil {
		return nil, fmt.Errorf("failed to get database client: %w", err)
	}

	if client.DatabaseType != "postgres" {
		return nil, fmt.Errorf("replication lag is only supported for PostgreSQL databases")
	}

	// Type assertion to convert interface{} to *pgxpool.Pool
	pool, ok := client.DB.(*pgxpool.Pool)
	if !ok {
		return nil, fmt.Errorf("invalid database connection type for PostgreSQL")
	}

	lag, err := postgres.GetReplicationLag(pool, databaseID)
	if err != nil {
		rm.safeLog("error", "Failed to get replication lag: %v", err)
		return nil, fmt.Errorf("failed to get replication lag: %w", err)
	}

	rm.safeLog("info", "Retrieved replication lag for database %s", databaseID)
	return lag, nil
}

// CheckPostgreSQLReplicationPrerequisites checks if PostgreSQL is configured for logical replication
func (rm *ReplicationManager) CheckPostgreSQLReplicationPrerequisites(databaseID string) error {
	rm.safeLog("info", "Checking PostgreSQL replication prerequisites for database %s", databaseID)

	client, err := rm.dm.GetDatabaseClient(databaseID)
	if err != nil {
		return fmt.Errorf("failed to get database client: %w", err)
	}

	if client.DatabaseType != "postgres" {
		return fmt.Errorf("replication prerequisites check is only supported for PostgreSQL databases")
	}

	// Type assertion to convert interface{} to *pgxpool.Pool
	pool, ok := client.DB.(*pgxpool.Pool)
	if !ok {
		return fmt.Errorf("invalid database connection type for PostgreSQL")
	}

	err = postgres.CheckLogicalReplicationPrerequisites(pool, rm.logger)
	if err != nil {
		rm.safeLog("error", "PostgreSQL replication prerequisites not met: %v", err)
		return fmt.Errorf("PostgreSQL replication prerequisites not met: %w", err)
	}

	rm.safeLog("info", "PostgreSQL replication prerequisites check passed for database %s", databaseID)
	return nil
}

// GetPostgreSQLReplicationConfig gets the current PostgreSQL replication configuration
func (rm *ReplicationManager) GetPostgreSQLReplicationConfig(databaseID string) (map[string]interface{}, error) {
	rm.safeLog("info", "Getting PostgreSQL replication configuration for database %s", databaseID)

	client, err := rm.dm.GetDatabaseClient(databaseID)
	if err != nil {
		return nil, fmt.Errorf("failed to get database client: %w", err)
	}

	if client.DatabaseType != "postgres" {
		return nil, fmt.Errorf("replication configuration is only supported for PostgreSQL databases")
	}

	// Type assertion to convert interface{} to *pgxpool.Pool
	pool, ok := client.DB.(*pgxpool.Pool)
	if !ok {
		return nil, fmt.Errorf("invalid database connection type for PostgreSQL")
	}

	config, err := postgres.GetPostgreSQLReplicationConfig(pool)
	if err != nil {
		rm.safeLog("error", "Failed to get PostgreSQL replication configuration: %v", err)
		return nil, fmt.Errorf("failed to get PostgreSQL replication configuration: %w", err)
	}

	rm.safeLog("info", "Retrieved PostgreSQL replication configuration for database %s", databaseID)
	return config, nil
}

// CleanupExistingReplicationSlots cleans up all existing replication slots for a database
func (rm *ReplicationManager) CleanupExistingReplicationSlots(databaseID string) error {
	rm.safeLog("info", "Cleaning up existing replication slots for database %s", databaseID)

	client, err := rm.dm.GetDatabaseClient(databaseID)
	if err != nil {
		return fmt.Errorf("failed to get database client: %w", err)
	}

	if client.DatabaseType != "postgres" {
		return fmt.Errorf("replication slot cleanup is only supported for PostgreSQL databases")
	}

	// Type assertion to convert interface{} to *pgxpool.Pool
	pool, ok := client.DB.(*pgxpool.Pool)
	if !ok {
		return fmt.Errorf("invalid database connection type for PostgreSQL")
	}

	err = postgres.CleanupExistingReplicationSlots(pool, databaseID, rm.logger)
	if err != nil {
		rm.safeLog("error", "Failed to clean up existing replication slots: %v", err)
		return fmt.Errorf("failed to clean up existing replication slots: %w", err)
	}

	rm.safeLog("info", "Successfully cleaned up existing replication slots for database %s", databaseID)
	return nil
}

// CheckReplicationSlotActive checks if a replication slot is currently active
func (rm *ReplicationManager) CheckReplicationSlotActive(databaseID string, slotName string) (bool, error) {
	rm.safeLog("info", "Checking if replication slot %s is active for database %s", slotName, databaseID)

	client, err := rm.dm.GetDatabaseClient(databaseID)
	if err != nil {
		return false, fmt.Errorf("failed to get database client: %w", err)
	}

	if client.DatabaseType != "postgres" {
		return false, fmt.Errorf("replication slot checking is only supported for PostgreSQL databases")
	}

	// Type assertion to convert interface{} to *pgxpool.Pool
	pool, ok := client.DB.(*pgxpool.Pool)
	if !ok {
		return false, fmt.Errorf("invalid database connection type for PostgreSQL")
	}

	isActive, err := postgres.IsReplicationSlotActive(pool, slotName)
	if err != nil {
		rm.safeLog("error", "Failed to check replication slot status: %v", err)
		return false, fmt.Errorf("failed to check replication slot status: %w", err)
	}

	rm.safeLog("info", "Replication slot %s is active: %v", slotName, isActive)
	return isActive, nil
}
