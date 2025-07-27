package postgres

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// ConnectReplication creates a new replication client and connection for PostgreSQL
func ConnectReplication(config common.ReplicationConfig) (*common.ReplicationClient, common.ReplicationSourceInterface, error) {
	// Validate configuration
	if config.DatabaseID == "" {
		return nil, nil, fmt.Errorf("database ID is required")
	}

	if len(config.TableNames) == 0 {
		return nil, nil, fmt.Errorf("at least one table name is required for replication")
	}

	// Create connection string for replication
	connString := fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=%s",
		config.Username, config.Password, config.Host, config.Port, config.DatabaseName, config.SSLMode)

	// Parse connection config
	pgConfig, err := pgconn.ParseConfig(connString)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing connection string: %w", err)
	}

	// Set replication mode
	pgConfig.RuntimeParams["replication"] = "database"

	// Create replication connection
	replicationConn, err := pgconn.ConnectConfig(context.Background(), pgConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating replication connection: %w", err)
	}

	// Create PostgreSQL-specific replication source details
	tableSet := make(map[string]struct{})
	for _, t := range config.TableNames {
		tableSet[t] = struct{}{}
	}
	sourceDetails := &PostgresReplicationSourceDetails{
		SlotName:        config.SlotName,
		PublicationName: config.PublicationName,
		DatabaseID:      config.DatabaseID,
		ReplicationConn: replicationConn,
		StopChan:        make(chan struct{}),
		isActive:        false,
		EventHandler:    config.EventHandler,
		TableNames:      tableSet,
	}

	// If slot name or publication name are not provided, generate them
	if sourceDetails.SlotName == "" {
		sourceDetails.SlotName = fmt.Sprintf("slot_%s_%s",
			sanitizeIdentifier(config.DatabaseID),
			sanitizeIdentifier(common.GenerateUniqueID()))
	}

	if sourceDetails.PublicationName == "" {
		sourceDetails.PublicationName = fmt.Sprintf("pub_%s_%s",
			sanitizeIdentifier(config.DatabaseID),
			sanitizeIdentifier(common.GenerateUniqueID()))
	}

	// TODO: Publication/slot management for multi-table (see CreateReplicationSource for details)
	// This function should ensure the publication includes all tables in tableSet, and slot is created if needed.
	// For now, this is a placeholder. Actual logic will be in CreateReplicationSource.

	client := &common.ReplicationClient{
		ReplicationID:     config.ReplicationID,
		DatabaseID:        config.DatabaseID,
		DatabaseType:      "postgres",
		Config:            config,
		Connection:        replicationConn,
		ReplicationSource: sourceDetails,
		EventHandler:      config.EventHandler,
		IsConnected:       1, // Mark as connected
		Status:            "connected",
		StatusMessage:     "Replication connection established",
		CreatedAt:         time.Now(),
		ConnectedAt:       &[]time.Time{time.Now()}[0],
		LastActivity:      time.Now(),
	}

	return client, sourceDetails, nil
}

// CreateReplicationSourceWithClient creates a replication source using an existing database client
func CreateReplicationSourceWithClient(pool *pgxpool.Pool, config common.ReplicationConfig) (common.ReplicationSourceInterface, error) {
	// Use the first table name for now
	if len(config.TableNames) == 0 {
		return nil, fmt.Errorf("at least one table name is required")
	}
	tableName := config.TableNames[0]

	// Create replication source using existing function
	details, err := CreateReplicationSource(pool, []string{tableName}, config.DatabaseID, config.DatabaseName, config.EventHandler, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create replication source: %w", err)
	}

	// Set the event handler
	details.EventHandler = config.EventHandler

	return details, nil
}

// sanitizeIdentifier converts a string to a valid PostgreSQL identifier
// PostgreSQL identifiers must start with a letter and contain only lowercase letters, numbers, and underscores
func sanitizeIdentifier(input string) string {
	// Remove any non-alphanumeric characters except underscores
	reg := regexp.MustCompile(`[^a-zA-Z0-9_]`)
	sanitized := reg.ReplaceAllString(input, "_")

	// Convert to lowercase
	sanitized = strings.ToLower(sanitized)

	// Ensure it starts with a letter (not a number or underscore)
	if len(sanitized) > 0 {
		if sanitized[0] >= '0' && sanitized[0] <= '9' {
			sanitized = "id_" + sanitized
		} else if sanitized[0] == '_' {
			sanitized = "id" + sanitized
		}
	}

	// Limit length to avoid overly long names (PostgreSQL has limits)
	if len(sanitized) > 50 {
		sanitized = sanitized[:50]
	}

	return sanitized
}

// CheckLogicalReplicationPrerequisites checks if PostgreSQL is configured for logical replication
func CheckLogicalReplicationPrerequisites(pool *pgxpool.Pool, logger *logger.Logger) error {
	// Check wal_level
	var walLevel string
	err := pool.QueryRow(context.Background(), "SHOW wal_level").Scan(&walLevel)
	if err != nil {
		return fmt.Errorf("error checking wal_level: %v", err)
	}

	// wal_level must be 'logical' or 'replica' for logical replication
	if walLevel != "logical" && walLevel != "replica" {
		return fmt.Errorf("wal_level is set to '%s' but must be 'logical' or 'replica' for logical replication. Please set wal_level = logical in postgresql.conf and restart the server", walLevel)
	}

	// Check if logical replication is enabled
	var maxReplicationSlotsStr string
	err = pool.QueryRow(context.Background(), "SHOW max_replication_slots").Scan(&maxReplicationSlotsStr)
	if err != nil {
		return fmt.Errorf("error checking max_replication_slots: %v", err)
	}

	maxReplicationSlots, err := strconv.Atoi(maxReplicationSlotsStr)
	if err != nil {
		return fmt.Errorf("error parsing max_replication_slots value '%s': %v", maxReplicationSlotsStr, err)
	}

	if maxReplicationSlots <= 0 {
		return fmt.Errorf("max_replication_slots is set to %d but must be greater than 0 for logical replication", maxReplicationSlots)
	}

	// Check if wal_keep_size is sufficient (optional but recommended)
	var walKeepSize string
	err = pool.QueryRow(context.Background(), "SHOW wal_keep_size").Scan(&walKeepSize)
	if err != nil {
		// This parameter might not exist in older PostgreSQL versions, so we'll just log a warning
		if logger != nil {
			logger.Warnf("Warning: Could not check wal_keep_size: %v", err)
		}
	} else {
		if logger != nil {
			logger.Infof("wal_keep_size is set to: %s", walKeepSize)
		}
	}

	return nil
}

// GetPostgreSQLReplicationConfig returns current PostgreSQL replication configuration
func GetPostgreSQLReplicationConfig(pool *pgxpool.Pool) (map[string]interface{}, error) {
	config := make(map[string]interface{})

	// Check various replication-related parameters
	params := []string{
		"wal_level",
		"max_replication_slots",
		"max_wal_senders",
		"wal_keep_size",
		"max_logical_replication_workers",
		"max_worker_processes",
	}

	for _, param := range params {
		var value string
		err := pool.QueryRow(context.Background(), fmt.Sprintf("SHOW %s", param)).Scan(&value)
		if err != nil {
			// Some parameters might not exist in older PostgreSQL versions
			config[param] = fmt.Sprintf("Error: %v", err)
		} else {
			// For numeric parameters, try to convert to int for better handling
			if param == "max_replication_slots" || param == "max_wal_senders" ||
				param == "max_logical_replication_workers" || param == "max_worker_processes" {
				if intVal, err := strconv.Atoi(value); err == nil {
					config[param] = intVal
				} else {
					config[param] = value
				}
			} else {
				config[param] = value
			}
		}
	}

	return config, nil
}

// CheckReplicationSlotStatus checks if a replication slot is active and properly configured
func CheckReplicationSlotStatus(pool *pgxpool.Pool, slotName string, logger *logger.Logger) (map[string]interface{}, error) {
	query := `
		SELECT 
			slot_name,
			plugin,
			slot_type,
			database,
			active,
			active_pid,
			restart_lsn,
			confirmed_flush_lsn,
			pg_wal_lsn_diff(restart_lsn, confirmed_flush_lsn) as lag_bytes
		FROM pg_replication_slots 
		WHERE slot_name = $1
	`

	row := pool.QueryRow(context.Background(), query, slotName)

	var (
		slotNameResult    string
		plugin            string
		slotType          string
		database          string
		active            bool
		activePid         *int
		restartLsn        *string
		confirmedFlushLsn *string
		lagBytes          *int64
	)

	err := row.Scan(&slotNameResult, &plugin, &slotType, &database, &active, &activePid, &restartLsn, &confirmedFlushLsn, &lagBytes)
	if err != nil {
		return nil, fmt.Errorf("error checking replication slot status: %v", err)
	}

	status := map[string]interface{}{
		"slot_name":           slotNameResult,
		"plugin":              plugin,
		"slot_type":           slotType,
		"database":            database,
		"active":              active,
		"active_pid":          activePid,
		"restart_lsn":         restartLsn,
		"confirmed_flush_lsn": confirmedFlushLsn,
		"lag_bytes":           lagBytes,
	}

	if logger != nil {
		logger.Infof("Replication slot status for %s: active=%v, plugin=%s, lag_bytes=%v", slotName, active, plugin, lagBytes)
	}

	return status, nil
}

// IsReplicationSlotActive checks if a replication slot is currently active
func IsReplicationSlotActive(pool *pgxpool.Pool, slotName string) (bool, error) {
	var active bool
	err := pool.QueryRow(context.Background(), "SELECT active FROM pg_replication_slots WHERE slot_name = $1", slotName).Scan(&active)
	if err != nil {
		return false, fmt.Errorf("error checking if replication slot is active: %v", err)
	}
	return active, nil
}

// DropActiveReplicationSlot drops a replication slot even if it's active
func DropActiveReplicationSlot(pool *pgxpool.Pool, slotName string, logger *logger.Logger) error {
	// First check if the slot exists and is active
	var exists bool
	var active bool
	err := pool.QueryRow(context.Background(), "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1), active FROM pg_replication_slots WHERE slot_name = $1", slotName, slotName).Scan(&exists, &active)
	if err != nil {
		return fmt.Errorf("error checking replication slot: %v", err)
	}

	if !exists {
		if logger != nil {
			logger.Infof("Replication slot %s does not exist", slotName)
		}
		return nil
	}

	if active {
		if logger != nil {
			logger.Warnf("Replication slot %s is active, terminating the process first", slotName)
		}
		// Terminate the process using the slot
		if err := TerminateReplicationSlotProcess(pool, slotName, logger); err != nil {
			if logger != nil {
				logger.Errorf("Failed to terminate process for slot %s: %v", slotName, err)
			}
			// Continue anyway, the slot might still be droppable
		}
	}

	// Drop the replication slot
	_, err = pool.Exec(context.Background(), fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", slotName))
	if err != nil {
		if logger != nil {
			logger.Errorf("Failed to drop replication slot %s: %v", slotName, err)
		}
		return fmt.Errorf("error dropping replication slot: %v", err)
	}

	if logger != nil {
		logger.Infof("Successfully dropped replication slot %s", slotName)
	}

	return nil
}

// CreateReplicationSource sets up a replication source for multiple tables
func CreateReplicationSource(pool *pgxpool.Pool, tableNames []string, databaseID string, databaseName string, eventHandler func(map[string]interface{}), logger *logger.Logger) (*PostgresReplicationSourceDetails, error) {
	var err error

	if len(tableNames) == 0 {
		return nil, fmt.Errorf("at least one table name is required")
	}

	// Check prerequisites before attempting to create replication
	if err = CheckLogicalReplicationPrerequisites(pool, logger); err != nil {
		return nil, fmt.Errorf("logical replication prerequisites not met: %v", err)
	}

	// Check if all tables can be replicated
	for _, tableName := range tableNames {
		tableReplicationStatus, err := CheckTableReplicationStatus(pool, tableName, logger)
		if err != nil {
			return nil, fmt.Errorf("error checking table replication status for %s: %v", tableName, err)
		}
		if canReplicate, ok := tableReplicationStatus["can_replicate"].(bool); !ok || !canReplicate {
			return nil, fmt.Errorf("table %s cannot be replicated (no primary key)", tableName)
		}
	}

	// Clean up any existing replication slots for this database to avoid conflicts
	if err := CleanupExistingReplicationSlots(pool, databaseID, logger); err != nil {
		if logger != nil {
			logger.Warnf("Warning: Could not clean up existing replication slots: %v", err)
		}
	}

	// Generate unique names for slot and publication
	slotName := fmt.Sprintf("slot_%s_%s", sanitizeIdentifier(databaseID), sanitizeIdentifier(common.GenerateUniqueID()))
	pubName := fmt.Sprintf("pub_%s_%s", sanitizeIdentifier(databaseID), sanitizeIdentifier(common.GenerateUniqueID()))

	// Check if a slot with this name already exists and is active
	slotExists, err := IsReplicationSlotActive(pool, slotName)
	if err == nil && slotExists {
		if logger != nil {
			logger.Warnf("Replication slot %s already exists and is active, dropping it first", slotName)
		}
		if err := DropActiveReplicationSlot(pool, slotName, logger); err != nil {
			if logger != nil {
				logger.Errorf("Failed to drop existing replication slot %s: %v", slotName, err)
			}
			slotName = fmt.Sprintf("slot_%s_%s", sanitizeIdentifier(databaseID), sanitizeIdentifier(common.GenerateUniqueID()))
			if logger != nil {
				logger.Infof("Generated new slot name: %s", slotName)
			}
		}
	}

	// Create publication for all tables
	tableList := strings.Join(tableNames, ", ")
	_, err = pool.Exec(context.Background(), fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pubName, tableList))
	if err != nil {
		return nil, fmt.Errorf("error creating publication: %v", err)
	}

	if logger != nil {
		logger.Infof("Created publication %s for tables: %s", pubName, tableList)
	}

	// Check publication status to ensure it's properly configured
	pubStatus, err := CheckPublicationStatus(pool, pubName, logger)
	if err != nil {
		if logger != nil {
			logger.Warnf("Warning: Could not check publication status: %v", err)
		}
	} else {
		if logger != nil {
			logger.Infof("Publication created successfully: %v", pubStatus)
		}
	}

	// Create replication slot
	_, err = pool.Exec(context.Background(), fmt.Sprintf("SELECT pg_create_logical_replication_slot('%s', 'pgoutput')", slotName))
	if err != nil {
		return nil, fmt.Errorf("error creating replication slot: %v", err)
	}

	if logger != nil {
		logger.Infof("Created replication slot %s with pgoutput plugin", slotName)
	}

	// Check replication slot status to ensure it's properly configured
	slotStatus, err := CheckReplicationSlotStatus(pool, slotName, logger)
	if err != nil {
		if logger != nil {
			logger.Warnf("Warning: Could not check replication slot status: %v", err)
		}
	} else {
		if logger != nil {
			logger.Infof("Replication slot created successfully: %v", slotStatus)
		}
	}

	tableSet := make(map[string]struct{})
	for _, t := range tableNames {
		tableSet[t] = struct{}{}
	}
	details := &PostgresReplicationSourceDetails{
		SlotName:        slotName,
		PublicationName: pubName,
		DatabaseID:      databaseID,
		StopChan:        make(chan struct{}),
		TableNames:      tableSet,
	}

	// Create the replication connection
	connString := getConnectionStringFromPool(pool, databaseName)
	if connString == "" {
		return nil, fmt.Errorf("could not get connection string from pool")
	}

	replicationConn, err := createReplicationConnection(connString, slotName, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create replication connection: %v", err)
	}

	details.ReplicationConn = replicationConn

	if logger != nil {
		logger.Infof("Created replication connection for slot: %s", slotName)
	}

	// Start listening for replication events
	go streamReplicationEvents(details.ReplicationConn, details, eventHandler, logger)

	return details, nil
}

// GetReplicationStatus gets the replication status for a database
func GetReplicationStatus(pool *pgxpool.Pool, databaseID string) (map[string]interface{}, error) {
	// Query replication slots status
	rows, err := pool.Query(context.Background(), `
		SELECT 
			slot_name,
			plugin,
			slot_type,
			database,
			active,
			active_pid,
			restart_lsn,
			confirmed_flush_lsn
		FROM pg_replication_slots 
		WHERE database = $1
	`, databaseID)
	if err != nil {
		return nil, fmt.Errorf("error querying replication slots: %v", err)
	}
	defer rows.Close()

	var slots []map[string]interface{}
	for rows.Next() {
		var (
			slotName          string
			plugin            string
			slotType          string
			database          string
			active            bool
			activePid         *int
			restartLsn        *string
			confirmedFlushLsn *string
		)

		if err := rows.Scan(&slotName, &plugin, &slotType, &database, &active, &activePid, &restartLsn, &confirmedFlushLsn); err != nil {
			return nil, fmt.Errorf("error scanning replication slot: %v", err)
		}

		slot := map[string]interface{}{
			"slot_name":           slotName,
			"plugin":              plugin,
			"slot_type":           slotType,
			"database":            database,
			"active":              active,
			"active_pid":          activePid,
			"restart_lsn":         restartLsn,
			"confirmed_flush_lsn": confirmedFlushLsn,
		}
		slots = append(slots, slot)
	}

	status := map[string]interface{}{
		"database_id": databaseID,
		"slots":       slots,
		"total_slots": len(slots),
		"status":      "active",
	}

	return status, nil
}

// ListReplicationSlots lists all replication slots for a database
func ListReplicationSlots(pool *pgxpool.Pool, databaseID string) ([]map[string]interface{}, error) {
	rows, err := pool.Query(context.Background(), `
		SELECT 
			slot_name,
			plugin,
			slot_type,
			database,
			active,
			active_pid,
			restart_lsn,
			confirmed_flush_lsn
		FROM pg_replication_slots 
		WHERE database = $1
	`, databaseID)
	if err != nil {
		return nil, fmt.Errorf("error querying replication slots: %v", err)
	}
	defer rows.Close()

	var slots []map[string]interface{}
	for rows.Next() {
		var (
			slotName          string
			plugin            string
			slotType          string
			database          string
			active            bool
			activePid         *int
			restartLsn        *string
			confirmedFlushLsn *string
		)

		if err := rows.Scan(&slotName, &plugin, &slotType, &database, &active, &activePid, &restartLsn, &confirmedFlushLsn); err != nil {
			return nil, fmt.Errorf("error scanning replication slot: %v", err)
		}

		slot := map[string]interface{}{
			"slot_name":           slotName,
			"plugin":              plugin,
			"slot_type":           slotType,
			"database":            database,
			"active":              active,
			"active_pid":          activePid,
			"restart_lsn":         restartLsn,
			"confirmed_flush_lsn": confirmedFlushLsn,
		}
		slots = append(slots, slot)
	}

	return slots, nil
}

// DropReplicationSlot drops a replication slot
func DropReplicationSlot(pool *pgxpool.Pool, slotName string) error {
	// Check if slot exists first
	var exists bool
	err := pool.QueryRow(context.Background(), "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", slotName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("error checking if replication slot exists: %v", err)
	}

	if !exists {
		return fmt.Errorf("replication slot %s does not exist", slotName)
	}

	// Drop the replication slot
	_, err = pool.Exec(context.Background(), fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", slotName))
	if err != nil {
		return fmt.Errorf("error dropping replication slot: %v", err)
	}

	return nil
}

// ListPublications lists all publications for a database
func ListPublications(pool *pgxpool.Pool) ([]map[string]interface{}, error) {
	rows, err := pool.Query(context.Background(), `
		SELECT 
			pubname,
			pubowner::regrole::text as pubowner,
			puballtables,
			pubinsert,
			pubupdate,
			pubdelete,
			pubtruncate
		FROM pg_publication
	`)
	if err != nil {
		return nil, fmt.Errorf("error querying publications: %v", err)
	}
	defer rows.Close()

	var publications []map[string]interface{}
	for rows.Next() {
		var (
			pubname      string
			pubowner     string
			puballtables bool
			pubinsert    bool
			pubupdate    bool
			pubdelete    bool
			pubtruncate  bool
		)

		if err := rows.Scan(&pubname, &pubowner, &puballtables, &pubinsert, &pubupdate, &pubdelete, &pubtruncate); err != nil {
			return nil, fmt.Errorf("error scanning publication: %v", err)
		}

		publication := map[string]interface{}{
			"pubname":      pubname,
			"pubowner":     pubowner,
			"puballtables": puballtables,
			"pubinsert":    pubinsert,
			"pubupdate":    pubupdate,
			"pubdelete":    pubdelete,
			"pubtruncate":  pubtruncate,
		}
		publications = append(publications, publication)
	}

	return publications, nil
}

// DropPublication drops a publication
func DropPublication(pool *pgxpool.Pool, publicationName string) error {
	// Check if publication exists first
	var exists bool
	err := pool.QueryRow(context.Background(), "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)", publicationName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("error checking if publication exists: %v", err)
	}

	if !exists {
		return fmt.Errorf("publication %s does not exist", publicationName)
	}

	// Drop the publication
	_, err = pool.Exec(context.Background(), fmt.Sprintf("DROP PUBLICATION %s", publicationName))
	if err != nil {
		return fmt.Errorf("error dropping publication: %v", err)
	}

	return nil
}

// GetReplicationLag gets the replication lag for a database
func GetReplicationLag(pool *pgxpool.Pool, databaseID string) (map[string]interface{}, error) {
	// Query replication lag information
	rows, err := pool.Query(context.Background(), `
		SELECT 
			slot_name,
			restart_lsn,
			confirmed_flush_lsn,
			pg_wal_lsn_diff(restart_lsn, confirmed_flush_lsn) as lag_bytes
		FROM pg_replication_slots 
		WHERE database = $1 AND active = true
	`, databaseID)
	if err != nil {
		return nil, fmt.Errorf("error querying replication lag: %v", err)
	}
	defer rows.Close()

	var totalLagBytes int64
	var activeSlots int
	var slotDetails []map[string]interface{}

	for rows.Next() {
		var (
			slotName          string
			restartLsn        *string
			confirmedFlushLsn *string
			lagBytes          *int64
		)

		if err := rows.Scan(&slotName, &restartLsn, &confirmedFlushLsn, &lagBytes); err != nil {
			return nil, fmt.Errorf("error scanning replication lag: %v", err)
		}

		activeSlots++
		if lagBytes != nil {
			totalLagBytes += *lagBytes
		}

		slotDetail := map[string]interface{}{
			"slot_name":           slotName,
			"restart_lsn":         restartLsn,
			"confirmed_flush_lsn": confirmedFlushLsn,
			"lag_bytes":           lagBytes,
		}
		slotDetails = append(slotDetails, slotDetail)
	}

	// Calculate lag time (approximate - 16MB per second is a rough estimate)
	var lagTime string
	if totalLagBytes > 0 {
		lagSeconds := totalLagBytes / (16 * 1024 * 1024) // 16MB per second
		lagTime = fmt.Sprintf("%d seconds", lagSeconds)
	} else {
		lagTime = "0 seconds"
	}

	lag := map[string]interface{}{
		"database_id":  databaseID,
		"lag_bytes":    totalLagBytes,
		"lag_time":     lagTime,
		"active_slots": activeSlots,
		"slot_details": slotDetails,
		"status":       "active",
	}

	return lag, nil
}

// CheckPublicationStatus checks if a publication is properly configured
func CheckPublicationStatus(pool *pgxpool.Pool, publicationName string, logger *logger.Logger) (map[string]interface{}, error) {
	query := `
		SELECT 
			pubname,
			pubowner::regrole::text as pubowner,
			puballtables,
			pubinsert,
			pubupdate,
			pubdelete,
			pubtruncate
		FROM pg_publication 
		WHERE pubname = $1
	`

	row := pool.QueryRow(context.Background(), query, publicationName)

	var (
		pubname      string
		pubowner     string
		puballtables bool
		pubinsert    bool
		pubupdate    bool
		pubdelete    bool
		pubtruncate  bool
	)

	err := row.Scan(&pubname, &pubowner, &puballtables, &pubinsert, &pubupdate, &pubdelete, &pubtruncate)
	if err != nil {
		return nil, fmt.Errorf("error checking publication status: %v", err)
	}

	status := map[string]interface{}{
		"pubname":      pubname,
		"pubowner":     pubowner,
		"puballtables": puballtables,
		"pubinsert":    pubinsert,
		"pubupdate":    pubupdate,
		"pubdelete":    pubdelete,
		"pubtruncate":  pubtruncate,
	}

	if logger != nil {
		logger.Infof("Publication status for %s: insert=%v, update=%v, delete=%v, truncate=%v",
			publicationName, pubinsert, pubupdate, pubdelete, pubtruncate)
	}

	return status, nil
}

// createReplicationConnection creates a dedicated replication connection
func createReplicationConnection(connString string, slotName string, logger *logger.Logger) (*pgconn.PgConn, error) {
	// Create a replication connection using pgconn
	config, err := pgconn.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("error parsing connection string: %v", err)
	}

	// Set replication mode
	config.RuntimeParams["replication"] = "database"

	if logger != nil {
		logger.Infof("Creating replication connection for slot: %s", slotName)
	}

	conn, err := pgconn.ConnectConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("error connecting for replication: %v", err)
	}

	return conn, nil
}

// startLogicalReplication starts logical replication streaming
func startLogicalReplication(conn *pgconn.PgConn, slotName string, publicationName string, logger *logger.Logger) error {
	// Check if connection is valid
	if conn == nil {
		return fmt.Errorf("cannot start logical replication: connection is nil")
	}

	// Start replication with the slot and publication
	query := fmt.Sprintf("START_REPLICATION SLOT %s LOGICAL 0/0 (proto_version '1', publication_names '%s')", slotName, publicationName)

	if logger != nil {
		logger.Infof("Starting logical replication with query: %s", query)
	}

	// Send the replication command
	_, err := conn.Exec(context.Background(), query).ReadAll()
	if err != nil {
		return fmt.Errorf("error starting replication: %v", err)
	}

	if logger != nil {
		logger.Infof("Logical replication started successfully for slot: %s", slotName)
	}

	return nil
}

// streamReplicationEvents streams replication events from the connection
func streamReplicationEvents(conn *pgconn.PgConn, details *PostgresReplicationSourceDetails, eventHandler func(map[string]interface{}), logger *logger.Logger) {
	if logger != nil {
		logger.Infof("Starting replication event stream for slot: %s, tables: %v", details.SlotName, details.TableNames)
	}

	// Check if connection is valid
	if conn == nil {
		if logger != nil {
			logger.Errorf("Cannot start replication stream: connection is nil for slot: %s", details.SlotName)
		}
		return
	}

	// Create a context that can be cancelled
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up a goroutine to handle stop signals
	go func() {
		<-details.StopChan
		if logger != nil {
			logger.Infof("Received stop signal for replication slot: %s", details.SlotName)
		}
		cancel()
	}()

	// Start logical replication
	if err := startLogicalReplication(conn, details.SlotName, details.PublicationName, logger); err != nil {
		if logger != nil {
			logger.Errorf("Failed to start logical replication for slot %s: %v", details.SlotName, err)
		}
		return
	}

	// Set up keepalive ticker
	keepaliveTicker := time.NewTicker(30 * time.Second) // Send keepalive every 30 seconds
	defer keepaliveTicker.Stop()

	// Read WAL messages from the replication stream
	for {
		select {
		case <-ctx.Done():
			if logger != nil {
				logger.Infof("Replication stream stopped for slot: %s", details.SlotName)
			}
			return
		case <-keepaliveTicker.C:
			// Send keepalive to prevent connection timeout
			if err := sendKeepaliveResponse(conn, logger); err != nil {
				if logger != nil {
					logger.Errorf("Failed to send keepalive for slot %s: %v", details.SlotName, err)
				}
			}
		default:
			// Read the next message from the replication stream with a timeout
			readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
			msg, err := conn.ReceiveMessage(readCtx)
			readCancel()

			if err != nil {
				// Check if this is a timeout (which is expected when no messages)
				if ctx.Err() != nil {
					return
				}
				// For timeouts, just continue to the next iteration
				if err.Error() == "context deadline exceeded" {
					continue
				}
				if logger != nil {
					logger.Errorf("Error receiving message from replication stream for slot %s: %v", details.SlotName, err)
				}
				// Wait a bit before retrying
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// Process the message based on its type
			switch msg := msg.(type) {
			case interface{ Data() []byte }:
				// This is WAL data (CopyData message)
				if err := processWALMessage(msg.Data(), details, eventHandler, logger); err != nil {
					if logger != nil {
						logger.Errorf("Error processing WAL message for slot %s: %v", details.SlotName, err)
					}
				}
			default:
				// Log other message types for debugging
				if logger != nil {
					logger.Debugf("Received message type for slot %s: %T", details.SlotName, msg)
				}
			}
		}
	}
}

// processWALMessage processes a WAL message and extracts replication events
func processWALMessage(walData []byte, details *PostgresReplicationSourceDetails, eventHandler func(map[string]interface{}), logger *logger.Logger) error {
	if logger != nil {
		logger.Debugf("Processing WAL message for slot %s, length: %d", details.SlotName, len(walData))
	}

	// Parse the WAL message to extract replication changes
	changes, err := parseWALMessage(walData, logger, details)
	if err != nil {
		return fmt.Errorf("error parsing WAL message: %v", err)
	}

	// Process each change
	for _, change := range changes {
		// Create event data
		event := map[string]interface{}{
			"operation":   change.Operation,
			"table_name":  change.TableName, // Use change.TableName
			"database_id": details.DatabaseID,
			"slot_name":   details.SlotName,
			"timestamp":   time.Now().UTC().Format(time.RFC3339),
			"data":        change.Data,
			"old_data":    change.OldData,
		}

		// Call the event handler
		if eventHandler != nil {
			eventHandler(event)
		}

		if logger != nil {
			logger.Infof("Processed replication event for slot %s: %s on table %s",
				details.SlotName, change.Operation, change.TableName)
		}
	}

	return nil
}

// parseWALMessage parses a WAL message to extract replication changes
func parseWALMessage(walData []byte, logger *logger.Logger, details *PostgresReplicationSourceDetails) ([]PostgresReplicationChange, error) {
	if logger != nil {
		logger.Debugf("Parsing WAL message, length: %d", len(walData))
	}

	var changes []PostgresReplicationChange

	if len(walData) == 0 {
		return changes, nil
	}

	if len(walData) < 1 {
		return changes, fmt.Errorf("WAL message too short")
	}

	messageType := walData[0]

	var operation string
	switch messageType {
	case 'I':
		operation = "INSERT"
	case 'U':
		operation = "UPDATE"
	case 'D':
		operation = "DELETE"
	case 'B':
		operation = "BEGIN"
	case 'C':
		operation = "COMMIT"
	case 'R':
		operation = "RELATION"
	default:
		operation = "UNKNOWN"
	}

	if logger != nil {
		logger.Debugf("Detected WAL message type: %c (%s)", messageType, operation)
	}

	// For now, set TableName to the only table if just one is present
	tableName := ""
	if details != nil && len(details.TableNames) == 1 {
		for t := range details.TableNames {
			tableName = t
			break
		}
	}

	change := PostgresReplicationChange{
		Operation: operation,
		TableName: tableName,
		Data: map[string]interface{}{
			"message_type": string(messageType),
			"raw_data":     string(walData),
			"data_length":  len(walData),
		},
	}

	if operation == "UPDATE" && len(walData) > 1 {
		change.OldData = map[string]interface{}{
			"message_type": string(messageType),
			"raw_data":     string(walData),
		}
		change.Data = map[string]interface{}{
			"message_type": string(messageType),
			"raw_data":     string(walData),
			"is_update":    true,
		}
	}

	changes = append(changes, change)

	if logger != nil {
		logger.Debugf("Parsed WAL message: operation=%s, data_length=%d", operation, len(walData))
	}

	return changes, nil
}

// sendKeepaliveResponse sends a keepalive response to the server
func sendKeepaliveResponse(conn *pgconn.PgConn, logger *logger.Logger) error {
	// Send a keepalive response to prevent connection timeout
	// In PostgreSQL logical replication, we need to send a StandbyStatusUpdate message

	if logger != nil {
		logger.Debugf("Sending keepalive response")
	}

	// For now, we'll use a simple approach
	// In a full implementation, you would construct the proper StandbyStatusUpdate message
	// according to PostgreSQL's replication protocol

	// Send a simple query to keep the connection alive
	_, err := conn.Exec(context.Background(), "SELECT 1").ReadAll()
	if err != nil {
		return fmt.Errorf("failed to send keepalive: %v", err)
	}

	if logger != nil {
		logger.Debugf("Keepalive response sent successfully")
	}

	return nil
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// CheckTableReplicationStatus checks if a table is being replicated
func CheckTableReplicationStatus(pool *pgxpool.Pool, tableName string, logger *logger.Logger) (map[string]interface{}, error) {
	// Check if the table exists and has a primary key (required for logical replication)
	query := `
		SELECT 
			t.table_name,
			t.table_schema,
			CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as has_primary_key,
			CASE WHEN pk.column_name IS NOT NULL THEN pk.column_name ELSE NULL END as primary_key_column
		FROM information_schema.tables t
		LEFT JOIN (
			SELECT 
				tc.table_name,
				tc.table_schema,
				tc.column_name
			FROM information_schema.table_constraints tco
			JOIN information_schema.key_column_usage tc ON tco.constraint_name = tc.constraint_name
			WHERE tco.constraint_type = 'PRIMARY KEY'
		) pk ON t.table_name = pk.table_name AND t.table_schema = pk.table_schema
		WHERE t.table_name = $1 AND t.table_schema = 'public'
	`

	row := pool.QueryRow(context.Background(), query, tableName)

	var (
		tableNameResult  string
		tableSchema      string
		hasPrimaryKey    bool
		primaryKeyColumn *string
	)

	err := row.Scan(&tableNameResult, &tableSchema, &hasPrimaryKey, &primaryKeyColumn)
	if err != nil {
		return nil, fmt.Errorf("error checking table replication status: %v", err)
	}

	status := map[string]interface{}{
		"table_name":         tableNameResult,
		"table_schema":       tableSchema,
		"has_primary_key":    hasPrimaryKey,
		"primary_key_column": primaryKeyColumn,
		"can_replicate":      hasPrimaryKey,
	}

	if logger != nil {
		if hasPrimaryKey {
			logger.Infof("Table %s can be replicated (has primary key: %s)", tableName, *primaryKeyColumn)
		} else {
			logger.Warnf("Table %s cannot be replicated (no primary key)", tableName)
		}
	}

	return status, nil
}

// getConnectionStringFromPool extracts the connection string from a pgxpool
// This is a simplified approach - in a real implementation, you'd need to store the original connection string
func getConnectionStringFromPool(pool *pgxpool.Pool, databaseName string) string {
	// For now, we'll use a placeholder approach
	// In a real implementation, you would need to store the original connection string
	// or extract it from the pool configuration

	// Use the provided database name instead of hardcoding "postgres"
	// This ensures we connect to the correct database where the replication slot was created
	return fmt.Sprintf("postgresql://postgres:postgres@localhost:5432/%s?sslmode=disable", databaseName)
}

// CleanupExistingReplicationSlots cleans up all existing replication slots for a database
func CleanupExistingReplicationSlots(pool *pgxpool.Pool, databaseID string, logger *logger.Logger) error {
	// Query for all replication slots for this database
	rows, err := pool.Query(context.Background(), `
		SELECT slot_name, active 
		FROM pg_replication_slots 
		WHERE database = $1 AND slot_name LIKE $2
	`, databaseID, fmt.Sprintf("slot_%s%%", sanitizeIdentifier(databaseID)))
	if err != nil {
		return fmt.Errorf("error querying existing replication slots: %v", err)
	}
	defer rows.Close()

	var cleanedSlots int
	for rows.Next() {
		var slotName string
		var active bool
		if err := rows.Scan(&slotName, &active); err != nil {
			if logger != nil {
				logger.Errorf("Error scanning replication slot: %v", err)
			}
			continue
		}

		if logger != nil {
			logger.Infof("Found existing replication slot: %s (active: %v)", slotName, active)
		}

		// Try to drop the slot
		if err := DropActiveReplicationSlot(pool, slotName, logger); err != nil {
			if logger != nil {
				logger.Errorf("Failed to drop replication slot %s: %v", slotName, err)
			}
		} else {
			cleanedSlots++
		}
	}

	if logger != nil {
		logger.Infof("Cleaned up %d existing replication slots for database %s", cleanedSlots, databaseID)
	}

	return nil
}

// TerminateReplicationSlotProcess terminates the process using a replication slot
func TerminateReplicationSlotProcess(pool *pgxpool.Pool, slotName string, logger *logger.Logger) error {
	// Get the PID of the process using the slot
	var activePid *int
	err := pool.QueryRow(context.Background(), "SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1", slotName).Scan(&activePid)
	if err != nil {
		return fmt.Errorf("error getting active PID for slot %s: %v", slotName, err)
	}

	if activePid == nil {
		if logger != nil {
			logger.Infof("No active process found for replication slot %s", slotName)
		}
		return nil
	}

	if logger != nil {
		logger.Warnf("Terminating process %d that is using replication slot %s", *activePid, slotName)
	}

	// Terminate the process
	_, err = pool.Exec(context.Background(), fmt.Sprintf("SELECT pg_terminate_backend(%d)", *activePid))
	if err != nil {
		return fmt.Errorf("error terminating process %d: %v", *activePid, err)
	}

	if logger != nil {
		logger.Infof("Successfully terminated process %d for replication slot %s", *activePid, slotName)
	}

	return nil
}
