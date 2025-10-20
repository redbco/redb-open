package database

import (
	"fmt"

	"github.com/redbco/redb-open/pkg/logger"
)

// LogLevel represents the severity of a log message
type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

// DatabaseLogContext provides structured context for database logging
type DatabaseLogContext struct {
	DatabaseType  string
	DatabaseID    string
	InstanceID    string
	ReplicationID string
	TenantID      string
	Host          string
	Port          int
	Operation     string
	IsInternal    bool // true for internal PostgreSQL, false for client databases
}

// DatabaseLogger provides unified logging for all database operations
type DatabaseLogger struct {
	logger *logger.Logger
}

// NewDatabaseLogger creates a new database logger
func NewDatabaseLogger(logger *logger.Logger) *DatabaseLogger {
	return &DatabaseLogger{
		logger: logger,
	}
}

// LogConnectionAttempt logs when a connection attempt is starting
func (dl *DatabaseLogger) LogConnectionAttempt(ctx DatabaseLogContext) {
	if dl.logger == nil {
		return
	}

	message := dl.formatConnectionMessage("Attempting connection", ctx)
	dl.logger.Info("%s", message)
}

// LogConnectionSuccess logs successful database connections
func (dl *DatabaseLogger) LogConnectionSuccess(ctx DatabaseLogContext) {
	if dl.logger == nil {
		return
	}

	message := dl.formatConnectionMessage("Connection established", ctx)
	dl.logger.Info("%s", message)
}

// LogConnectionFailure logs connection failures with appropriate severity
func (dl *DatabaseLogger) LogConnectionFailure(ctx DatabaseLogContext, err error) {
	if dl.logger == nil {
		return
	}

	message := dl.formatConnectionMessage("Connection failed", ctx)
	errorDetails := fmt.Sprintf("%s: %v", message, err)

	// Internal database failures are errors, client database failures are warnings
	if ctx.IsInternal {
		dl.logger.Error("%s", errorDetails)
	} else {
		dl.logger.Warn("%s", errorDetails)
	}
}

// LogDisconnectionAttempt logs when disconnection is starting
func (dl *DatabaseLogger) LogDisconnectionAttempt(ctx DatabaseLogContext) {
	if dl.logger == nil {
		return
	}

	message := dl.formatConnectionMessage("Attempting disconnection", ctx)
	dl.logger.Info("%s", message)
}

// LogDisconnectionSuccess logs successful disconnections
func (dl *DatabaseLogger) LogDisconnectionSuccess(ctx DatabaseLogContext) {
	if dl.logger == nil {
		return
	}

	message := dl.formatConnectionMessage("Disconnection completed", ctx)
	dl.logger.Info("%s", message)
}

// LogDisconnectionFailure logs disconnection failures
func (dl *DatabaseLogger) LogDisconnectionFailure(ctx DatabaseLogContext, err error) {
	if dl.logger == nil {
		return
	}

	message := dl.formatConnectionMessage("Disconnection failed", ctx)
	errorDetails := fmt.Sprintf("%s: %v", message, err)

	// All disconnection failures are warnings since service continues
	dl.logger.Warn("%s", errorDetails)
}

// LogOperationAttempt logs when a database operation is starting
func (dl *DatabaseLogger) LogOperationAttempt(ctx DatabaseLogContext) {
	if dl.logger == nil {
		return
	}

	message := dl.formatOperationMessage("Operation started", ctx)
	dl.logger.Debug("%s", message)
}

// LogOperationSuccess logs successful database operations
func (dl *DatabaseLogger) LogOperationSuccess(ctx DatabaseLogContext) {
	if dl.logger == nil {
		return
	}

	message := dl.formatOperationMessage("Operation completed", ctx)
	dl.logger.Debug("%s", message)
}

// LogOperationFailure logs operation failures
func (dl *DatabaseLogger) LogOperationFailure(ctx DatabaseLogContext, err error) {
	if dl.logger == nil {
		return
	}

	message := dl.formatOperationMessage("Operation failed", ctx)
	errorDetails := fmt.Sprintf("%s: %v", message, err)

	// Internal database operation failures are errors, client database failures are warnings
	if ctx.IsInternal {
		dl.logger.Error("%s", errorDetails)
	} else {
		dl.logger.Warn("%s", errorDetails)
	}
}

// LogHealthCheck logs database health check results
func (dl *DatabaseLogger) LogHealthCheck(ctx DatabaseLogContext, isHealthy bool, err error) {
	if dl.logger == nil {
		return
	}

	if isHealthy {
		message := dl.formatConnectionMessage("Health check passed", ctx)
		dl.logger.Debug("%s", message)
	} else {
		message := dl.formatConnectionMessage("Health check failed", ctx)
		errorDetails := fmt.Sprintf("%s: %v", message, err)

		// Internal database health failures are errors, client database failures are warnings
		if ctx.IsInternal {
			dl.logger.Error("%s", errorDetails)
		} else {
			dl.logger.Warn("%s", errorDetails)
		}
	}
}

// LogReplicationEvent logs replication-specific events
func (dl *DatabaseLogger) LogReplicationEvent(ctx DatabaseLogContext, event string, details interface{}) {
	if dl.logger == nil {
		return
	}

	message := dl.formatReplicationMessage(event, ctx)
	if details != nil {
		message = fmt.Sprintf("%s: %v", message, details)
	}

	dl.logger.Info("%s", message)
}

// LogReplicationError logs replication errors
func (dl *DatabaseLogger) LogReplicationError(ctx DatabaseLogContext, err error) {
	if dl.logger == nil {
		return
	}

	message := dl.formatReplicationMessage("Replication error", ctx)
	errorDetails := fmt.Sprintf("%s: %v", message, err)

	// Replication errors are warnings since they don't stop the service
	dl.logger.Warn("%s", errorDetails)
}

// Helper methods for formatting log messages

func (dl *DatabaseLogger) formatConnectionMessage(action string, ctx DatabaseLogContext) string {
	var connectionType string
	if ctx.IsInternal {
		connectionType = "internal"
	} else {
		connectionType = "client"
	}

	base := fmt.Sprintf("[%s:%s] %s", connectionType, ctx.DatabaseType, action)

	// Add specific identifiers based on what's available
	if ctx.DatabaseID != "" {
		base = fmt.Sprintf("%s database_id=%s", base, ctx.DatabaseID)
	}
	if ctx.InstanceID != "" {
		base = fmt.Sprintf("%s instance_id=%s", base, ctx.InstanceID)
	}
	if ctx.TenantID != "" {
		base = fmt.Sprintf("%s tenant_id=%s", base, ctx.TenantID)
	}

	// Add connection details
	if ctx.Host != "" {
		if ctx.Port > 0 {
			base = fmt.Sprintf("%s host=%s:%d", base, ctx.Host, ctx.Port)
		} else {
			base = fmt.Sprintf("%s host=%s", base, ctx.Host)
		}
	}

	return base
}

func (dl *DatabaseLogger) formatOperationMessage(action string, ctx DatabaseLogContext) string {
	var connectionType string
	if ctx.IsInternal {
		connectionType = "internal"
	} else {
		connectionType = "client"
	}

	base := fmt.Sprintf("[%s:%s] %s", connectionType, ctx.DatabaseType, action)

	// Add operation details
	if ctx.Operation != "" {
		base = fmt.Sprintf("%s operation=%s", base, ctx.Operation)
	}

	// Add identifiers
	if ctx.DatabaseID != "" {
		base = fmt.Sprintf("%s database_id=%s", base, ctx.DatabaseID)
	}
	if ctx.InstanceID != "" {
		base = fmt.Sprintf("%s instance_id=%s", base, ctx.InstanceID)
	}

	return base
}

func (dl *DatabaseLogger) formatReplicationMessage(event string, ctx DatabaseLogContext) string {
	base := fmt.Sprintf("[replication:%s] %s", ctx.DatabaseType, event)

	if ctx.ReplicationID != "" {
		base = fmt.Sprintf("%s replication_id=%s", base, ctx.ReplicationID)
	}
	if ctx.DatabaseID != "" {
		base = fmt.Sprintf("%s database_id=%s", base, ctx.DatabaseID)
	}

	return base
}

// Convenience methods for common logging patterns

// LogClientConnectionAttempt logs connection attempts for client databases
func (dl *DatabaseLogger) LogClientConnectionAttempt(databaseType, databaseID, host string, port int) {
	dl.LogConnectionAttempt(DatabaseLogContext{
		DatabaseType: databaseType,
		DatabaseID:   databaseID,
		Host:         host,
		Port:         port,
		IsInternal:   false,
	})
}

// LogClientConnectionFailure logs connection failures for client databases (as warnings)
func (dl *DatabaseLogger) LogClientConnectionFailure(databaseType, databaseID, host string, port int, err error) {
	dl.LogConnectionFailure(DatabaseLogContext{
		DatabaseType: databaseType,
		DatabaseID:   databaseID,
		Host:         host,
		Port:         port,
		IsInternal:   false,
	}, err)
}

// LogClientConnectionSuccess logs successful connections for client databases
func (dl *DatabaseLogger) LogClientConnectionSuccess(databaseType, databaseID, host string, port int) {
	dl.LogConnectionSuccess(DatabaseLogContext{
		DatabaseType: databaseType,
		DatabaseID:   databaseID,
		Host:         host,
		Port:         port,
		IsInternal:   false,
	})
}

// LogInternalConnectionAttempt logs connection attempts for internal PostgreSQL database
func (dl *DatabaseLogger) LogInternalConnectionAttempt(host string, port int) {
	dl.LogConnectionAttempt(DatabaseLogContext{
		DatabaseType: "postgres",
		DatabaseID:   "internal",
		Host:         host,
		Port:         port,
		IsInternal:   true,
	})
}

// LogInternalConnectionFailure logs connection failures for internal PostgreSQL database (as errors)
func (dl *DatabaseLogger) LogInternalConnectionFailure(host string, port int, err error) {
	dl.LogConnectionFailure(DatabaseLogContext{
		DatabaseType: "postgres",
		DatabaseID:   "internal",
		Host:         host,
		Port:         port,
		IsInternal:   true,
	}, err)
}

// LogInternalConnectionSuccess logs successful connections for internal PostgreSQL database
func (dl *DatabaseLogger) LogInternalConnectionSuccess(host string, port int) {
	dl.LogConnectionSuccess(DatabaseLogContext{
		DatabaseType: "postgres",
		DatabaseID:   "internal",
		Host:         host,
		Port:         port,
		IsInternal:   true,
	})
}
