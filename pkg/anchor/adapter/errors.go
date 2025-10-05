package adapter

import (
	"errors"
	"fmt"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// Standard adapter errors
var (
	// ErrOperationNotSupported is returned when an operation is not supported by the database
	ErrOperationNotSupported = errors.New("operation not supported by this database")

	// ErrConnectionClosed is returned when attempting to use a closed connection
	ErrConnectionClosed = errors.New("connection is closed")

	// ErrConnectionFailed is returned when a connection attempt fails
	ErrConnectionFailed = errors.New("connection failed")

	// ErrInvalidConfiguration is returned when the configuration is invalid
	ErrInvalidConfiguration = errors.New("invalid configuration")

	// ErrCapabilityNotFound is returned when a capability is not found
	ErrCapabilityNotFound = errors.New("capability not found")

	// ErrTableNotFound is returned when a table/collection is not found
	ErrTableNotFound = errors.New("table not found")

	// ErrDatabaseNotFound is returned when a database is not found
	ErrDatabaseNotFound = errors.New("database not found")

	// ErrAdapterNotFound is returned when an adapter is not registered
	ErrAdapterNotFound = errors.New("adapter not found")

	// ErrInvalidQuery is returned when a query is malformed
	ErrInvalidQuery = errors.New("invalid query")

	// ErrTransactionFailed is returned when a transaction fails
	ErrTransactionFailed = errors.New("transaction failed")

	// ErrAuthenticationFailed is returned when authentication fails
	ErrAuthenticationFailed = errors.New("authentication failed")

	// ErrPermissionDenied is returned when a permission is denied
	ErrPermissionDenied = errors.New("permission denied")
)

// DatabaseError wraps database-specific errors with additional context.
// This provides a consistent error structure across all database types.
type DatabaseError struct {
	DatabaseType dbcapabilities.DatabaseType
	Operation    string
	Cause        error
	Context      map[string]interface{}
}

// Error implements the error interface.
func (e *DatabaseError) Error() string {
	if len(e.Context) > 0 {
		return fmt.Sprintf("[%s] %s: %v (context: %v)", e.DatabaseType, e.Operation, e.Cause, e.Context)
	}
	return fmt.Sprintf("[%s] %s: %v", e.DatabaseType, e.Operation, e.Cause)
}

// Unwrap returns the underlying error.
func (e *DatabaseError) Unwrap() error {
	return e.Cause
}

// Is checks if the error matches the target error.
func (e *DatabaseError) Is(target error) bool {
	return errors.Is(e.Cause, target)
}

// NewDatabaseError creates a new DatabaseError.
func NewDatabaseError(dbType dbcapabilities.DatabaseType, operation string, cause error) *DatabaseError {
	return &DatabaseError{
		DatabaseType: dbType,
		Operation:    operation,
		Cause:        cause,
		Context:      make(map[string]interface{}),
	}
}

// WithContext adds context to a DatabaseError.
func (e *DatabaseError) WithContext(key string, value interface{}) *DatabaseError {
	if e.Context == nil {
		e.Context = make(map[string]interface{})
	}
	e.Context[key] = value
	return e
}

// UnsupportedOperationError is returned when an operation is not supported.
type UnsupportedOperationError struct {
	DatabaseType dbcapabilities.DatabaseType
	Operation    string
	Reason       string
}

// Error implements the error interface.
func (e *UnsupportedOperationError) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("%s does not support %s: %s", e.DatabaseType, e.Operation, e.Reason)
	}
	return fmt.Sprintf("%s does not support %s", e.DatabaseType, e.Operation)
}

// Is checks if the error is ErrOperationNotSupported.
func (e *UnsupportedOperationError) Is(target error) bool {
	return errors.Is(target, ErrOperationNotSupported)
}

// NewUnsupportedOperationError creates a new UnsupportedOperationError.
func NewUnsupportedOperationError(dbType dbcapabilities.DatabaseType, operation string, reason string) *UnsupportedOperationError {
	return &UnsupportedOperationError{
		DatabaseType: dbType,
		Operation:    operation,
		Reason:       reason,
	}
}

// ConnectionError is returned when a connection error occurs.
type ConnectionError struct {
	DatabaseType dbcapabilities.DatabaseType
	Host         string
	Port         int
	Cause        error
}

// Error implements the error interface.
func (e *ConnectionError) Error() string {
	return fmt.Sprintf("failed to connect to %s at %s:%d: %v", e.DatabaseType, e.Host, e.Port, e.Cause)
}

// Unwrap returns the underlying error.
func (e *ConnectionError) Unwrap() error {
	return e.Cause
}

// Is checks if the error is ErrConnectionFailed.
func (e *ConnectionError) Is(target error) bool {
	if errors.Is(target, ErrConnectionFailed) {
		return true
	}
	return errors.Is(e.Cause, target)
}

// NewConnectionError creates a new ConnectionError.
func NewConnectionError(dbType dbcapabilities.DatabaseType, host string, port int, cause error) *ConnectionError {
	return &ConnectionError{
		DatabaseType: dbType,
		Host:         host,
		Port:         port,
		Cause:        cause,
	}
}

// ConfigurationError is returned when a configuration error occurs.
type ConfigurationError struct {
	DatabaseType dbcapabilities.DatabaseType
	Field        string
	Reason       string
}

// Error implements the error interface.
func (e *ConfigurationError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("invalid configuration for %s: field '%s': %s", e.DatabaseType, e.Field, e.Reason)
	}
	return fmt.Sprintf("invalid configuration for %s: %s", e.DatabaseType, e.Reason)
}

// Is checks if the error is ErrInvalidConfiguration.
func (e *ConfigurationError) Is(target error) bool {
	return errors.Is(target, ErrInvalidConfiguration)
}

// NewConfigurationError creates a new ConfigurationError.
func NewConfigurationError(dbType dbcapabilities.DatabaseType, field string, reason string) *ConfigurationError {
	return &ConfigurationError{
		DatabaseType: dbType,
		Field:        field,
		Reason:       reason,
	}
}

// WrapError wraps an error with database context.
// If the error is already a DatabaseError, it returns it as-is.
func WrapError(dbType dbcapabilities.DatabaseType, operation string, err error) error {
	if err == nil {
		return nil
	}

	// Don't double-wrap
	var dbErr *DatabaseError
	if errors.As(err, &dbErr) {
		return err
	}

	return NewDatabaseError(dbType, operation, err)
}

// IsUnsupported checks if an error indicates an unsupported operation.
func IsUnsupported(err error) bool {
	return errors.Is(err, ErrOperationNotSupported)
}

// IsConnectionError checks if an error is a connection error.
func IsConnectionError(err error) bool {
	return errors.Is(err, ErrConnectionFailed)
}

// IsConfigurationError checks if an error is a configuration error.
func IsConfigurationError(err error) bool {
	return errors.Is(err, ErrInvalidConfiguration)
}

// IsAuthenticationError checks if an error is an authentication error.
func IsAuthenticationError(err error) bool {
	return errors.Is(err, ErrAuthenticationFailed)
}

// NotFoundError is returned when a resource is not found.
type NotFoundError struct {
	DatabaseType dbcapabilities.DatabaseType
	ResourceType string
	ResourceName string
}

// Error implements the error interface.
func (e *NotFoundError) Error() string {
	return fmt.Sprintf("%s not found in %s: %s", e.ResourceType, e.DatabaseType, e.ResourceName)
}

// Is checks if the error is ErrTableNotFound or ErrDatabaseNotFound.
func (e *NotFoundError) Is(target error) bool {
	if e.ResourceType == "table" || e.ResourceType == "collection" {
		return errors.Is(target, ErrTableNotFound)
	}
	if e.ResourceType == "database" {
		return errors.Is(target, ErrDatabaseNotFound)
	}
	return false
}

// NewNotFoundError creates a new NotFoundError.
func NewNotFoundError(dbType dbcapabilities.DatabaseType, resourceType string, resourceName string) *NotFoundError {
	return &NotFoundError{
		DatabaseType: dbType,
		ResourceType: resourceType,
		ResourceName: resourceName,
	}
}
