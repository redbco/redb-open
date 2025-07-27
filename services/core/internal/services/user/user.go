package user

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
	"golang.org/x/crypto/bcrypt"
)

// Service handles user-related operations
type Service struct {
	db     *database.PostgreSQL
	logger *logger.Logger
}

// NewService creates a new user service
func NewService(db *database.PostgreSQL, logger *logger.Logger) *Service {
	return &Service{
		db:     db,
		logger: logger,
	}
}

// User represents a user in the system
type User struct {
	ID              string
	TenantID        string
	Email           string
	Name            string
	PasswordHash    string
	Enabled         bool
	PasswordChanged time.Time
	Created         time.Time
	Updated         time.Time
}

// Create creates a new user
func (s *Service) Create(ctx context.Context, tenantID, email, name, password string) (*User, error) {
	s.logger.Infof("Creating user in database for tenant: %s, email: %s", tenantID, email)

	// First, check if the tenant exists
	var tenantExists bool
	err := s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM tenants WHERE tenant_id = $1)", tenantID).Scan(&tenantExists)
	if err != nil {
		return nil, fmt.Errorf("failed to check tenant existence: %w", err)
	}
	if !tenantExists {
		return nil, errors.New("tenant not found")
	}

	// Check if user with this email already exists (globally unique)
	var emailExists bool
	err = s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM users WHERE user_email = $1)", email).Scan(&emailExists)
	if err != nil {
		return nil, fmt.Errorf("failed to check email existence: %w", err)
	}
	if emailExists {
		return nil, errors.New("user with this email already exists")
	}

	// Hash the password
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return nil, fmt.Errorf("failed to hash password: %w", err)
	}

	// Insert the user into the database
	query := `
		INSERT INTO users (tenant_id, user_email, user_name, user_password_hash, user_enabled, password_changed)
		VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
		RETURNING user_id, tenant_id, user_email, user_name, user_password_hash, user_enabled, password_changed, created, updated
	`

	var user User
	err = s.db.Pool().QueryRow(ctx, query, tenantID, email, name, string(hashedPassword), true).Scan(
		&user.ID,
		&user.TenantID,
		&user.Email,
		&user.Name,
		&user.PasswordHash,
		&user.Enabled,
		&user.PasswordChanged,
		&user.Created,
		&user.Updated,
	)
	if err != nil {
		s.logger.Errorf("Failed to create user: %v", err)
		return nil, err
	}

	return &user, nil
}

// Get retrieves a user by ID
func (s *Service) Get(ctx context.Context, tenantID, userID string) (*User, error) {
	s.logger.Infof("Retrieving user from database with ID: %s", userID)
	query := `
		SELECT user_id, tenant_id, user_email, user_name, user_password_hash, user_enabled, password_changed, created, updated
		FROM users
		WHERE tenant_id = $1 AND user_id = $2
	`

	var user User
	err := s.db.Pool().QueryRow(ctx, query, tenantID, userID).Scan(
		&user.ID,
		&user.TenantID,
		&user.Email,
		&user.Name,
		&user.PasswordHash,
		&user.Enabled,
		&user.PasswordChanged,
		&user.Created,
		&user.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("user not found")
		}
		s.logger.Errorf("Failed to get user: %v", err)
		return nil, err
	}

	return &user, nil
}

// GetByEmail retrieves a user by email (globally unique)
func (s *Service) GetByEmail(ctx context.Context, email string) (*User, error) {
	s.logger.Infof("Retrieving user from database with email: %s", email)
	query := `
		SELECT user_id, tenant_id, user_email, user_name, user_password_hash, user_enabled, password_changed, created, updated
		FROM users
		WHERE user_email = $1
	`

	var user User
	err := s.db.Pool().QueryRow(ctx, query, email).Scan(
		&user.ID,
		&user.TenantID,
		&user.Email,
		&user.Name,
		&user.PasswordHash,
		&user.Enabled,
		&user.PasswordChanged,
		&user.Created,
		&user.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("user not found")
		}
		s.logger.Errorf("Failed to get user by email: %v", err)
		return nil, err
	}

	return &user, nil
}

// List retrieves all users for a tenant
func (s *Service) List(ctx context.Context, tenantID string) ([]*User, error) {
	s.logger.Infof("Listing users from database for tenant: %s", tenantID)
	query := `
		SELECT user_id, tenant_id, user_email, user_name, user_password_hash, user_enabled, password_changed, created, updated
		FROM users
		WHERE tenant_id = $1
		ORDER BY user_id
	`

	// Add detailed logging
	//s.logger.Infof("Executing query: %s with args: %v", query, tenantID)

	rows, err := s.db.Pool().Query(ctx, query, tenantID)
	if err != nil {
		s.logger.Errorf("Failed to list users: %v", err)
		return nil, fmt.Errorf("database query error: %w", err)
	}
	defer rows.Close()

	var users []*User
	for rows.Next() {
		var user User
		err := rows.Scan(
			&user.ID,
			&user.TenantID,
			&user.Email,
			&user.Name,
			&user.PasswordHash,
			&user.Enabled,
			&user.PasswordChanged,
			&user.Created,
			&user.Updated,
		)
		if err != nil {
			return nil, err
		}
		users = append(users, &user)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return users, nil
}

// Update updates specific fields of a user
func (s *Service) Update(ctx context.Context, tenantID, userID string, updates map[string]interface{}) (*User, error) {
	s.logger.Infof("Updating user in database with ID: %s, updates: %v", userID, updates)
	// If no updates, just return the current user
	if len(updates) == 0 {
		return s.Get(ctx, tenantID, userID)
	}

	// Build the update query dynamically based on provided fields
	query := "UPDATE users SET updated = CURRENT_TIMESTAMP"
	args := []interface{}{}
	argIndex := 1

	// Add each field that needs to be updated
	for field, value := range updates {
		// Check if the value is a SQL function (like CURRENT_TIMESTAMP)
		if strValue, ok := value.(string); ok && strValue == "CURRENT_TIMESTAMP" {
			// For SQL functions, don't use parameter binding
			query += fmt.Sprintf(", %s = %s", field, strValue)
		} else {
			// For regular values, use parameter binding
			query += fmt.Sprintf(", %s = $%d", field, argIndex)
			args = append(args, value)
			argIndex++
		}
	}

	// Add the WHERE clause with the user ID
	query += fmt.Sprintf(" WHERE tenant_id = $%d AND user_id = $%d RETURNING user_id, tenant_id, user_email, user_name, user_password_hash, user_enabled, password_changed, created, updated", argIndex, argIndex+1)
	args = append(args, tenantID, userID)

	// Execute the update query
	var user User
	err := s.db.Pool().QueryRow(ctx, query, args...).Scan(
		&user.ID,
		&user.TenantID,
		&user.Email,
		&user.Name,
		&user.PasswordHash,
		&user.Enabled,
		&user.PasswordChanged,
		&user.Created,
		&user.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("user not found")
		}
		s.logger.Errorf("Failed to update user: %v", err)
		return nil, err
	}

	return &user, nil
}

// UpdatePassword updates a user's password
func (s *Service) UpdatePassword(ctx context.Context, tenantID, userID, newPassword string) error {
	s.logger.Infof("Updating password for user: %s", userID)

	// Hash the new password
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(newPassword), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("failed to hash password: %w", err)
	}

	// Update the password
	query := `
		UPDATE users 
		SET user_password_hash = $1, password_changed = CURRENT_TIMESTAMP, updated = CURRENT_TIMESTAMP
		WHERE tenant_id = $2 AND user_id = $3
	`

	commandTag, err := s.db.Pool().Exec(ctx, query, string(hashedPassword), tenantID, userID)
	if err != nil {
		s.logger.Errorf("Failed to update password: %v", err)
		return err
	}

	if commandTag.RowsAffected() == 0 {
		return errors.New("user not found")
	}

	return nil
}

// VerifyPassword verifies a user's password
func (s *Service) VerifyPassword(ctx context.Context, tenantID, userID, password string) (bool, error) {
	s.logger.Infof("Verifying password for user: %s", userID)

	// Get the user's password hash
	query := `
		SELECT user_password_hash
		FROM users
		WHERE tenant_id = $1 AND user_id = $2
	`

	var passwordHash string
	err := s.db.Pool().QueryRow(ctx, query, tenantID, userID).Scan(&passwordHash)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, errors.New("user not found")
		}
		return false, err
	}

	// Compare the password with the hash
	err = bcrypt.CompareHashAndPassword([]byte(passwordHash), []byte(password))
	if err != nil {
		return false, nil // Password doesn't match
	}

	return true, nil
}

// Delete deletes a user
func (s *Service) Delete(ctx context.Context, tenantID, userID string) error {
	s.logger.Infof("Deleting user from database with ID: %s", userID)
	query := `
		DELETE FROM users
		WHERE tenant_id = $1 AND user_id = $2
	`

	commandTag, err := s.db.Pool().Exec(ctx, query, tenantID, userID)
	if err != nil {
		s.logger.Errorf("Failed to delete user: %v", err)
		return err
	}

	if commandTag.RowsAffected() == 0 {
		return errors.New("user not found")
	}

	return nil
}

// Exists checks if a user with the given ID exists for a tenant
func (s *Service) Exists(ctx context.Context, tenantID, userID string) (bool, error) {
	query := `
		SELECT EXISTS(SELECT 1 FROM users WHERE tenant_id = $1 AND user_id = $2)
	`

	var exists bool
	err := s.db.Pool().QueryRow(ctx, query, tenantID, userID).Scan(&exists)
	if err != nil {
		return false, err
	}

	return exists, nil
}

// EmailExists checks if a user with the given email exists (globally)
func (s *Service) EmailExists(ctx context.Context, email string) (bool, error) {
	query := `
		SELECT EXISTS(SELECT 1 FROM users WHERE user_email = $1)
	`

	var exists bool
	err := s.db.Pool().QueryRow(ctx, query, email).Scan(&exists)
	if err != nil {
		return false, err
	}

	return exists, nil
}

// GenerateRandomPassword generates a random password
func (s *Service) GenerateRandomPassword(length int) (string, error) {
	if length <= 0 {
		length = 16
	}

	bytes := make([]byte, length)
	_, err := rand.Read(bytes)
	if err != nil {
		return "", err
	}

	return hex.EncodeToString(bytes)[:length], nil
}
