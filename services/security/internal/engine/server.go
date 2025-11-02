package engine

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	mathrand "math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	securityv1 "github.com/redbco/redb-open/api/proto/security/v1"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/keyring"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SecurityServer struct {
	securityv1.UnimplementedSecurityServiceServer
	engine        *Engine
	secretManager *TenantJWTSecretManager
}

func NewSecurityServer(engine *Engine) *SecurityServer {
	return &SecurityServer{
		engine:        engine,
		secretManager: NewTenantJWTSecretManager(),
	}
}

// User represents a user from the database
type User struct {
	UserID       string
	TenantID     string
	Email        string
	Name         string
	PasswordHash string
	Enabled      bool
}

// JWTClaims represents the claims in our JWT tokens
type JWTClaims struct {
	UserID    string `json:"user_id"`
	TenantID  string `json:"tenant_id"`
	Email     string `json:"email"`
	SessionID string `json:"session_id"`
	jwt.RegisteredClaims
}

const (
	// Keyring service name for reDB security
	KeyringService = "redb-security"
	// Keyring key prefix for tenant JWT secrets
	JWTSecretKeyPrefix = "tenant-jwt-secret"
	// Default secret length in bytes
	DefaultSecretLength = 64
)

// TenantJWTSecretManager handles secure storage and retrieval of tenant-specific JWT secrets
type TenantJWTSecretManager struct {
	keyringManager *keyring.KeyringManager
	keyPrefix      string
	cache          map[string][]byte // In-memory cache for secrets
	cacheMu        sync.RWMutex      // Protects the cache
}

// NewTenantJWTSecretManager creates a new tenant JWT secret manager
func NewTenantJWTSecretManager() *TenantJWTSecretManager {
	// Initialize keyring manager
	keyringPath := keyring.GetDefaultKeyringPath()
	masterPassword := keyring.GetMasterPasswordFromEnv()
	km := keyring.NewKeyringManager(keyringPath, masterPassword)

	return &TenantJWTSecretManager{
		keyringManager: km,
		keyPrefix:      JWTSecretKeyPrefix,
		cache:          make(map[string][]byte),
	}
}

// getTenantSecretKey generates the keyring key for a specific tenant
func (tjsm *TenantJWTSecretManager) getTenantSecretKey(tenantID string) string {
	return fmt.Sprintf("%s-%s", tjsm.keyPrefix, tenantID)
}

// GetTenantSecret retrieves the JWT signing secret for a specific tenant from the keyring
func (tjsm *TenantJWTSecretManager) GetTenantSecret(tenantID string) ([]byte, error) {
	if tenantID == "" {
		return nil, errors.New("tenant ID is required")
	}

	// Check cache first (with read lock)
	tjsm.cacheMu.RLock()
	if cachedSecret, exists := tjsm.cache[tenantID]; exists {
		tjsm.cacheMu.RUnlock()
		// Return a copy to avoid external modifications
		secretCopy := make([]byte, len(cachedSecret))
		copy(secretCopy, cachedSecret)
		return secretCopy, nil
	}
	tjsm.cacheMu.RUnlock()

	// Not in cache, load from keyring
	secretKey := tjsm.getTenantSecretKey(tenantID)

	// Try to get existing secret from keyring
	secret, err := tjsm.keyringManager.Get(KeyringService, secretKey)
	if err != nil {
		return nil, errors.New("tenant JWT secret not found")
	}

	// Decode base64 secret
	secretBytes, err := base64.StdEncoding.DecodeString(secret)
	if err != nil {
		return nil, fmt.Errorf("failed to decode tenant secret: %w", err)
	}

	// Store in cache (with write lock)
	tjsm.cacheMu.Lock()
	tjsm.cache[tenantID] = secretBytes
	tjsm.cacheMu.Unlock()

	// Return a copy to avoid external modifications
	secretCopy := make([]byte, len(secretBytes))
	copy(secretCopy, secretBytes)
	return secretCopy, nil
}

// CreateTenantSecret generates and stores a new JWT secret for a tenant
func (tjsm *TenantJWTSecretManager) CreateTenantSecret(tenantID string) ([]byte, error) {
	if tenantID == "" {
		return nil, errors.New("tenant ID is required")
	}

	// Check if secret already exists
	existing, err := tjsm.GetTenantSecret(tenantID)
	if err == nil && len(existing) > 0 {
		return nil, errors.New("tenant JWT secret already exists")
	}

	// Generate random secret
	secretBytes := make([]byte, DefaultSecretLength)
	if _, err := rand.Read(secretBytes); err != nil {
		return nil, fmt.Errorf("failed to generate random secret: %w", err)
	}

	// Encode secret as base64 for storage
	secretString := base64.StdEncoding.EncodeToString(secretBytes)

	// Store in keyring
	secretKey := tjsm.getTenantSecretKey(tenantID)
	err = tjsm.keyringManager.Set(KeyringService, secretKey, secretString)
	if err != nil {
		return nil, fmt.Errorf("failed to store tenant secret: %w", err)
	}

	// Update cache
	tjsm.cacheMu.Lock()
	tjsm.cache[tenantID] = secretBytes
	tjsm.cacheMu.Unlock()

	return secretBytes, nil
}

// RotateTenantSecret generates a new JWT secret for a tenant and replaces the existing one
func (tjsm *TenantJWTSecretManager) RotateTenantSecret(tenantID string) ([]byte, error) {
	if tenantID == "" {
		return nil, errors.New("tenant ID is required")
	}

	// Generate random secret
	secretBytes := make([]byte, DefaultSecretLength)
	if _, err := rand.Read(secretBytes); err != nil {
		return nil, fmt.Errorf("failed to generate random secret: %w", err)
	}

	// Encode secret as base64 for storage
	secretString := base64.StdEncoding.EncodeToString(secretBytes)

	// Store in keyring (this will overwrite the existing secret)
	secretKey := tjsm.getTenantSecretKey(tenantID)
	err := tjsm.keyringManager.Set(KeyringService, secretKey, secretString)
	if err != nil {
		return nil, fmt.Errorf("failed to rotate tenant secret: %w", err)
	}

	// Update cache
	tjsm.cacheMu.Lock()
	tjsm.cache[tenantID] = secretBytes
	tjsm.cacheMu.Unlock()

	return secretBytes, nil
}

// SetTenantSecret stores a JWT secret for a tenant (overwriting any existing secret)
func (tjsm *TenantJWTSecretManager) SetTenantSecret(tenantID string, secret []byte) error {
	if tenantID == "" {
		return errors.New("tenant ID is required")
	}
	if len(secret) == 0 {
		return errors.New("secret is required")
	}

	// Encode secret as base64 for storage
	secretString := base64.StdEncoding.EncodeToString(secret)

	// Store in keyring (this will overwrite any existing secret)
	secretKey := tjsm.getTenantSecretKey(tenantID)
	err := tjsm.keyringManager.Set(KeyringService, secretKey, secretString)
	if err != nil {
		return fmt.Errorf("failed to set tenant secret: %w", err)
	}

	// Update cache
	tjsm.cacheMu.Lock()
	tjsm.cache[tenantID] = secret
	tjsm.cacheMu.Unlock()

	return nil
}

// DeleteTenantSecret removes the JWT secret for a tenant from the keyring
func (tjsm *TenantJWTSecretManager) DeleteTenantSecret(tenantID string) error {
	if tenantID == "" {
		return errors.New("tenant ID is required")
	}

	secretKey := tjsm.getTenantSecretKey(tenantID)
	err := tjsm.keyringManager.Delete(KeyringService, secretKey)
	
	// Remove from cache
	tjsm.cacheMu.Lock()
	delete(tjsm.cache, tenantID)
	tjsm.cacheMu.Unlock()
	
	// Note: file-based keyring doesn't return "not found" errors the same way
	// so we'll just ignore any errors here
	return err
}

// getTenantIDByURL resolves a tenant_url to a tenant_id
func (s *SecurityServer) getTenantIDByURL(ctx context.Context, db *database.PostgreSQL, tenantURL string) (string, error) {
	if tenantURL == "" {
		return "", errors.New("tenant URL is required")
	}

	query := `SELECT tenant_id FROM tenants WHERE tenant_url = $1`

	var tenantID string
	row := db.Pool().QueryRow(ctx, query, tenantURL)

	err := row.Scan(&tenantID)
	if err != nil {
		if err.Error() == "no rows in result set" {
			return "", errors.New("tenant not found")
		}
		return "", err
	}

	return tenantID, nil
}

// getTenantJWTSecret returns the JWT signing secret for a specific tenant
func (s *SecurityServer) getTenantJWTSecret(tenantID string) ([]byte, error) {
	if tenantID == "" {
		return nil, errors.New("tenant ID is required")
	}

	// Use the cached secret manager
	secret, err := s.secretManager.GetTenantSecret(tenantID)
	if err != nil {
		s.engine.logger.Error("Failed to get tenant JWT secret from keyring")
		return nil, fmt.Errorf("tenant JWT secret not found for tenant %s", tenantID)
	}

	return secret, nil
}

// Login handles user login requests
func (s *SecurityServer) Login(ctx context.Context, req *securityv1.LoginRequest) (*securityv1.LoginResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementLoginAttempts()
	s.engine.IncrementRequestsProcessed()

	// Validate input
	if req.Username == "" || req.Password == "" {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.InvalidArgument, "username and password are required")
	}

	if req.TenantUrl == "" {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.InvalidArgument, "tenant URL is required")
	}

	// Get database connection
	db := s.engine.GetDatabase()
	if db == nil {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.Internal, "authentication service temporarily unavailable")
	}

	// Resolve tenant_url to tenant_id
	tenantID, err := s.getTenantIDByURL(ctx, db, req.TenantUrl)
	if err != nil {
		s.engine.IncrementErrors()
		// Don't reveal if tenant exists or not - return as invalid credentials
		return nil, status.Error(codes.Unauthenticated, "invalid credentials")
	}

	// Query user by email (username is treated as email)
	user, err := s.getUserByEmail(ctx, db, req.Username)
	if err != nil {
		s.engine.IncrementErrors()
		// Don't reveal if user exists or not - return as invalid credentials
		return nil, status.Error(codes.Unauthenticated, "invalid credentials")
	}

	// Validate that user belongs to the requested tenant
	if user.TenantID != tenantID {
		s.engine.IncrementErrors()
		// Don't reveal tenant mismatch - return as invalid credentials
		return nil, status.Error(codes.Unauthenticated, "invalid credentials")
	}

	// Check if user is enabled
	if !user.Enabled {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.FailedPrecondition, "user account is disabled")
	}

	// Verify password
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)); err != nil {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.Unauthenticated, "invalid credentials")
	}

	// Generate session ID
	sessionID := s.generateSessionID()

	// Collect session information from request
	sessionInfo := &SessionInfo{
		SessionName: getStringValue(req.SessionName),
		UserAgent:   getStringValue(req.UserAgent),
		IPAddress:   getStringValue(req.IpAddress),
		Platform:    getStringValue(req.Platform),
		Browser:     getStringValue(req.Browser),
		OS:          getStringValue(req.OperatingSystem),
		DeviceType:  getStringValue(req.DeviceType),
		Location:    getStringValue(req.Location),
	}

	// Generate JWT tokens with session ID
	accessToken, refreshToken, err := s.generateTokens(user, sessionID, req.ExpiryTimeHours)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.Internal, "failed to generate authentication tokens")
	}

	// Store tokens with session information in database
	if err := s.storeTokensWithSession(ctx, db, user.UserID, sessionID, accessToken, refreshToken, sessionInfo); err != nil {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.Internal, "failed to store authentication tokens")
	}

	// Get tenant workspaces
	workspaces, err := s.getTenantWorkspaces(ctx, db, tenantID)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.Internal, "failed to get tenant workspaces")
	}

	// Return successful response
	return &securityv1.LoginResponse{
		Profile: &securityv1.Profile{
			TenantId:   user.TenantID,
			UserId:     user.UserID,
			Username:   user.Email,
			Email:      user.Email,
			Name:       user.Name,
			Workspaces: workspaces,
		},
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		SessionId:    sessionID,
		Status:       commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// Authenticate handles authentication requests using JWT tokens
func (s *SecurityServer) Authenticate(ctx context.Context, req *securityv1.AuthenticationRequest) (*securityv1.AuthenticationResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementAuthenticationRequests()
	s.engine.IncrementRequestsProcessed()

	// Validate input
	if req.Token == "" {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.InvalidArgument, "token is required")
	}

	// Get database connection
	db := s.engine.GetDatabase()
	if db == nil {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.Internal, "authentication service temporarily unavailable")
	}

	var tenantID string
	var err error

	// For global operations, tenant_url may be empty
	// In this case, we'll extract tenant ID from the JWT token itself
	if req.TenantUrl == "" {
		// Parse token to extract tenant ID (without full validation)
		token, _ := jwt.ParseWithClaims(req.Token, &JWTClaims{}, func(token *jwt.Token) (interface{}, error) {
			// We don't validate the signature here, just extract claims
			return nil, errors.New("parsing for tenant ID only")
		})

		if token != nil {
			if claims, ok := token.Claims.(*JWTClaims); ok && claims.TenantID != "" {
				tenantID = claims.TenantID
			}
		}

		if tenantID == "" {
			s.engine.IncrementErrors()
			return nil, status.Error(codes.InvalidArgument, "tenant information not found in token")
		}
	} else {
		// Resolve tenant_url to tenant_id for tenant-specific operations
		tenantID, err = s.getTenantIDByURL(ctx, db, req.TenantUrl)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Error(codes.Unauthenticated, "invalid tenant")
		}
	}

	// Parse and validate the JWT token
	token, err := jwt.ParseWithClaims(req.Token, &JWTClaims{}, func(token *jwt.Token) (interface{}, error) {
		// Verify the signing method
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, errors.New("invalid signing method")
		}

		// Use the resolved tenant ID for JWT secret lookup
		tenantSecret, err := s.getTenantJWTSecret(tenantID)
		if err != nil {
			return nil, fmt.Errorf("failed to get tenant JWT secret: %w", err)
		}
		return tenantSecret, nil
	})

	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.Unauthenticated, "invalid token")
	}

	// Extract claims
	claims, ok := token.Claims.(*JWTClaims)
	if !ok || !token.Valid {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.Unauthenticated, "invalid token claims")
	}

	// Verify tenant ID matches the resolved tenant from tenant_url
	if claims.TenantID != tenantID {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.Unauthenticated, "tenant mismatch")
	}

	// Get user from database to ensure they still exist and are enabled
	_, err = s.getUserByID(ctx, db, claims.UserID)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.Unauthenticated, "user not found or disabled")
	}

	// Additional validation: check if token exists in database and is not expired
	dbUser, err := s.validateTokenInDatabase(ctx, db, req.Token, req.TokenType)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.Unauthenticated, "token not found in database or expired")
	}

	// Use the user from database validation for consistency
	user := dbUser

	// Check token type and handle accordingly
	var accessToken, refreshToken string

	if req.TokenType == "refresh" {
		// For refresh tokens, generate new access and refresh tokens with same session ID
		accessToken, refreshToken, err = s.generateTokens(user, claims.SessionID, nil)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Error(codes.Internal, "failed to generate authentication tokens")
		}

		// Update tokens in database (keep same session)
		if err := s.updateTokensInSession(ctx, db, user.UserID, claims.SessionID, accessToken, refreshToken); err != nil {
			s.engine.IncrementErrors()
			return nil, status.Error(codes.Internal, "failed to store authentication tokens")
		}
	} else {
		// For access tokens, just return the user profile without generating new tokens
		accessToken = req.Token
		refreshToken = "" // Don't return refresh token for access token validation

		// Update last activity
		if err := s.updateLastActivity(ctx, db, user.UserID, claims.SessionID); err != nil {
			// Log but don't fail - this is not critical
			s.engine.logger.Warnf("Failed to update last activity for session %s: %v", claims.SessionID, err)
		}
	}

	// Get tenant workspaces
	workspaces, err := s.getTenantWorkspaces(ctx, db, tenantID)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.Internal, "failed to get tenant workspaces")
	}

	// Return successful response
	return &securityv1.AuthenticationResponse{
		Profile: &securityv1.Profile{
			TenantId:   user.TenantID,
			UserId:     user.UserID,
			Username:   user.Email,
			Email:      user.Email,
			Name:       user.Name,
			Workspaces: workspaces,
		},
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		Status:       commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// Authorize handles authorization checks
func (s *SecurityServer) Authorize(ctx context.Context, req *securityv1.AuthorizationRequest) (*securityv1.AuthorizationResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementAuthorizationRequests()
	s.engine.IncrementRequestsProcessed()

	// TODO: Implement actual authorization logic based on the comprehensive
	// authorization system described in the README
	// For now, return a placeholder response that allows everything
	return &securityv1.AuthorizationResponse{
		Authorized: true,
		Message:    "Access granted (placeholder implementation)",
		Status:     commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// Logout handles user logout requests
func (s *SecurityServer) Logout(ctx context.Context, req *securityv1.LogoutRequest) (*securityv1.LogoutResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Validate input
	if req.RefreshToken == "" {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.InvalidArgument, "refresh token is required")
	}

	// Get database connection
	db := s.engine.GetDatabase()
	if db == nil {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.Internal, "authentication service temporarily unavailable")
	}

	// Try to parse the refresh token to get user ID (optional for better cleanup)
	// If parsing fails, we'll still try to delete the token directly
	token, _ := jwt.ParseWithClaims(req.RefreshToken, &JWTClaims{}, func(token *jwt.Token) (interface{}, error) {
		// We don't need to validate the signature here since we're just extracting user ID
		// The token might be expired or invalid, but we still want to clean it up
		return nil, errors.New("parsing for user ID only")
	})

	var userID string
	if token != nil {
		if claims, ok := token.Claims.(*JWTClaims); ok {
			userID = claims.UserID
		}
	}

	// Delete the refresh token from the database
	// If we have a valid userID, delete all tokens for that user
	// Otherwise, try to delete the specific token
	var deleteQuery string
	var args []interface{}

	if userID != "" {
		// Delete all tokens for the user (logout from all devices)
		deleteQuery = `DELETE FROM user_jwt_tokens WHERE user_id = $1`
		args = []interface{}{userID}
	} else {
		// Delete only the specific refresh token
		deleteQuery = `DELETE FROM user_jwt_tokens WHERE refresh_token = $1`
		args = []interface{}{req.RefreshToken}
	}

	result, err := db.Pool().Exec(ctx, deleteQuery, args...)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.Internal, "failed to logout")
	}

	// Check if any rows were affected
	rowsAffected := result.RowsAffected()
	if rowsAffected == 0 {
		// Token not found, but still return success for security reasons
		// We don't want to leak information about token existence
		return &securityv1.LogoutResponse{
			Message: "Logout successful",
			Status:  commonv1.Status_STATUS_SUCCESS,
		}, nil
	}

	return &securityv1.LogoutResponse{
		Message: "Logout successful",
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// ChangePassword handles password change requests
func (s *SecurityServer) ChangePassword(ctx context.Context, req *securityv1.ChangePasswordRequest) (*securityv1.ChangePasswordResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Validate input
	if req.TenantId == "" {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.InvalidArgument, "tenant ID is required")
	}

	if req.UserId == "" {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.InvalidArgument, "user ID is required")
	}

	if req.OldPassword == "" {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.InvalidArgument, "old password is required")
	}

	if req.NewPassword == "" {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.InvalidArgument, "new password is required")
	}

	// Get database connection
	db := s.engine.GetDatabase()
	if db == nil {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.Internal, "authentication service temporarily unavailable")
	}

	// Get user from database
	user, err := s.getUserByID(ctx, db, req.UserId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.NotFound, "user not found")
	}

	// Validate that user belongs to the specified tenant (security check)
	if user.TenantID != req.TenantId {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.PermissionDenied, "access denied")
	}

	// Check if old password is correct
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.OldPassword)); err != nil {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.Unauthenticated, "invalid credentials")
	}

	// Hash new password
	newPasswordHash, err := bcrypt.GenerateFromPassword([]byte(req.NewPassword), bcrypt.DefaultCost)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.Internal, "failed to process password")
	}

	// Update password in database
	_, err = db.Pool().Exec(ctx, "UPDATE users SET user_password_hash = $1, updated = CURRENT_TIMESTAMP WHERE user_id = $2", newPasswordHash, req.UserId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.Internal, "failed to update password")
	}

	// Return success response
	return &securityv1.ChangePasswordResponse{
		Status:  commonv1.Status_STATUS_SUCCESS,
		Message: "Password changed successfully",
	}, nil
}

// GetTenantJWTSecrets retrieves the JWT secrets for a tenant (for administrative purposes)
func (s *SecurityServer) GetTenantJWTSecrets(ctx context.Context, req *securityv1.GetTenantJWTSecretsRequest) (*securityv1.GetTenantJWTSecretsResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Validate input
	if req.TenantId == "" {
		s.engine.IncrementErrors()
		return &securityv1.GetTenantJWTSecretsResponse{
			Status: commonv1.Status_STATUS_FAILURE,
		}, errors.New("tenant ID is required")
	}

	// Get tenant secret
	secret, err := s.secretManager.GetTenantSecret(req.TenantId)
	if err != nil {
		s.engine.IncrementErrors()
		return &securityv1.GetTenantJWTSecretsResponse{
			Status: commonv1.Status_STATUS_FAILURE,
		}, fmt.Errorf("failed to get tenant JWT secret: %w", err)
	}

	// Return base64 encoded secret (for administrative purposes)
	secretBase64 := base64.StdEncoding.EncodeToString(secret)

	return &securityv1.GetTenantJWTSecretsResponse{
		AccessTokenSecret:  secretBase64,
		RefreshTokenSecret: secretBase64, // Same secret for both access and refresh tokens
		Status:             commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// SetTenantJWTSecrets stores JWT secrets for a tenant
func (s *SecurityServer) SetTenantJWTSecrets(ctx context.Context, req *securityv1.SetTenantJWTSecretsRequest) (*securityv1.SetTenantJWTSecretsResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Validate input
	if req.TenantId == "" {
		s.engine.IncrementErrors()
		return &securityv1.SetTenantJWTSecretsResponse{
			Status: commonv1.Status_STATUS_FAILURE,
		}, errors.New("tenant ID is required")
	}

	if req.AccessTokenSecret == "" {
		s.engine.IncrementErrors()
		return &securityv1.SetTenantJWTSecretsResponse{
			Status: commonv1.Status_STATUS_FAILURE,
		}, errors.New("access token secret is required")
	}

	// For now, we use the same secret for both access and refresh tokens
	var secretToStore string
	if req.RefreshTokenSecret != "" {
		// If both secrets are provided, ensure they match (since we use the same secret)
		if req.AccessTokenSecret != req.RefreshTokenSecret {
			s.engine.IncrementErrors()
			return &securityv1.SetTenantJWTSecretsResponse{
				Status: commonv1.Status_STATUS_FAILURE,
			}, errors.New("access and refresh token secrets must be the same")
		}
		secretToStore = req.RefreshTokenSecret
	} else {
		secretToStore = req.AccessTokenSecret
	}

	// Decode the base64 secret
	secretBytes, err := base64.StdEncoding.DecodeString(secretToStore)
	if err != nil {
		s.engine.IncrementErrors()
		return &securityv1.SetTenantJWTSecretsResponse{
			Status: commonv1.Status_STATUS_FAILURE,
		}, fmt.Errorf("failed to decode secret: %w", err)
	}

	// Validate secret length for security
	if len(secretBytes) < 32 {
		s.engine.IncrementErrors()
		return &securityv1.SetTenantJWTSecretsResponse{
			Status: commonv1.Status_STATUS_FAILURE,
		}, errors.New("secret must be at least 32 bytes")
	}

	// Store the secret
	err = s.secretManager.SetTenantSecret(req.TenantId, secretBytes)
	if err != nil {
		s.engine.IncrementErrors()
		return &securityv1.SetTenantJWTSecretsResponse{
			Status: commonv1.Status_STATUS_FAILURE,
		}, fmt.Errorf("failed to set tenant JWT secret: %w", err)
	}

	// Log the operation for audit purposes
	if s.engine.logger != nil {
		s.engine.logger.Info("Tenant JWT secrets set for tenant: " + req.TenantId)
	}

	return &securityv1.SetTenantJWTSecretsResponse{
		Status: commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// RotateTenantJWTSecrets generates new JWT secrets for a tenant
func (s *SecurityServer) RotateTenantJWTSecrets(ctx context.Context, req *securityv1.RotateTenantJWTSecretsRequest) (*securityv1.RotateTenantJWTSecretsResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Validate input
	if req.TenantId == "" {
		s.engine.IncrementErrors()
		return &securityv1.RotateTenantJWTSecretsResponse{
			Status: commonv1.Status_STATUS_FAILURE,
		}, errors.New("tenant ID is required")
	}

	// Rotate tenant secret
	_, err := s.secretManager.RotateTenantSecret(req.TenantId)
	if err != nil {
		s.engine.IncrementErrors()
		return &securityv1.RotateTenantJWTSecretsResponse{
			Status: commonv1.Status_STATUS_FAILURE,
		}, fmt.Errorf("failed to rotate tenant JWT secrets: %w", err)
	}

	// Log the rotation for audit purposes
	if s.engine.logger != nil {
		s.engine.logger.Info("Tenant JWT secrets rotated for tenant: " + req.TenantId)
	}

	return &securityv1.RotateTenantJWTSecretsResponse{
		Status: commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// DeleteTenantJWTSecrets removes the JWT secrets for a tenant
func (s *SecurityServer) DeleteTenantJWTSecrets(ctx context.Context, req *securityv1.DeleteTenantJWTSecretsRequest) (*securityv1.DeleteTenantJWTSecretsResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Validate input
	if req.TenantId == "" {
		s.engine.IncrementErrors()
		return &securityv1.DeleteTenantJWTSecretsResponse{
			Status: commonv1.Status_STATUS_FAILURE,
		}, errors.New("tenant ID is required")
	}

	// Delete tenant secret
	err := s.secretManager.DeleteTenantSecret(req.TenantId)
	if err != nil {
		s.engine.IncrementErrors()
		return &securityv1.DeleteTenantJWTSecretsResponse{
			Status: commonv1.Status_STATUS_FAILURE,
		}, fmt.Errorf("failed to delete tenant JWT secrets: %w", err)
	}

	// Also invalidate all existing tokens for this tenant by removing them from database
	db := s.engine.GetDatabase()
	if db != nil {
		// Delete all tokens for users of this tenant
		deleteQuery := `
			DELETE FROM user_jwt_tokens 
			WHERE user_id IN (
				SELECT user_id FROM users WHERE tenant_id = $1
			)
		`
		_, err := db.Pool().Exec(ctx, deleteQuery, req.TenantId)
		if err != nil {
			// Log the error but don't fail the secret deletion
			if s.engine.logger != nil {
				s.engine.logger.Error("Failed to invalidate tenant tokens in database for tenant: " + req.TenantId + " error: " + err.Error())
			}
		}
	}

	// Log the deletion for audit purposes
	if s.engine.logger != nil {
		s.engine.logger.Info("Tenant JWT secrets deleted for tenant: " + req.TenantId)
	}

	return &securityv1.DeleteTenantJWTSecretsResponse{
		Status: commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// ListSessions retrieves all active sessions for a user
func (s *SecurityServer) ListSessions(ctx context.Context, req *securityv1.ListSessionsRequest) (*securityv1.ListSessionsResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Validate input
	if req.TenantId == "" || req.UserId == "" {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.InvalidArgument, "tenant ID and user ID are required")
	}

	// Get database connection
	db := s.engine.GetDatabase()
	if db == nil {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.Internal, "authentication service temporarily unavailable")
	}

	// Query all active sessions for the user
	query := `
		SELECT session_id, session_name, session_agent, session_ip_address, 
		       session_platform, session_browser, session_os, session_device_type, 
		       session_location, last_activity, created, expires
		FROM user_jwt_tokens 
		WHERE user_id = $1 AND expires > CURRENT_TIMESTAMP
		ORDER BY last_activity DESC
	`

	rows, err := db.Pool().Query(ctx, query, req.UserId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.Internal, "failed to retrieve sessions")
	}
	defer rows.Close()

	var sessions []*securityv1.SessionInfo
	for rows.Next() {
		var session securityv1.SessionInfo
		var lastActivity, created, expires time.Time

		err := rows.Scan(
			&session.SessionId, &session.SessionName, &session.UserAgent, &session.IpAddress,
			&session.Platform, &session.Browser, &session.OperatingSystem, &session.DeviceType,
			&session.Location, &lastActivity, &created, &expires,
		)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Error(codes.Internal, "failed to parse session data")
		}

		// Format timestamps
		session.LastActivity = lastActivity.Format(time.RFC3339)
		session.Created = created.Format(time.RFC3339)
		session.Expires = expires.Format(time.RFC3339)

		sessions = append(sessions, &session)
	}

	return &securityv1.ListSessionsResponse{
		Sessions: sessions,
		Status:   commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// LogoutSession logs out a specific session
func (s *SecurityServer) LogoutSession(ctx context.Context, req *securityv1.LogoutSessionRequest) (*securityv1.LogoutSessionResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Validate input
	if req.TenantId == "" || req.UserId == "" || req.SessionId == "" {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.InvalidArgument, "tenant ID, user ID, and session ID are required")
	}

	// Get database connection
	db := s.engine.GetDatabase()
	if db == nil {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.Internal, "authentication service temporarily unavailable")
	}

	// Delete the specific session
	deleteQuery := `DELETE FROM user_jwt_tokens WHERE user_id = $1 AND session_id = $2`
	result, err := db.Pool().Exec(ctx, deleteQuery, req.UserId, req.SessionId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.Internal, "failed to logout session")
	}

	if result.RowsAffected() == 0 {
		return nil, status.Error(codes.NotFound, "session not found")
	}

	return &securityv1.LogoutSessionResponse{
		Status:  commonv1.Status_STATUS_SUCCESS,
		Message: "Session logged out successfully",
	}, nil
}

// LogoutAllSessions logs out all sessions for a user
func (s *SecurityServer) LogoutAllSessions(ctx context.Context, req *securityv1.LogoutAllSessionsRequest) (*securityv1.LogoutAllSessionsResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Validate input
	if req.TenantId == "" || req.UserId == "" {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.InvalidArgument, "tenant ID and user ID are required")
	}

	// Get database connection
	db := s.engine.GetDatabase()
	if db == nil {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.Internal, "authentication service temporarily unavailable")
	}

	var deleteQuery string
	var args []interface{}

	if req.ExcludeCurrent != nil && *req.ExcludeCurrent {
		// TODO: Implement logic to exclude current session
		// For now, we'll delete all sessions
		deleteQuery = `DELETE FROM user_jwt_tokens WHERE user_id = $1`
		args = []interface{}{req.UserId}
	} else {
		deleteQuery = `DELETE FROM user_jwt_tokens WHERE user_id = $1`
		args = []interface{}{req.UserId}
	}

	result, err := db.Pool().Exec(ctx, deleteQuery, args...)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.Internal, "failed to logout sessions")
	}

	sessionsLoggedOut := int32(result.RowsAffected())

	return &securityv1.LogoutAllSessionsResponse{
		SessionsLoggedOut: sessionsLoggedOut,
		Status:            commonv1.Status_STATUS_SUCCESS,
		Message:           fmt.Sprintf("Successfully logged out %d session(s)", sessionsLoggedOut),
	}, nil
}

// UpdateSessionName updates the display name of a session
func (s *SecurityServer) UpdateSessionName(ctx context.Context, req *securityv1.UpdateSessionNameRequest) (*securityv1.UpdateSessionNameResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Validate input
	if req.TenantId == "" || req.UserId == "" || req.SessionId == "" {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.InvalidArgument, "tenant ID, user ID, and session ID are required")
	}

	if req.SessionName == "" {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.InvalidArgument, "session name is required")
	}

	// Get database connection
	db := s.engine.GetDatabase()
	if db == nil {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.Internal, "authentication service temporarily unavailable")
	}

	// Update the session name
	updateQuery := `
		UPDATE user_jwt_tokens 
		SET session_name = $3, updated = CURRENT_TIMESTAMP 
		WHERE user_id = $1 AND session_id = $2
	`
	result, err := db.Pool().Exec(ctx, updateQuery, req.UserId, req.SessionId, req.SessionName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Error(codes.Internal, "failed to update session name")
	}

	if result.RowsAffected() == 0 {
		return nil, status.Error(codes.NotFound, "session not found")
	}

	return &securityv1.UpdateSessionNameResponse{
		Status:  commonv1.Status_STATUS_SUCCESS,
		Message: "Session name updated successfully",
	}, nil
}

// Helper methods

// getUserByEmail retrieves a user by email from the database
func (s *SecurityServer) getUserByEmail(ctx context.Context, db *database.PostgreSQL, email string) (*User, error) {
	query := `
		SELECT user_id, tenant_id, user_email, user_name, user_password_hash, user_enabled
		FROM users 
		WHERE user_email = $1 AND user_enabled = true
	`

	var user User
	row := db.Pool().QueryRow(ctx, query, email)

	err := row.Scan(&user.UserID, &user.TenantID, &user.Email, &user.Name, &user.PasswordHash, &user.Enabled)
	if err != nil {
		if err.Error() == "no rows in result set" {
			return nil, errors.New("user not found")
		}
		return nil, err
	}

	return &user, nil
}

// getUserByID retrieves a user by ID from the database
func (s *SecurityServer) getUserByID(ctx context.Context, db *database.PostgreSQL, userID string) (*User, error) {
	query := `
		SELECT user_id, tenant_id, user_email, user_name, user_password_hash, user_enabled
		FROM users 
		WHERE user_id = $1 AND user_enabled = true
	`

	var user User
	row := db.Pool().QueryRow(ctx, query, userID)

	err := row.Scan(&user.UserID, &user.TenantID, &user.Email, &user.Name, &user.PasswordHash, &user.Enabled)
	if err != nil {
		if err.Error() == "no rows in result set" {
			return nil, errors.New("user not found")
		}
		return nil, err
	}

	return &user, nil
}

// getTenantWorkspaces retrieves a tenant's workspaces from the database
func (s *SecurityServer) getTenantWorkspaces(ctx context.Context, db *database.PostgreSQL, tenantID string) ([]*securityv1.Workspace, error) {
	query := `
		SELECT workspace_id, workspace_name, workspace_description
		FROM workspaces
		WHERE tenant_id = $1
	`

	var workspaces []*securityv1.Workspace
	rows, err := db.Pool().Query(ctx, query, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var workspace securityv1.Workspace
		err := rows.Scan(&workspace.WorkspaceId, &workspace.WorkspaceName, &workspace.WorkspaceDescription)
		if err != nil {
			return nil, err
		}
		workspaces = append(workspaces, &workspace)
	}

	return workspaces, nil
}

// generateTokens creates access and refresh JWT tokens using tenant-specific secrets
func (s *SecurityServer) generateTokens(user *User, sessionID string, expiryHours *string) (accessToken, refreshToken string, err error) {
	// Default expiry times
	accessTokenExpiry := time.Hour * 24       // 24 hours
	refreshTokenExpiry := time.Hour * 24 * 30 // 30 days

	// Parse custom expiry if provided
	if expiryHours != nil && *expiryHours != "" {
		if hours, parseErr := strconv.Atoi(*expiryHours); parseErr == nil && hours > 0 {
			accessTokenExpiry = time.Hour * time.Duration(hours)
		}
	}

	// Get tenant-specific JWT secret
	tenantSecret, err := s.getTenantJWTSecret(user.TenantID)
	if err != nil {
		return "", "", fmt.Errorf("failed to get tenant JWT secret: %w", err)
	}

	// Create access token
	accessClaims := &JWTClaims{
		UserID:    user.UserID,
		TenantID:  user.TenantID,
		Email:     user.Email,
		SessionID: sessionID,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(accessTokenExpiry)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			Subject:   user.UserID,
		},
	}

	accessTokenObj := jwt.NewWithClaims(jwt.SigningMethodHS256, accessClaims)
	accessToken, err = accessTokenObj.SignedString(tenantSecret)
	if err != nil {
		return "", "", fmt.Errorf("failed to sign access token: %w", err)
	}

	// Create refresh token
	refreshClaims := &JWTClaims{
		UserID:    user.UserID,
		TenantID:  user.TenantID,
		Email:     user.Email,
		SessionID: sessionID,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(refreshTokenExpiry)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			Subject:   user.UserID,
		},
	}

	refreshTokenObj := jwt.NewWithClaims(jwt.SigningMethodHS256, refreshClaims)
	refreshToken, err = refreshTokenObj.SignedString(tenantSecret)
	if err != nil {
		return "", "", fmt.Errorf("failed to sign refresh token: %w", err)
	}

	return accessToken, refreshToken, nil
}

// validateTokenInDatabase checks if a token exists and is valid in the database
func (s *SecurityServer) validateTokenInDatabase(ctx context.Context, db *database.PostgreSQL, token, tokenType string) (*User, error) {
	var query string

	if tokenType == "refresh" {
		query = `
			SELECT u.user_id, u.tenant_id, u.user_email, u.user_name, u.user_password_hash, u.user_enabled
			FROM users u
			JOIN user_jwt_tokens ujt ON u.user_id = ujt.user_id
			WHERE ujt.refresh_token = $1 AND u.user_enabled = true AND ujt.expires > CURRENT_TIMESTAMP
		`
	} else {
		query = `
			SELECT u.user_id, u.tenant_id, u.user_email, u.user_name, u.user_password_hash, u.user_enabled
			FROM users u
			JOIN user_jwt_tokens ujt ON u.user_id = ujt.user_id
			WHERE ujt.access_token = $1 AND u.user_enabled = true AND ujt.expires > CURRENT_TIMESTAMP
		`
	}

	var user User
	row := db.Pool().QueryRow(ctx, query, token)

	err := row.Scan(&user.UserID, &user.TenantID, &user.Email, &user.Name, &user.PasswordHash, &user.Enabled)
	if err != nil {
		if err.Error() == "no rows in result set" {
			return nil, errors.New("token not found or expired")
		}
		return nil, err
	}

	return &user, nil
}

// updateTokensInSession updates the tokens in the database for a specific session
func (s *SecurityServer) updateTokensInSession(ctx context.Context, db *database.PostgreSQL, userID, sessionID, accessToken, refreshToken string) error {
	// Update tokens in database
	updateQuery := `
		UPDATE user_jwt_tokens 
		SET access_token = $3, refresh_token = $4, updated = CURRENT_TIMESTAMP 
		WHERE user_id = $1 AND session_id = $2
	`
	_, err := db.Pool().Exec(ctx, updateQuery, userID, sessionID, accessToken, refreshToken)
	return err
}

// updateLastActivity updates the last activity timestamp for a session
func (s *SecurityServer) updateLastActivity(ctx context.Context, db *database.PostgreSQL, userID, sessionID string) error {
	// Update last activity timestamp
	updateQuery := `
		UPDATE user_jwt_tokens 
		SET last_activity = CURRENT_TIMESTAMP 
		WHERE user_id = $1 AND session_id = $2
	`
	_, err := db.Pool().Exec(ctx, updateQuery, userID, sessionID)
	return err
}

// storeTokensWithSession stores the JWT tokens with session information in the database
func (s *SecurityServer) storeTokensWithSession(ctx context.Context, db *database.PostgreSQL, userID, sessionID, accessToken, refreshToken string, sessionInfo *SessionInfo) error {
	// Insert new tokens with session information
	insertQuery := `
		INSERT INTO user_jwt_tokens (
			user_id, session_id, refresh_token, access_token, 
			session_name, session_agent, session_ip_address, 
			session_platform, session_browser, session_os, 
			session_device_type, session_location, last_activity, 
			created, updated, expires
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP + INTERVAL '24 hours')
	`

	_, err := db.Pool().Exec(ctx, insertQuery,
		userID, sessionID, refreshToken, accessToken,
		sessionInfo.SessionName, sessionInfo.UserAgent, sessionInfo.IPAddress,
		sessionInfo.Platform, sessionInfo.Browser, sessionInfo.OS,
		sessionInfo.DeviceType, sessionInfo.Location,
	)
	return err
}

// getStringValue safely extracts string value from optional proto string
func getStringValue(optionalString *string) string {
	if optionalString == nil {
		return ""
	}
	return *optionalString
}

// SessionInfo holds session-related information
type SessionInfo struct {
	SessionName string
	UserAgent   string
	IPAddress   string
	Platform    string
	Browser     string
	OS          string
	DeviceType  string
	Location    string
}

// generateSessionID creates a unique session identifier
func (s *SecurityServer) generateSessionID() string {
	return fmt.Sprintf("session_%d_%s", time.Now().UnixNano(), generateRandomString(8))
}

// generateRandomString creates a random string of specified length
func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[mathrand.Intn(len(charset))]
	}
	return string(b)
}

// ValidateMCPSession validates an MCP session token (JWT or API token)
func (s *SecurityServer) ValidateMCPSession(ctx context.Context, req *securityv1.ValidateMCPSessionRequest) (*securityv1.ValidateMCPSessionResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementAuthenticationRequests()
	s.engine.IncrementRequestsProcessed()

	// Validate input
	if req.Token == "" {
		s.engine.IncrementErrors()
		return &securityv1.ValidateMCPSessionResponse{
			Valid:   false,
			Message: "token is required",
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	// Get database connection
	db := s.engine.GetDatabase()
	if db == nil {
		s.engine.IncrementErrors()
		return &securityv1.ValidateMCPSessionResponse{
			Valid:   false,
			Message: "authentication service temporarily unavailable",
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	// Handle JWT token validation
	if req.TokenType == "jwt" || req.TokenType == "" {
		// Parse token to extract tenant ID (without full validation)
		token, _ := jwt.ParseWithClaims(req.Token, &JWTClaims{}, func(token *jwt.Token) (interface{}, error) {
			return nil, errors.New("parsing for tenant ID only")
		})

		var tenantID string
		if token != nil {
			if claims, ok := token.Claims.(*JWTClaims); ok && claims.TenantID != "" {
				tenantID = claims.TenantID
			}
		}

		if tenantID == "" {
			return &securityv1.ValidateMCPSessionResponse{
				Valid:   false,
				Message: "tenant information not found in token",
				Status:  commonv1.Status_STATUS_ERROR,
			}, nil
		}

		// Parse and validate the JWT token with proper signature verification
		validatedToken, err := jwt.ParseWithClaims(req.Token, &JWTClaims{}, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, errors.New("invalid signing method")
			}
			tenantSecret, err := s.getTenantJWTSecret(tenantID)
			if err != nil {
				return nil, fmt.Errorf("failed to get tenant JWT secret: %w", err)
			}
			return tenantSecret, nil
		})

		if err != nil || !validatedToken.Valid {
			return &securityv1.ValidateMCPSessionResponse{
				Valid:   false,
				Message: "invalid token",
				Status:  commonv1.Status_STATUS_ERROR,
			}, nil
		}

		// Extract claims
		_, ok := validatedToken.Claims.(*JWTClaims)
		if !ok {
			return &securityv1.ValidateMCPSessionResponse{
				Valid:   false,
				Message: "invalid token claims",
				Status:  commonv1.Status_STATUS_ERROR,
			}, nil
		}

		// Validate token in database
		user, err := s.validateTokenInDatabase(ctx, db, req.Token, "access")
		if err != nil {
			return &securityv1.ValidateMCPSessionResponse{
				Valid:   false,
				Message: "token not found or expired",
				Status:  commonv1.Status_STATUS_ERROR,
			}, nil
		}

		// Get default workspace for tenant
		workspaceID := "default"
		workspaces, err := s.getTenantWorkspaces(ctx, db, user.TenantID)
		if err == nil && len(workspaces) > 0 {
			workspaceID = workspaces[0].WorkspaceId
		}

		return &securityv1.ValidateMCPSessionResponse{
			Valid:       true,
			TenantId:    user.TenantID,
			WorkspaceId: workspaceID,
			UserId:      user.UserID,
			Message:     "token validated successfully",
			Status:      commonv1.Status_STATUS_SUCCESS,
		}, nil
	}

	// Handle API token validation
	if req.TokenType == "api_token" {
		// Query API token from apitokens table
		var tenantID, workspaceID, userID string
		var enabled bool
		var expires *time.Time

		query := `
			SELECT tenant_id, workspace_id, user_id, apitoken_enabled, apitoken_expires
			FROM apitokens
			WHERE apitoken_hash = $1
		`

		// Hash the provided token to compare with stored hash
		tokenHash := hashAPIToken(req.Token)
		err := db.Pool().QueryRow(ctx, query, tokenHash).Scan(&tenantID, &workspaceID, &userID, &enabled, &expires)
		if err != nil {
			return &securityv1.ValidateMCPSessionResponse{
				Valid:   false,
				Message: "invalid API token",
				Status:  commonv1.Status_STATUS_ERROR,
			}, nil
		}

		// Check if token is enabled
		if !enabled {
			return &securityv1.ValidateMCPSessionResponse{
				Valid:   false,
				Message: "API token is disabled",
				Status:  commonv1.Status_STATUS_ERROR,
			}, nil
		}

		// Check if token is expired
		if expires != nil && expires.Before(time.Now()) {
			return &securityv1.ValidateMCPSessionResponse{
				Valid:   false,
				Message: "API token has expired",
				Status:  commonv1.Status_STATUS_ERROR,
			}, nil
		}

		// Update last used timestamp
		_, _ = db.Pool().Exec(ctx, `
			UPDATE apitokens 
			SET apitoken_last_used = CURRENT_TIMESTAMP 
			WHERE apitoken_hash = $1
		`, tokenHash)

		return &securityv1.ValidateMCPSessionResponse{
			Valid:       true,
			TenantId:    tenantID,
			WorkspaceId: workspaceID,
			UserId:      userID,
			Message:     "API token validated successfully",
			Status:      commonv1.Status_STATUS_SUCCESS,
		}, nil
	}

	return &securityv1.ValidateMCPSessionResponse{
		Valid:   false,
		Message: "unsupported token type",
		Status:  commonv1.Status_STATUS_ERROR,
	}, nil
}

// AuthorizeMCPOperation authorizes an MCP operation
func (s *SecurityServer) AuthorizeMCPOperation(ctx context.Context, req *securityv1.AuthorizeMCPOperationRequest) (*securityv1.AuthorizeMCPOperationResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get database connection
	db := s.engine.GetDatabase()
	if db == nil {
		s.engine.IncrementErrors()
		return &securityv1.AuthorizeMCPOperationResponse{
			Authorized: false,
			Message:    "authorization service temporarily unavailable",
			Status:     commonv1.Status_STATUS_ERROR,
		}, nil
	}

	// Verify user exists and is enabled
	var userEnabled bool
	err := db.Pool().QueryRow(ctx, `
		SELECT user_enabled 
		FROM users 
		WHERE user_id = $1 AND tenant_id = $2
	`, req.UserId, req.TenantId).Scan(&userEnabled)

	if err != nil {
		return &securityv1.AuthorizeMCPOperationResponse{
			Authorized: false,
			Message:    "user not found",
			Status:     commonv1.Status_STATUS_ERROR,
		}, nil
	}

	if !userEnabled {
		return &securityv1.AuthorizeMCPOperationResponse{
			Authorized: false,
			Message:    "user is disabled",
			Status:     commonv1.Status_STATUS_ERROR,
		}, nil
	}

	// Verify MCP server exists and is enabled
	var mcpServerEnabled bool
	var policyIDs []string
	err = db.Pool().QueryRow(ctx, `
		SELECT mcpserver_enabled, COALESCE(policy_ids, '{}')
		FROM mcpservers 
		WHERE mcpserver_id = $1 AND tenant_id = $2
	`, req.McpServerId, req.TenantId).Scan(&mcpServerEnabled, &policyIDs)

	if err != nil {
		return &securityv1.AuthorizeMCPOperationResponse{
			Authorized: false,
			Message:    "MCP server not found",
			Status:     commonv1.Status_STATUS_ERROR,
		}, nil
	}

	if !mcpServerEnabled {
		return &securityv1.AuthorizeMCPOperationResponse{
			Authorized: false,
			Message:    "MCP server is disabled",
			Status:     commonv1.Status_STATUS_ERROR,
		}, nil
	}

	// TODO: Implement policy evaluation
	// For now, if the user exists and is enabled, and the MCP server exists and is enabled,
	// we authorize the operation. In the future, this should check policies from the policies table.

	appliedPolicies := []string{}
	if len(policyIDs) > 0 {
		// In future implementation, evaluate these policies
		appliedPolicies = policyIDs
	}

	return &securityv1.AuthorizeMCPOperationResponse{
		Authorized:      true,
		Message:         "operation authorized",
		AppliedPolicies: appliedPolicies,
		Status:          commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// GetMCPPolicies retrieves MCP policies
func (s *SecurityServer) GetMCPPolicies(ctx context.Context, req *securityv1.GetMCPPoliciesRequest) (*securityv1.GetMCPPoliciesResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get database connection
	db := s.engine.GetDatabase()
	if db == nil {
		s.engine.IncrementErrors()
		return &securityv1.GetMCPPoliciesResponse{
			Policies: []*securityv1.MCPPolicy{},
			Status:   commonv1.Status_STATUS_ERROR,
		}, nil
	}

	// If no policy IDs provided, return empty list
	if len(req.PolicyIds) == 0 {
		return &securityv1.GetMCPPoliciesResponse{
			Policies: []*securityv1.MCPPolicy{},
			Status:   commonv1.Status_STATUS_SUCCESS,
		}, nil
	}

	// Query policies from database
	query := `
		SELECT policy_id, policy_name, policy_description, policy_object
		FROM policies
		WHERE policy_id = ANY($1) AND tenant_id = $2
	`

	rows, err := db.Pool().Query(ctx, query, req.PolicyIds, req.TenantId)
	if err != nil {
		s.engine.IncrementErrors()
		return &securityv1.GetMCPPoliciesResponse{
			Policies: []*securityv1.MCPPolicy{},
			Status:   commonv1.Status_STATUS_ERROR,
		}, nil
	}
	defer rows.Close()

	var policies []*securityv1.MCPPolicy
	for rows.Next() {
		var policyID, policyName, policyDescription string
		var policyObject []byte

		err := rows.Scan(&policyID, &policyName, &policyDescription, &policyObject)
		if err != nil {
			continue
		}

		policies = append(policies, &securityv1.MCPPolicy{
			PolicyId:          policyID,
			PolicyName:        policyName,
			PolicyDescription: policyDescription,
			PolicyObject:      policyObject,
		})
	}

	return &securityv1.GetMCPPoliciesResponse{
		Policies: policies,
		Status:   commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// hashAPIToken creates a hash of an API token for storage/comparison
func hashAPIToken(token string) string {
	hash := sha256.Sum256([]byte(token))
	return fmt.Sprintf("%x", hash)
}
