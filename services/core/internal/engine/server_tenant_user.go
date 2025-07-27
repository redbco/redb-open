package engine

import (
	"context"
	"fmt"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	"github.com/redbco/redb-open/services/core/internal/services/tenant"
	"github.com/redbco/redb-open/services/core/internal/services/user"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ============================================================================
// UserService gRPC handlers
// ============================================================================

func (s *Server) ListUsers(ctx context.Context, req *corev1.ListUsersRequest) (*corev1.ListUsersResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get user service
	userService := user.NewService(s.engine.db, s.engine.logger)

	// List users for the tenant
	users, err := userService.List(ctx, req.TenantId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to list users: %v", err)
	}

	// Convert to protobuf format
	protoUsers := make([]*corev1.User, len(users))
	for i, u := range users {
		protoUsers[i] = s.userToProto(u)
	}

	return &corev1.ListUsersResponse{
		Users: protoUsers,
	}, nil
}

func (s *Server) ShowUser(ctx context.Context, req *corev1.ShowUserRequest) (*corev1.ShowUserResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get user service
	userService := user.NewService(s.engine.db, s.engine.logger)

	// Get the user
	userObj, err := userService.Get(ctx, req.TenantId, req.UserId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "user not found: %v", err)
	}

	// Convert to protobuf format
	protoUser := s.userToProto(userObj)

	return &corev1.ShowUserResponse{
		User: protoUser,
	}, nil
}

func (s *Server) AddUser(ctx context.Context, req *corev1.AddUserRequest) (*corev1.AddUserResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get user service
	userService := user.NewService(s.engine.db, s.engine.logger)

	// Create the user
	createdUser, err := userService.Create(ctx, req.TenantId, req.UserEmail, req.UserName, req.UserPassword)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create user: %v", err)
	}

	// Convert to protobuf format
	protoUser := s.userToProto(createdUser)

	return &corev1.AddUserResponse{
		Message: fmt.Sprintf("User %s created successfully", createdUser.Email),
		Success: true,
		User:    protoUser,
		Status:  commonv1.Status_STATUS_CREATED,
	}, nil
}

func (s *Server) ModifyUser(ctx context.Context, req *corev1.ModifyUserRequest) (*corev1.ModifyUserResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get user service
	userService := user.NewService(s.engine.db, s.engine.logger)

	// Build updates map
	updates := make(map[string]interface{})
	if req.UserName != nil {
		updates["user_name"] = *req.UserName
	}
	if req.UserEmail != nil {
		updates["user_email"] = *req.UserEmail
	}
	if req.UserPassword != nil {
		// Hash the new password
		hashedPassword, err := bcrypt.GenerateFromPassword([]byte(*req.UserPassword), bcrypt.DefaultCost)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to hash password: %v", err)
		}
		updates["user_password_hash"] = string(hashedPassword)
		updates["password_changed"] = "CURRENT_TIMESTAMP"
	}
	if req.UserEnabled != nil {
		updates["user_enabled"] = *req.UserEnabled
	}

	// Update the user
	updatedUser, err := userService.Update(ctx, req.TenantId, req.UserId, updates)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to update user: %v", err)
	}

	// Convert to protobuf format
	protoUser := s.userToProto(updatedUser)

	return &corev1.ModifyUserResponse{
		Message: fmt.Sprintf("User %s updated successfully", updatedUser.Email),
		Success: true,
		User:    protoUser,
		Status:  commonv1.Status_STATUS_UPDATED,
	}, nil
}

func (s *Server) DeleteUser(ctx context.Context, req *corev1.DeleteUserRequest) (*corev1.DeleteUserResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get user service
	userService := user.NewService(s.engine.db, s.engine.logger)

	// Delete the user
	err := userService.Delete(ctx, req.TenantId, req.UserId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to delete user: %v", err)
	}

	return &corev1.DeleteUserResponse{
		Message: "User deleted successfully",
		Success: true,
		Status:  commonv1.Status_STATUS_DELETED,
	}, nil
}

// ============================================================================
// TenantService gRPC handlers
// ============================================================================

func (s *Server) ListTenants(ctx context.Context, req *corev1.ListTenantsRequest) (*corev1.ListTenantsResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get tenant service
	tenantService := tenant.NewService(s.engine.db, s.engine.logger)

	// List all tenants
	tenants, err := tenantService.List(ctx)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to list tenants: %v", err)
	}

	// Convert to protobuf format
	protoTenants := make([]*corev1.Tenant, len(tenants))
	for i, t := range tenants {
		protoTenants[i] = s.tenantToProto(t)
	}

	return &corev1.ListTenantsResponse{
		Tenants: protoTenants,
	}, nil
}

func (s *Server) ShowTenant(ctx context.Context, req *corev1.ShowTenantRequest) (*corev1.ShowTenantResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get tenant service
	tenantService := tenant.NewService(s.engine.db, s.engine.logger)

	// Get the tenant
	tenantObj, err := tenantService.Get(ctx, req.TenantId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "tenant not found: %v", err)
	}

	// Convert to protobuf format
	protoTenant := s.tenantToProto(tenantObj)

	return &corev1.ShowTenantResponse{
		Tenant: protoTenant,
	}, nil
}

func (s *Server) AddTenant(ctx context.Context, req *corev1.AddTenantRequest) (*corev1.AddTenantResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get tenant service
	tenantService := tenant.NewService(s.engine.db, s.engine.logger)

	// Check if tenant with this name already exists
	nameExists, err := tenantService.NameExists(ctx, req.TenantName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to check tenant name existence: %v", err)
	}
	if nameExists {
		return nil, status.Errorf(codes.AlreadyExists, "tenant with name %s already exists", req.TenantName)
	}

	// Check if tenant with this URL already exists
	urlExists, err := tenantService.URLExists(ctx, req.TenantUrl)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to check tenant URL existence: %v", err)
	}
	if urlExists {
		return nil, status.Errorf(codes.AlreadyExists, "tenant with URL %s already exists", req.TenantUrl)
	}

	// Check if user with this email already exists (globally unique)
	userService := user.NewService(s.engine.db, s.engine.logger)
	emailExists, err := userService.EmailExists(ctx, req.UserEmail)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to check user email existence: %v", err)
	}
	if emailExists {
		return nil, status.Errorf(codes.AlreadyExists, "user with email %s already exists", req.UserEmail)
	}

	// Create the tenant
	createdTenant, err := tenantService.Create(ctx, req.TenantName, req.TenantDescription, req.TenantUrl)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create tenant: %v", err)
	}

	// Create the root user for this tenant
	createdUser, err := userService.Create(ctx, createdTenant.ID, req.UserEmail, req.UserEmail, req.UserPassword)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create root user for tenant: %v", err)
	}

	// Convert to protobuf format
	protoTenant := s.tenantToProto(createdTenant)

	return &corev1.AddTenantResponse{
		Message: fmt.Sprintf("Tenant %s created successfully with root user %s", createdTenant.Name, createdUser.Email),
		Success: true,
		Tenant:  protoTenant,
	}, nil
}

func (s *Server) ModifyTenant(ctx context.Context, req *corev1.ModifyTenantRequest) (*corev1.ModifyTenantResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get tenant service
	tenantService := tenant.NewService(s.engine.db, s.engine.logger)

	// Build updates map
	updates := make(map[string]interface{})
	if req.TenantName != nil {
		// Check if new name already exists
		nameExists, err := tenantService.NameExists(ctx, *req.TenantName)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to check tenant name existence: %v", err)
		}
		if nameExists {
			return nil, status.Errorf(codes.AlreadyExists, "tenant with name %s already exists", *req.TenantName)
		}
		updates["tenant_name"] = *req.TenantName
	}
	if req.TenantDescription != nil {
		updates["tenant_description"] = *req.TenantDescription
	}

	// Update the tenant
	updatedTenant, err := tenantService.Update(ctx, req.TenantId, updates)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to update tenant: %v", err)
	}

	// Convert to protobuf format
	protoTenant := s.tenantToProto(updatedTenant)

	return &corev1.ModifyTenantResponse{
		Message: fmt.Sprintf("Tenant %s updated successfully", updatedTenant.Name),
		Success: true,
		Tenant:  protoTenant,
	}, nil
}

func (s *Server) DeleteTenant(ctx context.Context, req *corev1.DeleteTenantRequest) (*corev1.DeleteTenantResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get tenant service
	tenantService := tenant.NewService(s.engine.db, s.engine.logger)

	// Delete the tenant
	err := tenantService.Delete(ctx, req.TenantId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to delete tenant: %v", err)
	}

	return &corev1.DeleteTenantResponse{
		Message: "Tenant deleted successfully",
		Success: true,
	}, nil
}
