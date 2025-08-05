package users

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"
	"syscall"
	"text/tabwriter"

	"github.com/redbco/redb-open/cmd/cli/internal/config"
	"github.com/redbco/redb-open/cmd/cli/internal/httpclient"
	"golang.org/x/term"
)

type User struct {
	TenantID    string `json:"tenant_id"`
	UserID      string `json:"user_id"`
	UserName    string `json:"user_name"`
	UserEmail   string `json:"user_email"`
	UserEnabled bool   `json:"user_enabled"`
}

// Response wraps the API response for listing users
type Response struct {
	Users []User `json:"users"`
}

// UserResponse wraps the API response for a single user
type UserResponse struct {
	User User `json:"user"`
}

// CreateUserResponse wraps the API response for creating a user
type CreateUserResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	User    User   `json:"user"`
	Status  string `json:"status"`
}

// UpdateUserResponse wraps the API response for updating a user
type UpdateUserResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	User    User   `json:"user"`
	Status  string `json:"status"`
}

type CreateUserRequest struct {
	UserName     string `json:"user_name"`
	UserEmail    string `json:"user_email"`
	UserPassword string `json:"user_password"`
	UserEnabled  bool   `json:"user_enabled,omitempty"`
}

type UpdateUserRequest struct {
	UserName     string `json:"user_name,omitempty"`
	UserEmail    string `json:"user_email,omitempty"`
	UserPassword string `json:"user_password,omitempty"`
	UserEnabled  *bool  `json:"user_enabled,omitempty"`
}

// ListUsers lists all users
func ListUsers() error {
	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/users", tenantURL)

	var response Response
	if err := client.Get(url, &response, true); err != nil {
		return fmt.Errorf("failed to get users: %v", err)
	}

	users := response.Users

	if len(users) == 0 {
		fmt.Println("No users found")
		return nil
	}

	// Sort users by name
	sort.Slice(users, func(i, j int) bool {
		return users[i].UserName < users[j].UserName
	})

	// Create a tabwriter for formatted output
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)

	fmt.Println()

	// Print header
	fmt.Fprintln(w, "Name\tEmail\tStatus\tUser ID")
	fmt.Fprintln(w, "----\t-----\t------\t-------")

	// Print each user
	for _, user := range users {
		status := "Enabled"
		if !user.UserEnabled {
			status = "Disabled"
		}

		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n",
			user.UserName,
			user.UserEmail,
			status,
			user.UserID)
	}

	_ = w.Flush()
	fmt.Println()
	return nil
}

// ShowUser displays details of a specific user
func ShowUser(userID string) error {
	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/users/%s", tenantURL, userID)

	var response UserResponse
	if err := client.Get(url, &response, true); err != nil {
		return fmt.Errorf("failed to get user: %v", err)
	}

	user := response.User

	fmt.Println()
	fmt.Printf("User ID: %s\n", user.UserID)
	fmt.Printf("Name: %s\n", user.UserName)
	fmt.Printf("Email: %s\n", user.UserEmail)
	fmt.Printf("Status: %s\n", func() string {
		if user.UserEnabled {
			return "Enabled"
		}
		return "Disabled"
	}())
	fmt.Printf("Tenant ID: %s\n", user.TenantID)
	fmt.Println()
	return nil
}

// AddUser creates a new user
func AddUser(args []string) error {
	reader := bufio.NewReader(os.Stdin)

	// Get user name
	var userName string
	if len(args) > 0 && strings.HasPrefix(args[0], "--name=") {
		userName = strings.TrimPrefix(args[0], "--name=")
	} else {
		fmt.Print("User Name: ")
		userName, _ = reader.ReadString('\n')
		userName = strings.TrimSpace(userName)
	}

	if userName == "" {
		return fmt.Errorf("user name is required")
	}

	// Get user email
	var userEmail string
	if len(args) > 1 && strings.HasPrefix(args[1], "--email=") {
		userEmail = strings.TrimPrefix(args[1], "--email=")
	} else {
		fmt.Print("User Email: ")
		userEmail, _ = reader.ReadString('\n')
		userEmail = strings.TrimSpace(userEmail)
	}

	if userEmail == "" {
		return fmt.Errorf("user email is required")
	}

	// Get user password
	var userPassword string
	if len(args) > 2 && strings.HasPrefix(args[2], "--password=") {
		userPassword = strings.TrimPrefix(args[2], "--password=")
	} else {
		fmt.Print("User Password: ")
		passwordBytes, err := term.ReadPassword(int(syscall.Stdin))
		if err != nil {
			return fmt.Errorf("failed to read password: %v", err)
		}
		userPassword = string(passwordBytes)
		fmt.Println() // Add newline after password input
	}

	if userPassword == "" {
		return fmt.Errorf("user password is required")
	}

	// Get optional enabled status
	userEnabled := true
	if len(args) > 3 && strings.HasPrefix(args[3], "--enabled=") {
		enabledStr := strings.TrimPrefix(args[3], "--enabled=")
		if enabledStr == "false" {
			userEnabled = false
		}
	} else {
		fmt.Print("User Enabled (true/false, default: true): ")
		enabledStr, _ := reader.ReadString('\n')
		enabledStr = strings.TrimSpace(enabledStr)
		if enabledStr == "false" {
			userEnabled = false
		}
	}

	// Create the user
	createReq := CreateUserRequest{
		UserName:     userName,
		UserEmail:    userEmail,
		UserPassword: userPassword,
		UserEnabled:  userEnabled,
	}

	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/users", tenantURL)

	var createResponse CreateUserResponse
	if err := client.Post(url, createReq, &createResponse, true); err != nil {
		return fmt.Errorf("failed to create user: %v", err)
	}

	fmt.Printf("Successfully created user '%s' (ID: %s)\n", createResponse.User.UserName, createResponse.User.UserID)
	return nil
}

// ModifyUser updates an existing user
func ModifyUser(userID string, args []string) error {
	// First find the user to get its details
	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/users/%s", tenantURL, userID)

	fmt.Println()

	var response UserResponse
	if err := client.Get(url, &response, true); err != nil {
		return fmt.Errorf("failed to get user: %v", err)
	}

	targetUser := response.User

	reader := bufio.NewReader(os.Stdin)
	updateReq := UpdateUserRequest{}
	hasChanges := false

	// Parse command line arguments or prompt for input
	for _, arg := range args {
		switch {
		case strings.HasPrefix(arg, "--name="):
			updateReq.UserName = strings.TrimPrefix(arg, "--name=")
			hasChanges = true
		case strings.HasPrefix(arg, "--email="):
			updateReq.UserEmail = strings.TrimPrefix(arg, "--email=")
			hasChanges = true
		case strings.HasPrefix(arg, "--password="):
			updateReq.UserPassword = strings.TrimPrefix(arg, "--password=")
			hasChanges = true
		case strings.HasPrefix(arg, "--enabled="):
			enabledStr := strings.TrimPrefix(arg, "--enabled=")
			enabled := enabledStr == "true"
			updateReq.UserEnabled = &enabled
			hasChanges = true
		}
	}

	// If no arguments provided, prompt for input
	if !hasChanges {
		fmt.Printf("Modifying user '%s' (press Enter to keep current value):\n", userID)

		fmt.Printf("New Name [%s]: ", targetUser.UserName)
		newName, _ := reader.ReadString('\n')
		newName = strings.TrimSpace(newName)
		if newName != "" {
			updateReq.UserName = newName
			hasChanges = true
		}

		fmt.Printf("New Email [%s]: ", targetUser.UserEmail)
		newEmail, _ := reader.ReadString('\n')
		newEmail = strings.TrimSpace(newEmail)
		if newEmail != "" {
			updateReq.UserEmail = newEmail
			hasChanges = true
		}

		fmt.Print("New Password (leave blank to keep current): ")
		passwordBytes, err := term.ReadPassword(int(syscall.Stdin))
		if err != nil {
			return fmt.Errorf("failed to read password: %v", err)
		}
		newPassword := string(passwordBytes)
		fmt.Println() // Add newline after password input
		if newPassword != "" {
			updateReq.UserPassword = newPassword
			hasChanges = true
		}

		currentStatus := "true"
		if !targetUser.UserEnabled {
			currentStatus = "false"
		}
		fmt.Printf("Enabled (true/false) [%s]: ", currentStatus)
		enabledStr, _ := reader.ReadString('\n')
		enabledStr = strings.TrimSpace(enabledStr)
		if enabledStr != "" {
			enabled := enabledStr == "true"
			updateReq.UserEnabled = &enabled
			hasChanges = true
		}
	}

	if !hasChanges {
		fmt.Println("No changes made")
		return nil
	}

	// Update the user
	updateURL := fmt.Sprintf("%s/api/v1/users/%s", tenantURL, userID)

	var updateResponse UpdateUserResponse
	if err := client.Put(updateURL, updateReq, &updateResponse, true); err != nil {
		return fmt.Errorf("failed to update user: %v", err)
	}

	fmt.Printf("Successfully updated user '%s'\n", updateResponse.User.UserName)
	fmt.Println()
	return nil
}

// DeleteUser removes an existing user
func DeleteUser(userID string, args []string) error {
	// Check for force flag
	force := false
	for _, arg := range args {
		if arg == "--force" || arg == "-f" {
			force = true
			break
		}
	}

	// First find the user to get its details
	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	client := httpclient.GetClient()

	// Confirm deletion unless force flag is used
	if !force {
		reader := bufio.NewReader(os.Stdin)
		fmt.Println()
		fmt.Printf("Are you sure you want to delete user '%s'? This action cannot be undone. (y/N): ", userID)
		confirmation, _ := reader.ReadString('\n')
		confirmation = strings.TrimSpace(strings.ToLower(confirmation))

		if confirmation != "y" && confirmation != "yes" {
			fmt.Println("Operation cancelled")
			fmt.Println()
			return nil
		}
	}

	// Delete the user
	deleteURL := fmt.Sprintf("%s/api/v1/users/%s", tenantURL, userID)

	if err := client.Delete(deleteURL, true); err != nil {
		return fmt.Errorf("failed to delete user: %v", err)
	}

	fmt.Printf("Successfully deleted user '%s'\n", userID)
	fmt.Println()
	return nil
}
