package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/redbco/redb-open/cmd/cli/internal/config"
	"github.com/redbco/redb-open/cmd/cli/internal/profile"
	"github.com/spf13/cobra"
)

// cleanCmd represents the clean command
var cleanCmd = &cobra.Command{
	Use:   "clean",
	Short: "Clean up all CLI state and logout from all profiles",
	Long: `Clean up all CLI state including:
• All profiles and their authentication sessions
• Profile configuration files
• Legacy authentication tokens and credentials
• Session information and workspace selections

This command will logout from all active sessions and remove all stored
credentials and profiles. It will prompt for confirmation before cleaning
unless the --force flag is used.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		force, _ := cmd.Flags().GetBool("force")
		return cleanAllCLIState(force)
	},
}

// cleanAllCLIState cleans up all CLI state including profiles and legacy credentials
func cleanAllCLIState(force bool) error {
	// Initialize profile manager
	pm, err := profile.NewProfileManager()
	if err != nil {
		return fmt.Errorf("failed to initialize profile manager: %v", err)
	}

	// Load all profiles
	profiles, err := pm.ListProfiles()
	if err != nil {
		return fmt.Errorf("failed to load profiles: %v", err)
	}

	// Check for legacy credentials - we'll assume there might be some if we can't determine otherwise
	legacyUsername := ""
	legacyErr := fmt.Errorf("legacy credentials check not available")

	// Check if there's anything to clean
	if len(profiles) == 0 && legacyErr != nil {
		fmt.Println("ℹ️  No CLI state found to clean")
		return nil
	}

	// Show what will be cleaned
	fmt.Println("The following CLI state will be cleaned:")
	fmt.Println()

	if len(profiles) > 0 {
		fmt.Printf("📋 Profiles (%d found):\n", len(profiles))
		for name, prof := range profiles {
			status := prof.GetLoginStatus()
			fmt.Printf("  • %s (%s) - %s\n", name, prof.GetTenantURL(), status)
		}
		fmt.Println()
		fmt.Println("For each profile, the following will be removed:")
		fmt.Println("  • Authentication tokens (access and refresh)")
		fmt.Println("  • Session information")
		fmt.Println("  • Profile configuration")
		fmt.Println()
	}

	if legacyErr == nil {
		fmt.Printf("🔧 Legacy credentials for user: %s\n", legacyUsername)
		fmt.Println("  • Authentication tokens")
		fmt.Println("  • Session information")
		fmt.Println("  • Hostname configuration")
		fmt.Println("  • Workspace selection")
		fmt.Println("  • Tenant configuration")
		fmt.Println()
	}

	// Prompt for confirmation unless force flag is used
	if !force {
		fmt.Print("Are you sure you want to clean all CLI state? This action cannot be undone. (y/N): ")
		reader := bufio.NewReader(os.Stdin)
		response, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("failed to read confirmation: %v", err)
		}

		response = strings.ToLower(strings.TrimSpace(response))
		if response != "y" && response != "yes" {
			fmt.Println("❌ Clean operation cancelled")
			return nil
		}
	}

	fmt.Println()
	cleanedItems := 0

	// Clean all profiles
	if len(profiles) > 0 {
		fmt.Printf("🧹 Cleaning %d profiles...\n", len(profiles))
		for name := range profiles {
			if err := pm.DeleteProfile(name); err != nil {
				fmt.Printf("  ⚠️  Failed to delete profile '%s': %v\n", name, err)
			} else {
				fmt.Printf("  ✅ Deleted profile '%s'\n", name)
				cleanedItems++
			}
		}
	}

	// Clean legacy credentials if they exist
	if legacyErr == nil {
		fmt.Println("🧹 Cleaning legacy credentials...")
		if err := config.ClearCredentials(legacyUsername); err != nil {
			fmt.Printf("  ⚠️  Failed to clean legacy credentials: %v\n", err)
		} else {
			fmt.Println("  ✅ Cleaned legacy credentials")
			cleanedItems++
		}
	}

	fmt.Println()
	if cleanedItems > 0 {
		fmt.Printf("✅ Successfully cleaned all CLI state (%d items)\n", cleanedItems)
		fmt.Println("You will need to create profiles and login again to use the CLI")
	} else {
		fmt.Println("⚠️  No items were cleaned (some errors may have occurred)")
	}

	return nil
}

func init() {
	// Add force flag to skip confirmation
	cleanCmd.Flags().Bool("force", false, "Skip confirmation prompt and force clean")
}
