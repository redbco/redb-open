package main

import (
	"fmt"
	"os"
	"runtime"

	"github.com/redbco/redb-open/cmd/cli/internal/config"
	"github.com/spf13/cobra"
)

var (
	configFile string
	version    = "0.0.1"
	// Build information variables (similar to supervisor)
	Version   = "dev"     // Default version for development
	GitCommit = "unknown" // Git commit hash
	BuildTime = "unknown" // Build timestamp
)

// printVersionInfo displays detailed version information
func printVersionInfo() {
	fmt.Printf("reDB CLI v%s - Open Source Version (build %s)\n", version, Version)
	fmt.Printf("Built: %s, from commit: %s\n", BuildTime, GitCommit)
	fmt.Printf("Go version: %s\n", runtime.Version())
	fmt.Printf("OS/Arch: %s/%s\n", runtime.GOOS, runtime.GOARCH)
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "redb-cli",
	Short: "reDB Command Line Interface",
	Long: "A comprehensive CLI for managing reDB resources including authentication, regions, workspaces, databases, " +
		"and more.",
	RunE: func(cmd *cobra.Command, args []string) error {
		// Check if --version flag is set
		if cmd.Flags().Lookup("version") != nil && cmd.Flags().Lookup("version").Changed {
			printVersionInfo()
			return nil
		}
		return cmd.Help()
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	// Initialize configuration
	rootCmd.PersistentFlags().StringVar(&configFile, "config", os.ExpandEnv("$HOME/.redb/config.yaml"), "Path to config file")

	// Add version flag
	rootCmd.Flags().Bool("version", false, "Show version information and exit")

	// Initialize config when the command is executed
	cobra.OnInitialize(func() {
		if err := config.Init(configFile); err != nil {
			fmt.Printf("Error initializing config: %v\n", err)
			os.Exit(1)
		}
	})

	// Setup all commands
	setupCommands()

	// Setup completion
	setupCompletion()
}

func main() {
	Execute()
}
