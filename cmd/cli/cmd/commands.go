package main

import (
	"os"

	"github.com/spf13/cobra"
)

// setupCommands initializes all commands and their relationships
func setupCommands() {
	// Add auth commands
	rootCmd.AddCommand(authCmd)

	// Add regions commands
	rootCmd.AddCommand(regionsCmd)

	// Add workspaces commands
	rootCmd.AddCommand(workspacesCmd)

	// Add tenants commands
	rootCmd.AddCommand(tenantsCmd)

	// Add users commands
	rootCmd.AddCommand(usersCmd)

	// Add environments commands
	rootCmd.AddCommand(environmentsCmd)

	// Add instances commands
	rootCmd.AddCommand(instancesCmd)

	// Add databases commands
	rootCmd.AddCommand(databasesCmd)

	// Add mappings commands
	rootCmd.AddCommand(mappingsCmd)

	// Add repos commands
	rootCmd.AddCommand(reposCmd)

	// Add branches commands
	rootCmd.AddCommand(branchesCmd)

	// Add commits commands
	rootCmd.AddCommand(commitsCmd)
}

// setupCompletion adds shell completion support
func setupCompletion() {
	// Add completion command
	rootCmd.AddCommand(completionCmd)

	// Setup custom completions
	setupCustomCompletions()
}

// completionCmd represents the completion command
var completionCmd = &cobra.Command{
	Use:   "completion [bash|zsh|fish|powershell]",
	Short: "Generate completion script",
	Long: `To load completions:

Bash:
  $ source <(redb-cli completion bash)

  # To load completions for each session, execute once:
  # Linux:
  $ redb-cli completion bash > /etc/bash_completion.d/redb-cli
  # macOS:
  $ redb-cli completion bash > /usr/local/etc/bash_completion.d/redb-cli

Zsh:
  # If shell completion is not already enabled in your environment,
  # you will need to enable it, see https://zsh.sourceforge.io/Doc/Release/Options.html#index-COMPLETE_005fALIASES

  $ source <(redb-cli completion zsh)

  # To load completions for each session, execute once:
  $ redb-cli completion zsh > "${fpath[1]}/_redb-cli"

Fish:
  $ redb-cli completion fish | source

  # To load completions for each session, execute once:
  $ redb-cli completion fish > ~/.config/fish/completions/redb-cli.fish

PowerShell:
  PS> redb-cli completion powershell | Out-String | Invoke-Expression

  # To load completions for every new session, run:
  PS> redb-cli completion powershell > redb-cli.ps1
  # and source this file from your PowerShell profile.
`,
	DisableFlagsInUseLine: true,
	ValidArgs:             []string{"bash", "zsh", "fish", "powershell"},
	Args:                  cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
	Run: func(cmd *cobra.Command, args []string) {
		switch args[0] {
		case "bash":
			cmd.Root().GenBashCompletion(os.Stdout)
		case "zsh":
			cmd.Root().GenZshCompletion(os.Stdout)
		case "fish":
			cmd.Root().GenFishCompletion(os.Stdout, true)
		case "powershell":
			cmd.Root().GenPowerShellCompletion(os.Stdout)
		}
	},
}
