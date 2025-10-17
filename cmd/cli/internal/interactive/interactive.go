package interactive

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/chzyer/readline"
	"github.com/redbco/redb-open/cmd/cli/internal/profile"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// StartInteractiveMode starts the interactive REPL mode
func StartInteractiveMode(rootCmd *cobra.Command) error {
	// Create completer for tab completion
	completer := buildCompleter(rootCmd)

	// Configure readline
	rl, err := readline.NewEx(&readline.Config{
		Prompt:          getPromptString(),
		HistoryFile:     getHistoryFile(),
		AutoComplete:    completer,
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	if err != nil {
		return fmt.Errorf("failed to initialize interactive mode: %v", err)
	}
	defer rl.Close()

	fmt.Println("Welcome to reDB interactive mode!")
	fmt.Println("Type 'help' for available commands, 'exit' or 'quit' to exit.")
	fmt.Println()

	// Main REPL loop
	for {
		// Update prompt dynamically
		rl.SetPrompt(getPromptString())

		// Read line
		line, err := rl.Readline()
		if err != nil {
			if err == readline.ErrInterrupt {
				// Handle Ctrl+C
				if len(line) == 0 {
					fmt.Println("Type 'exit' or 'quit' to exit")
					continue
				}
				continue
			} else if err == io.EOF {
				// Handle Ctrl+D
				fmt.Println("exit")
				break
			}
			return err
		}

		// Trim whitespace
		line = strings.TrimSpace(line)

		// Skip empty lines
		if line == "" {
			continue
		}

		// Handle special commands
		if handleSpecialCommand(line) {
			continue
		}

		// Check for exit commands
		if line == "exit" || line == "quit" {
			fmt.Println("Goodbye!")
			break
		}

		// Execute command through Cobra
		if err := parseAndExecuteCommand(line, rootCmd); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		}
	}

	return nil
}

// getPromptString generates the dynamic prompt based on profile and auth status
func getPromptString() string {
	pm, err := profile.NewProfileManager()
	if err != nil {
		return "reDB (error)> "
	}

	activeProfileName, err := pm.GetActiveProfile()
	if err != nil || activeProfileName == "" {
		// No active profile
		return "reDB (not logged in)> "
	}

	prof, err := pm.GetProfile(activeProfileName)
	if err != nil {
		return "reDB (error)> "
	}

	// Check if logged in
	if !prof.IsLoggedIn() {
		return fmt.Sprintf("reDB %s (not logged in)> ", activeProfileName)
	}

	// Logged in - show user and workspace
	workspace := prof.Workspace
	if workspace == "" {
		workspace = "no-workspace"
	}

	return fmt.Sprintf("reDB %s (%s@%s)> ", activeProfileName, prof.Username, workspace)
}

// getHistoryFile returns the path to the history file
func getHistoryFile() string {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return homeDir + "/.redb/cli_history"
}

// handleSpecialCommand handles built-in special commands
// Returns true if the command was handled
func handleSpecialCommand(line string) bool {
	switch line {
	case "clear":
		// Clear screen
		fmt.Print("\033[H\033[2J")
		return true
	}
	return false
}

// parseAndExecuteCommand parses a command line and executes it through Cobra
func parseAndExecuteCommand(line string, rootCmd *cobra.Command) error {
	// Parse command line into arguments (respecting quotes)
	args, err := parseCommandLine(line)
	if err != nil {
		return fmt.Errorf("failed to parse command: %v", err)
	}

	if len(args) == 0 {
		return nil
	}

	// Create a copy of the root command to avoid state pollution
	// We need to reset the command tree for each execution
	cmdCopy := cloneCommand(rootCmd)

	// Set arguments
	cmdCopy.SetArgs(args)

	// Execute the command
	if err := cmdCopy.Execute(); err != nil {
		return err
	}

	return nil
}

// cloneCommand creates a fresh instance of the command for execution
func cloneCommand(cmd *cobra.Command) *cobra.Command {
	// Reset flags to their default values
	cmd.Flags().VisitAll(func(flag *pflag.Flag) {
		flag.Changed = false
	})
	cmd.PersistentFlags().VisitAll(func(flag *pflag.Flag) {
		flag.Changed = false
	})

	// Reset subcommand flags as well
	for _, subCmd := range cmd.Commands() {
		resetCommandFlags(subCmd)
	}

	return cmd
}

// resetCommandFlags recursively resets flags for a command and its subcommands
func resetCommandFlags(cmd *cobra.Command) {
	cmd.Flags().VisitAll(func(flag *pflag.Flag) {
		flag.Changed = false
	})
	cmd.PersistentFlags().VisitAll(func(flag *pflag.Flag) {
		flag.Changed = false
	})

	for _, subCmd := range cmd.Commands() {
		resetCommandFlags(subCmd)
	}
}

// parseCommandLine parses a command line into arguments, respecting quotes
func parseCommandLine(line string) ([]string, error) {
	var args []string
	var current strings.Builder
	inQuote := false
	quoteChar := rune(0)

	for i, ch := range line {
		switch {
		case (ch == '"' || ch == '\'') && !inQuote:
			// Start quote
			inQuote = true
			quoteChar = ch
		case ch == quoteChar && inQuote:
			// End quote
			inQuote = false
			quoteChar = 0
		case ch == ' ' && !inQuote:
			// Whitespace outside quotes - delimiter
			if current.Len() > 0 {
				args = append(args, current.String())
				current.Reset()
			}
		case ch == '\\' && i+1 < len(line):
			// Escape sequence
			next := rune(line[i+1])
			if next == '"' || next == '\'' || next == '\\' {
				current.WriteRune(next)
				// Skip next character (it's consumed by escape)
				continue
			} else {
				current.WriteRune(ch)
			}
		default:
			current.WriteRune(ch)
		}
	}

	// Add last argument
	if current.Len() > 0 {
		args = append(args, current.String())
	}

	if inQuote {
		return nil, fmt.Errorf("unterminated quote")
	}

	return args, nil
}

// buildCompleter creates a readline completer from the Cobra command tree
func buildCompleter(rootCmd *cobra.Command) *readline.PrefixCompleter {
	items := buildCompletionItems(rootCmd)
	return readline.NewPrefixCompleter(items...)
}

// buildCompletionItems recursively builds completion items from Cobra commands
func buildCompletionItems(cmd *cobra.Command) []readline.PrefixCompleterInterface {
	var items []readline.PrefixCompleterInterface

	// Add subcommands
	for _, subCmd := range cmd.Commands() {
		if subCmd.Hidden {
			continue
		}

		// Recursively build completion for subcommands
		subItems := buildCompletionItems(subCmd)

		items = append(items, readline.PcItem(subCmd.Name(),
			subItems...,
		))

		// Add aliases if any
		for _, alias := range subCmd.Aliases {
			items = append(items, readline.PcItem(alias,
				subItems...,
			))
		}
	}

	// Add flags for this command
	cmd.Flags().VisitAll(func(flag *pflag.Flag) {
		if !flag.Hidden {
			items = append(items, readline.PcItem("--"+flag.Name))
		}
	})

	// Add persistent flags
	cmd.PersistentFlags().VisitAll(func(flag *pflag.Flag) {
		if !flag.Hidden {
			items = append(items, readline.PcItem("--"+flag.Name))
		}
	})

	return items
}
