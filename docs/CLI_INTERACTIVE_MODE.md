# CLI Interactive Mode

## Overview

The reDB CLI now supports an interactive REPL (Read-Eval-Print Loop) mode that allows users to execute commands without repeatedly typing `./redb-cli` before each command. This mode provides a better user experience with features like command history, line editing, and tab completion.

## Starting Interactive Mode

Simply run the CLI without any arguments:

```bash
./redb-cli
```

You'll see a welcome message:

```
Welcome to reDB interactive mode!
Type 'help' for available commands, 'exit' or 'quit' to exit.
```

## Prompt Format

The prompt dynamically reflects your current authentication and profile status:

### Logged in with workspace
```
reDB node1 (john@workspace1)> 
```
Shows: `reDB <profile_name> (<username>@<workspace>)> `

### Profile active but not logged in
```
reDB node1 (not logged in)> 
```
Shows: `reDB <profile_name> (not logged in)> `

### No active profile
```
reDB (not logged in)> 
```
Shows: `reDB (not logged in)> `

## Features

### 1. Command Execution

Execute any CLI command without the `redb-cli` prefix:

```
reDB node1 (th@default)> databases list
reDB node1 (th@default)> workspaces list
reDB node1 (th@default)> auth status
```

### 2. Command History

- Use **↑** (Up Arrow) to navigate to previous commands
- Use **↓** (Down Arrow) to navigate to next commands
- Command history is persistent across sessions (stored in `~/.redb/cli_history`)

### 3. Tab Completion

Press **Tab** to autocomplete:
- Command names
- Subcommands
- Flag names (--flag)

Examples:
```
reDB> data<TAB>       → databases
reDB> databases li<TAB> → databases list
reDB> profiles --<TAB>  → shows available flags
```

### 4. Line Editing

- **Ctrl+A**: Move to beginning of line
- **Ctrl+E**: Move to end of line
- **Ctrl+K**: Delete from cursor to end of line
- **Ctrl+U**: Delete from cursor to beginning of line
- **Ctrl+W**: Delete word before cursor
- **←/→**: Move cursor left/right

### 5. Special Commands

#### Exit Commands
```
exit    # Exit interactive mode
quit    # Exit interactive mode (alias)
```

#### Clear Screen
```
clear   # Clear the terminal screen
```

#### Help
```
help            # Show available commands
help databases  # Show help for a specific command
```

### 6. Signal Handling

- **Ctrl+C**: Cancel current line (doesn't exit)
- **Ctrl+D**: Exit interactive mode (EOF)

## Examples

### Example Session 1: Database Management

```
$ ./redb-cli
Welcome to reDB interactive mode!
Type 'help' for available commands, 'exit' or 'quit' to exit.

reDB node1 (th@default)> databases list
Name   Type       Vendor   Instance      Status      Enabled
----   ----       ------   --------      ------      -------
my     mysql      custom   my_instance   connected   Yes
pg     postgres   custom   pg_instance   connected   Yes

reDB node1 (th@default)> databases show my --tables
[database details...]

reDB node1 (th@default)> exit
Goodbye!
```

### Example Session 2: Profile Management

```
$ ./redb-cli
Welcome to reDB interactive mode!
Type 'help' for available commands, 'exit' or 'quit' to exit.

reDB node1 (th@default)> profiles list
NAME            ENDPOINT                  TENANT          USERNAME             ACTIVE
-------------------------------------------------------------------------------------
node1           localhost:8081            default         admin                ✓
node2           localhost:8082            default         admin                

reDB node1 (th@default)> profiles activate node2
Successfully activated profile: node2

reDB node2 (admin@production)> auth status
[status information...]

reDB node2 (admin@production)> quit
Goodbye!
```

### Example Session 3: Not Logged In

```
$ ./redb-cli
Welcome to reDB interactive mode!
Type 'help' for available commands, 'exit' or 'quit' to exit.

reDB (not logged in)> profiles create local
[profile creation wizard...]

reDB local (not logged in)> auth login --profile local
Password: ****
Successfully logged in to profile 'local' as john

reDB local (john@default)> workspaces list
[workspace listing...]

reDB local (john@default)> exit
Goodbye!
```

## Non-Interactive Mode

The traditional CLI mode still works when you provide arguments:

```bash
./redb-cli databases list
./redb-cli --version
./redb-cli profiles list
```

This is useful for:
- Scripting
- CI/CD pipelines
- One-off commands
- Integration with other tools

## Technical Details

### Implementation

- **Interactive Library**: Uses `github.com/chzyer/readline` for advanced terminal features
- **Command Execution**: Integrates with Cobra command framework
- **History File**: `~/.redb/cli_history`
- **Profile Integration**: Reads from `~/.redb/profiles.json` and keyring

### Command Parsing

The interactive mode supports:
- Simple commands: `databases list`
- Commands with flags: `databases show my --tables`
- Quoted arguments: `databases create "My Database"`
- Escaped quotes: `databases create "My \"Special\" Database"`

### State Management

- Command state is reset between executions to prevent flag pollution
- Profile and authentication state is checked dynamically for each prompt
- Tab completion is generated from the current Cobra command tree

## Troubleshooting

### Tab completion not working
- Ensure your terminal supports ANSI escape codes
- Try in a different terminal emulator if issues persist

### History not saving
- Check permissions on `~/.redb/` directory
- Ensure `~/.redb/cli_history` is writable

### Prompt not updating after login
- The prompt updates on each new line
- Try pressing Enter to see the updated prompt

### Commands not executing
- Ensure the command works in non-interactive mode first
- Check for typos using tab completion
- Use `help` to see available commands

## Feedback and Contributions

The interactive mode is designed to improve the user experience. If you have suggestions for improvements or encounter issues, please:

1. Check existing documentation
2. Review the CLI reference: `docs/CLI_REFERENCE.md`
3. Report issues with detailed reproduction steps
4. Suggest enhancements with use cases

## See Also

- [CLI Reference](CLI_REFERENCE.md) - Complete command reference
- [Installation Guide](INSTALL.md) - Getting started with reDB
- [Architecture](ARCHITECTURE.md) - Understanding reDB's design

