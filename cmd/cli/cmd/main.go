package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/redbco/redb-open/cmd/cli/internal/auth"
	"github.com/redbco/redb-open/cmd/cli/internal/branches"
	"github.com/redbco/redb-open/cmd/cli/internal/commits"
	"github.com/redbco/redb-open/cmd/cli/internal/config"
	"github.com/redbco/redb-open/cmd/cli/internal/databases"
	"github.com/redbco/redb-open/cmd/cli/internal/environments"
	"github.com/redbco/redb-open/cmd/cli/internal/instances"
	"github.com/redbco/redb-open/cmd/cli/internal/mappings"
	"github.com/redbco/redb-open/cmd/cli/internal/regions"
	"github.com/redbco/redb-open/cmd/cli/internal/repos"
	"github.com/redbco/redb-open/cmd/cli/internal/tenants"
	"github.com/redbco/redb-open/cmd/cli/internal/users"
	"github.com/redbco/redb-open/cmd/cli/internal/workspaces"
)

var (
	configFile = flag.String("config", os.ExpandEnv("$HOME/.redb/config.yaml"), "Path to config file")
)

var version = "0.0.11"

func printUsage() {
	fmt.Println("Usage: redb-cli [command] [arguments]")
	fmt.Println("Version: ", version)
	fmt.Println("\nAuthentication Commands:")
	fmt.Println("  auth login                    Login to reDB (will prompt for username, password, hostname, and optionally tenant)")
	fmt.Println("  auth logout                   Logout from reDB")
	fmt.Println("  auth status                   Show authentication status")
	fmt.Println("  auth profile                  Show user profile")
	fmt.Println("  change password               Change user password")
	fmt.Println("  select workspace <name>       Select active workspace")
	fmt.Println("\nSession Management Commands:")
	fmt.Println("  auth sessions                 List all active sessions")
	fmt.Println("  auth logout-session <id>      Logout a specific session by ID")
	fmt.Println("  auth logout-all               Logout all sessions")
	fmt.Println("  auth logout-all --keep-current Logout all sessions except the current one")
	fmt.Println("  auth update-session <id> <name> Update session name")
	fmt.Println("\nRegion Commands:")
	fmt.Println("  list regions                  List all regions")
	fmt.Println("  show region <name>            Show region details")
	fmt.Println("  add region                    Add a new region")
	fmt.Println("  modify region <name>          Modify an existing region")
	fmt.Println("  delete region <name>          Delete a region")
	fmt.Println("\nWorkspace Commands:")
	fmt.Println("  list workspaces               List all workspaces")
	fmt.Println("  show workspace <name>         Show workspace details")
	fmt.Println("  add workspace                 Add a new workspace")
	fmt.Println("  modify workspace <name>       Modify an existing workspace")
	fmt.Println("  delete workspace <name>       Delete a workspace")
	fmt.Println("\nTenant Commands:")
	fmt.Println("  list tenants                  List all tenants")
	fmt.Println("  show tenant <id>              Show tenant details")
	fmt.Println("  add tenant                    Add a new tenant")
	fmt.Println("  modify tenant <id>            Modify an existing tenant")
	fmt.Println("  delete tenant <id>            Delete a tenant")
	fmt.Println("\nUser Commands:")
	fmt.Println("  list users                    List all users")
	fmt.Println("  show user <id>                Show user details")
	fmt.Println("  add user                      Add a new user")
	fmt.Println("  modify user <id>              Modify an existing user")
	fmt.Println("  delete user <id>              Delete a user")
	fmt.Println("\nEnvironment Commands:")
	fmt.Println("  list environments             List all environments")
	fmt.Println("  show environment <name>       Show environment details")
	fmt.Println("  add environment               Add a new environment")
	fmt.Println("  modify environment <name>     Modify an existing environment")
	fmt.Println("  delete environment <name>     Delete an environment")
	fmt.Println("\nInstance Commands:")
	fmt.Println("  list instances                List all instances")
	fmt.Println("  show instance <name>          Show instance details")
	fmt.Println("  connect instance              Connect a new instance")
	fmt.Println("  modify instance <name>        Modify an existing instance")
	fmt.Println("  reconnect instance <name>     Reconnect an instance")
	fmt.Println("  disconnect instance <name>    Disconnect an instance")
	fmt.Println("\nDatabase Commands:")
	fmt.Println("  list databases                List all databases")
	fmt.Println("  show database <name>          Show database details")
	fmt.Println("  create database               Create a new database")
	fmt.Println("  modify database <name>        Modify an existing database")
	fmt.Println("  delete database <name>        Delete a database")
	fmt.Println("  connect database <name>       Connect a database")
	fmt.Println("  reconnect database <name>     Reconnect a database")
	fmt.Println("  disconnect database <name>    Disconnect a database")
	fmt.Println("  wipe database <name>          Wipe a database")
	fmt.Println("  drop database <name>          Drop a database")
	fmt.Println("  clone table-data <mapping>    Clone data from one table to another using a mapping")
	fmt.Println("\nMapping Commands:")
	fmt.Println("  list mappings                 List all mappings")
	fmt.Println("  show mapping <name>           Show mapping details")
	fmt.Println("  add table-mapping             Add a new table mapping")
	fmt.Println("\nRepository Commands:")
	fmt.Println("  list repos                    List all repositories")
	fmt.Println("  show repo <name>              Show repository details")
	fmt.Println("  add repo                      Add a new repository")
	fmt.Println("  modify repo <name>            Modify an existing repository")
	fmt.Println("  delete repo <name>            Delete a repository")
	fmt.Println("  clone repo <name>             Clone an existing repository")
	fmt.Println("\nBranch Commands:")
	fmt.Println("  show branch <repo>/<branch>   Show branch details")
	fmt.Println("  modify branch <repo>/<branch> Modify an existing branch")
	fmt.Println("  attach branch <repo>/<branch> Attach a branch to a connected database")
	fmt.Println("  detach branch <repo>/<branch> Detach a branch from an attached database")
	fmt.Println("  delete branch <repo>/<branch> Delete a branch")
	fmt.Println("\nCommit Commands:")
	fmt.Println("  show commit <repo>/<branch>/<commit>   Show commit details")
	fmt.Println("  branch commit <repo>/<branch>/<commit> Branch a commit into a new branch")
	fmt.Println("  merge commit <repo>/<branch>/<commit>  Merge a commit to the parent branch")
	fmt.Println("  deploy commit <repo>/<branch>/<commit> Deploy the commit to the database attached to the branch")
	fmt.Println("\nUse 'redb-cli <command> --help' for more information about a command.")
}

func handleAuthCommands(args []string) error {
	if len(args) < 2 {
		fmt.Println("Error: auth command requires a subcommand")
		fmt.Println("\nAvailable auth commands:")
		fmt.Println("  login                   Login to reDB (prompts for username, password, hostname, and optionally tenant)")
		fmt.Println("  logout                  Logout from reDB")
		fmt.Println("  status                  Show authentication status")
		fmt.Println("  profile                 Show user profile")
		fmt.Println("  change password         Change user password")
		fmt.Println("  sessions                List all active sessions")
		fmt.Println("  logout-session <id>     Logout a specific session by ID")
		fmt.Println("  logout-all              Logout all sessions")
		fmt.Println("  update-session <id> <name> Update session name")
		return fmt.Errorf("missing auth subcommand")
	}

	switch args[1] {
	case "login":
		return auth.Login(args[2:])
	case "logout":
		return auth.Logout()
	case "status":
		return auth.Status()
	case "profile":
		return auth.Profile()
	case "password":
		return auth.ChangePassword(args[2:])
	case "sessions":
		return auth.ListSessions()
	case "logout-session":
		if len(args) < 3 {
			return fmt.Errorf("session ID is required for logout-session command")
		}
		return auth.LogoutSession(args[2])
	case "logout-all":
		// Check if --keep-current flag is provided
		excludeCurrent := false
		if len(args) > 2 && args[2] == "--keep-current" {
			excludeCurrent = true
		}
		return auth.LogoutAllSessions(excludeCurrent)
	case "update-session":
		if len(args) < 4 {
			return fmt.Errorf("session ID and new name are required for update-session command")
		}
		return auth.UpdateSessionName(args[2], args[3])
	default:
		return fmt.Errorf("unknown auth command: %s", args[1])
	}
}

func handleRegionCommands(cmd string, args []string) error {
	switch {
	case cmd == "list regions":
		return regions.ListRegions()
	case strings.HasPrefix(cmd, "show region "):
		regionName := cmd[12:] // Remove "show region " prefix
		if regionName == "" {
			return fmt.Errorf("region name is required")
		}
		return regions.ShowRegion(regionName)
	case cmd == "add region":
		return regions.AddRegion(args)
	case strings.HasPrefix(cmd, "modify region "):
		regionName := cmd[14:] // Remove "modify region " prefix
		if regionName == "" {
			return fmt.Errorf("region name is required")
		}
		return regions.ModifyRegion(regionName, args)
	case strings.HasPrefix(cmd, "delete region "):
		regionName := cmd[14:] // Remove "delete region " prefix
		if regionName == "" {
			return fmt.Errorf("region name is required")
		}
		return regions.DeleteRegion(regionName, args)
	default:
		return fmt.Errorf("unknown region command: %s", cmd)
	}
}

func handleWorkspaceCommands(cmd string, args []string) error {
	switch {
	case cmd == "list workspaces":
		return workspaces.ListWorkspaces()
	case strings.HasPrefix(cmd, "show workspace "):
		workspaceName := cmd[15:] // Remove "show workspace " prefix
		if workspaceName == "" {
			return fmt.Errorf("workspace name is required")
		}
		return workspaces.ShowWorkspace(workspaceName)
	case cmd == "add workspace":
		return workspaces.AddWorkspace(args)
	case strings.HasPrefix(cmd, "modify workspace "):
		workspaceName := cmd[17:] // Remove "modify workspace " prefix
		if workspaceName == "" {
			return fmt.Errorf("workspace name is required")
		}
		return workspaces.ModifyWorkspace(workspaceName, args)
	case strings.HasPrefix(cmd, "delete workspace "):
		workspaceName := cmd[17:] // Remove "delete workspace " prefix
		if workspaceName == "" {
			return fmt.Errorf("workspace name is required")
		}
		return workspaces.DeleteWorkspace(workspaceName, args)
	default:
		return fmt.Errorf("unknown workspace command: %s", cmd)
	}
}

func handleTenantCommands(cmd string, args []string) error {
	switch {
	case cmd == "list tenants":
		return tenants.ListTenants()
	case strings.HasPrefix(cmd, "show tenant "):
		tenantID := cmd[12:] // Remove "show tenant " prefix
		if tenantID == "" {
			return fmt.Errorf("tenant ID is required")
		}
		return tenants.ShowTenant(tenantID)
	case cmd == "add tenant":
		return tenants.AddTenant(args)
	case strings.HasPrefix(cmd, "modify tenant "):
		tenantID := cmd[14:] // Remove "modify tenant " prefix
		if tenantID == "" {
			return fmt.Errorf("tenant ID is required")
		}
		return tenants.ModifyTenant(tenantID, args)
	case strings.HasPrefix(cmd, "delete tenant "):
		tenantID := cmd[14:] // Remove "delete tenant " prefix
		if tenantID == "" {
			return fmt.Errorf("tenant ID is required")
		}
		return tenants.DeleteTenant(tenantID, args)
	default:
		return fmt.Errorf("unknown tenant command: %s", cmd)
	}
}

func handleUserCommands(cmd string, args []string) error {
	switch {
	case cmd == "list users":
		return users.ListUsers()
	case strings.HasPrefix(cmd, "show user "):
		userID := cmd[10:] // Remove "show user " prefix
		if userID == "" {
			return fmt.Errorf("user ID is required")
		}
		return users.ShowUser(userID)
	case cmd == "add user":
		return users.AddUser(args)
	case strings.HasPrefix(cmd, "modify user "):
		userID := cmd[12:] // Remove "modify user " prefix
		if userID == "" {
			return fmt.Errorf("user ID is required")
		}
		return users.ModifyUser(userID, args)
	case strings.HasPrefix(cmd, "delete user "):
		userID := cmd[12:] // Remove "delete user " prefix
		if userID == "" {
			return fmt.Errorf("user ID is required")
		}
		return users.DeleteUser(userID, args)
	default:
		return fmt.Errorf("unknown user command: %s", cmd)
	}
}

func handleEnvironmentCommands(cmd string, args []string) error {
	switch {
	case cmd == "list environments":
		return environments.ListEnvironments()
	case strings.HasPrefix(cmd, "show environment "):
		environmentName := cmd[17:] // Remove "show environment " prefix
		if environmentName == "" {
			return fmt.Errorf("environment name is required")
		}
		return environments.ShowEnvironment(environmentName)
	case cmd == "add environment":
		return environments.AddEnvironment(args)
	case strings.HasPrefix(cmd, "modify environment "):
		environmentName := cmd[19:] // Remove "modify environment " prefix
		if environmentName == "" {
			return fmt.Errorf("environment name is required")
		}
		return environments.ModifyEnvironment(environmentName, args)
	case strings.HasPrefix(cmd, "delete environment "):
		environmentName := cmd[19:] // Remove "delete environment " prefix
		if environmentName == "" {
			return fmt.Errorf("environment name is required")
		}
		return environments.DeleteEnvironment(environmentName, args)
	default:
		return fmt.Errorf("unknown environment command: %s", cmd)
	}
}

func handleInstanceCommands(cmd string, args []string) error {
	switch {
	case cmd == "list instances":
		return instances.ListInstances()
	case strings.HasPrefix(cmd, "show instance "):
		instanceName := cmd[13:] // Remove "show instance " prefix
		if instanceName == "" {
			return fmt.Errorf("instance name is required")
		}
		return instances.ShowInstance(instanceName)
	case cmd == "connect instance":
		return instances.ConnectInstance(args)
	case strings.HasPrefix(cmd, "modify instance "):
		instanceName := cmd[15:] // Remove "modify instance " prefix
		if instanceName == "" {
			return fmt.Errorf("instance name is required")
		}
		return instances.ModifyInstance(instanceName, args)
	case strings.HasPrefix(cmd, "disconnect instance "):
		instanceName := cmd[20:] // Remove "disconnect instance " prefix
		if instanceName == "" {
			return fmt.Errorf("instance name is required")
		}
		return instances.DisconnectInstance(instanceName, args)
	case strings.HasPrefix(cmd, "reconnect instance "):
		instanceName := cmd[19:] // Remove "reconnect instance " prefix
		if instanceName == "" {
			return fmt.Errorf("instance name is required")
		}
		return instances.ReconnectInstance(instanceName, args)
	default:
		return fmt.Errorf("unknown instance command: %s", cmd)
	}
}

func handleDatabaseCommands(cmd string, args []string) error {
	switch {
	case cmd == "list databases":
		return databases.ListDatabases()
	case strings.HasPrefix(cmd, "show database "):
		if len(args) < 3 {
			return fmt.Errorf("database name is required")
		}
		databaseName := args[2]
		extraArgs := args[3:]
		return databases.ShowDatabase(databaseName, extraArgs)
	case cmd == "create database":
		return databases.CreateDatabase(args)
	case strings.HasPrefix(cmd, "modify database "):
		databaseName := cmd[17:] // Remove "modify database " prefix
		if databaseName == "" {
			return fmt.Errorf("database name is required")
		}
		return databases.ModifyDatabase(databaseName, args)
	case strings.HasPrefix(cmd, "delete database "):
		databaseName := cmd[17:] // Remove "delete database " prefix
		if databaseName == "" {
			return fmt.Errorf("database name is required")
		}
		return databases.DeleteDatabase(databaseName, args)
	case strings.HasPrefix(cmd, "connect database "):
		databaseName := cmd[17:] // Remove "connect database " prefix
		if databaseName == "" {
			return fmt.Errorf("database name is required")
		}
		return databases.ConnectDatabase(databaseName, args)
	case strings.HasPrefix(cmd, "reconnect database "):
		databaseName := cmd[19:] // Remove "reconnect database " prefix
		if databaseName == "" {
			return fmt.Errorf("database name is required")
		}
		return databases.ReconnectDatabase(databaseName, args)
	case strings.HasPrefix(cmd, "disconnect database "):
		databaseName := cmd[20:] // Remove "disconnect database " prefix
		if databaseName == "" {
			return fmt.Errorf("database name is required")
		}
		return databases.DisconnectDatabase(databaseName, args)
	case strings.HasPrefix(cmd, "wipe database "):
		databaseName := cmd[14:] // Remove "wipe database " prefix
		if databaseName == "" {
			return fmt.Errorf("database name is required")
		}
		return databases.WipeDatabase(databaseName, args)
	case strings.HasPrefix(cmd, "drop database "):
		databaseName := cmd[14:] // Remove "drop database " prefix
		if databaseName == "" {
			return fmt.Errorf("database name is required")
		}
		return databases.DropDatabase(databaseName, args)
	case strings.HasPrefix(cmd, "clone table-data "):
		mappingName := cmd[16:] // Remove "clone table-data " prefix
		if mappingName == "" {
			return fmt.Errorf("mapping name is required")
		}
		return databases.CloneTableData(mappingName, args)
	default:
		return fmt.Errorf("unknown database command: %s", cmd)
	}
}

func handleSelectCommands(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("select command requires a subcommand")
	}

	switch args[1] {
	case "workspace":
		if len(args) < 3 {
			return fmt.Errorf("workspace name is required")
		}
		return auth.SelectWorkspace(args[2])
	default:
		return fmt.Errorf("unknown select command: %s", args[1])
	}
}

func handleRepoCommands(cmd string, args []string) error {
	switch {
	case cmd == "list repos":
		return repos.ListRepos()
	case strings.HasPrefix(cmd, "show repo "):
		repoName := cmd[10:] // Remove "show repo " prefix
		if repoName == "" {
			return fmt.Errorf("repository name is required")
		}
		return repos.ShowRepo(repoName)
	case cmd == "add repo":
		return repos.AddRepo(args)
	case strings.HasPrefix(cmd, "modify repo "):
		repoName := cmd[12:] // Remove "modify repo " prefix
		if repoName == "" {
			return fmt.Errorf("repository name is required")
		}
		return repos.ModifyRepo(repoName, args)
	case strings.HasPrefix(cmd, "delete repo "):
		repoName := cmd[12:] // Remove "delete repo " prefix
		if repoName == "" {
			return fmt.Errorf("repository name is required")
		}
		return repos.DeleteRepo(repoName, args)
	case strings.HasPrefix(cmd, "clone repo "):
		repoName := cmd[11:] // Remove "clone repo " prefix
		if repoName == "" {
			return fmt.Errorf("repository name is required")
		}
		return repos.CloneRepo(repoName, args)
	default:
		return fmt.Errorf("unknown repository command: %s", cmd)
	}
}

func handleBranchCommands(cmd string, args []string) error {
	switch {
	case strings.HasPrefix(cmd, "show branch "):
		repoBranchStr := cmd[12:] // Remove "show branch " prefix
		if repoBranchStr == "" {
			return fmt.Errorf("repository/branch is required")
		}
		return branches.ShowBranch(repoBranchStr)
	case strings.HasPrefix(cmd, "modify branch "):
		repoBranchStr := cmd[14:] // Remove "modify branch " prefix
		if repoBranchStr == "" {
			return fmt.Errorf("repository/branch is required")
		}
		return branches.ModifyBranch(repoBranchStr, args)
	case strings.HasPrefix(cmd, "attach branch "):
		repoBranchStr := cmd[14:] // Remove "attach branch " prefix
		if repoBranchStr == "" {
			return fmt.Errorf("repository/branch is required")
		}
		return branches.AttachBranch(repoBranchStr, args)
	case strings.HasPrefix(cmd, "detach branch "):
		repoBranchStr := cmd[14:] // Remove "detach branch " prefix
		if repoBranchStr == "" {
			return fmt.Errorf("repository/branch is required")
		}
		return branches.DetachBranch(repoBranchStr, args)
	case strings.HasPrefix(cmd, "delete branch "):
		repoBranchStr := cmd[14:] // Remove "delete branch " prefix
		if repoBranchStr == "" {
			return fmt.Errorf("repository/branch is required")
		}
		return branches.DeleteBranch(repoBranchStr, args)
	default:
		return fmt.Errorf("unknown branch command: %s", cmd)
	}
}

func handleCommitCommands(cmd string, args []string) error {
	switch {
	case strings.HasPrefix(cmd, "show commit "):
		repoBranchCommitStr := cmd[12:] // Remove "show commit " prefix
		if repoBranchCommitStr == "" {
			return fmt.Errorf("repository/branch/commit is required")
		}
		return commits.ShowCommit(repoBranchCommitStr)
	case strings.HasPrefix(cmd, "branch commit "):
		repoBranchCommitStr := cmd[14:] // Remove "branch commit " prefix
		if repoBranchCommitStr == "" {
			return fmt.Errorf("repository/branch/commit is required")
		}
		return commits.BranchCommit(repoBranchCommitStr, args)
	case strings.HasPrefix(cmd, "merge commit "):
		repoBranchCommitStr := cmd[13:] // Remove "merge commit " prefix
		if repoBranchCommitStr == "" {
			return fmt.Errorf("repository/branch/commit is required")
		}
		return commits.MergeCommit(repoBranchCommitStr, args)
	case strings.HasPrefix(cmd, "deploy commit "):
		repoBranchCommitStr := cmd[14:] // Remove "deploy commit " prefix
		if repoBranchCommitStr == "" {
			return fmt.Errorf("repository/branch/commit is required")
		}
		return commits.DeployCommit(repoBranchCommitStr, args)
	default:
		return fmt.Errorf("unknown commit command: %s", cmd)
	}
}

func handleMappingCommands(cmd string, args []string) error {
	switch {
	case cmd == "list mappings":
		return mappings.ListMappings()
	case strings.HasPrefix(cmd, "show mapping "):
		mappingName := cmd[13:] // Remove "show mapping " prefix
		if mappingName == "" {
			return fmt.Errorf("mapping name is required")
		}
		return mappings.ShowMapping(mappingName)
	case cmd == "add table-mapping":
		return mappings.AddTableMapping(args)
	default:
		return fmt.Errorf("unknown mapping command: %s", cmd)
	}
}

func main() {
	flag.Parse()

	// Initialize configuration
	if err := config.Init(*configFile); err != nil {
		fmt.Printf("Error initializing config: %v\n", err)
		os.Exit(1)
	}

	args := flag.Args()
	if len(args) == 0 {
		printUsage()
		return
	}

	// Build command string for pattern matching
	cmd := strings.Join(args, " ")
	var err error

	// Handle different command categories
	switch {
	case args[0] == "auth":
		err = handleAuthCommands(args)
	case strings.HasPrefix(cmd, "change password"):
		err = handleAuthCommands(args)
	case args[0] == "select":
		err = handleSelectCommands(args)
	case strings.HasPrefix(cmd, "show workspace"):
		err = handleWorkspaceCommands(cmd, args)
	case cmd == "list workspaces":
		err = handleWorkspaceCommands(cmd, args)
	case cmd == "add workspace":
		err = handleWorkspaceCommands(cmd, args)
	case strings.HasPrefix(cmd, "modify workspace "):
		err = handleWorkspaceCommands(cmd, args)
	case strings.HasPrefix(cmd, "delete workspace "):
		err = handleWorkspaceCommands(cmd, args)
	case strings.HasPrefix(cmd, "show region"):
		err = handleRegionCommands(cmd, args)
	case cmd == "list regions":
		err = handleRegionCommands(cmd, args)
	case cmd == "add region":
		err = handleRegionCommands(cmd, args)
	case strings.HasPrefix(cmd, "modify region "):
		err = handleRegionCommands(cmd, args)
	case strings.HasPrefix(cmd, "delete region "):
		err = handleRegionCommands(cmd, args)
	case strings.HasPrefix(cmd, "show environment"):
		err = handleEnvironmentCommands(cmd, args)
	case cmd == "list environments":
		err = handleEnvironmentCommands(cmd, args)
	case cmd == "add environment":
		err = handleEnvironmentCommands(cmd, args)
	case strings.HasPrefix(cmd, "modify environment "):
		err = handleEnvironmentCommands(cmd, args)
	case strings.HasPrefix(cmd, "delete environment "):
		err = handleEnvironmentCommands(cmd, args)
	case strings.HasPrefix(cmd, "show user"):
		err = handleUserCommands(cmd, args)
	case cmd == "list users":
		err = handleUserCommands(cmd, args)
	case cmd == "add user":
		err = handleUserCommands(cmd, args)
	case strings.HasPrefix(cmd, "modify user "):
		err = handleUserCommands(cmd, args)
	case strings.HasPrefix(cmd, "delete user "):
		err = handleUserCommands(cmd, args)
	case strings.HasPrefix(cmd, "show tenant"):
		err = handleTenantCommands(cmd, args)
	case cmd == "list tenants":
		err = handleTenantCommands(cmd, args)
	case cmd == "add tenant":
		err = handleTenantCommands(cmd, args)
	case strings.HasPrefix(cmd, "modify tenant "):
		err = handleTenantCommands(cmd, args)
	case strings.HasPrefix(cmd, "delete tenant "):
		err = handleTenantCommands(cmd, args)
	case strings.HasPrefix(cmd, "show instance "):
		err = handleInstanceCommands(cmd, args)
	case cmd == "list instances":
		err = handleInstanceCommands(cmd, args)
	case cmd == "connect instance":
		err = handleInstanceCommands(cmd, args)
	case strings.HasPrefix(cmd, "modify instance "):
		err = handleInstanceCommands(cmd, args)
	case strings.HasPrefix(cmd, "reconnect instance "):
		err = handleInstanceCommands(cmd, args)
	case strings.HasPrefix(cmd, "disconnect instance "):
		err = handleInstanceCommands(cmd, args)
	case strings.HasPrefix(cmd, "show database "):
		err = handleDatabaseCommands(cmd, args)
	case cmd == "list databases":
		err = handleDatabaseCommands(cmd, args)
	case cmd == "create database":
		err = handleDatabaseCommands(cmd, args)
	case strings.HasPrefix(cmd, "modify database "):
		err = handleDatabaseCommands(cmd, args)
	case strings.HasPrefix(cmd, "delete database "):
		err = handleDatabaseCommands(cmd, args)
	case strings.HasPrefix(cmd, "connect database "):
		err = handleDatabaseCommands(cmd, args)
	case strings.HasPrefix(cmd, "reconnect database "):
		err = handleDatabaseCommands(cmd, args)
	case strings.HasPrefix(cmd, "disconnect database "):
		err = handleDatabaseCommands(cmd, args)
	case strings.HasPrefix(cmd, "wipe database "):
		err = handleDatabaseCommands(cmd, args)
	case strings.HasPrefix(cmd, "drop database "):
		err = handleDatabaseCommands(cmd, args)
	case strings.HasPrefix(cmd, "clone table-data "):
		err = handleDatabaseCommands(cmd, args)
	case cmd == "list mappings":
		err = handleMappingCommands(cmd, args)
	case strings.HasPrefix(cmd, "show mapping "):
		err = handleMappingCommands(cmd, args)
	case cmd == "add table-mapping":
		err = handleMappingCommands(cmd, args)
	case cmd == "list repos":
		err = handleRepoCommands(cmd, args)
	case cmd == "add repo":
		err = handleRepoCommands(cmd, args)
	case strings.HasPrefix(cmd, "modify repo "):
		err = handleRepoCommands(cmd, args)
	case strings.HasPrefix(cmd, "delete repo "):
		err = handleRepoCommands(cmd, args)
	case strings.HasPrefix(cmd, "clone repo "):
		err = handleRepoCommands(cmd, args)
	case strings.HasPrefix(cmd, "modify branch "):
		err = handleBranchCommands(cmd, args)
	case strings.HasPrefix(cmd, "attach branch "):
		err = handleBranchCommands(cmd, args)
	case strings.HasPrefix(cmd, "detach branch "):
		err = handleBranchCommands(cmd, args)
	case strings.HasPrefix(cmd, "delete branch "):
		err = handleBranchCommands(cmd, args)
	case strings.HasPrefix(cmd, "branch commit "):
		err = handleCommitCommands(cmd, args)
	case strings.HasPrefix(cmd, "merge commit "):
		err = handleCommitCommands(cmd, args)
	case strings.HasPrefix(cmd, "deploy commit "):
		err = handleCommitCommands(cmd, args)
	case strings.HasPrefix(cmd, "show repo "):
		err = handleRepoCommands(cmd, args)
	case strings.HasPrefix(cmd, "show branch "):
		err = handleBranchCommands(cmd, args)
	case strings.HasPrefix(cmd, "show commit "):
		err = handleCommitCommands(cmd, args)
	default:
		fmt.Printf("Unknown command: %s\n", cmd)
		printUsage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}
