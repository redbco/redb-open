package databases

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"

	"github.com/redbco/redb-open/cmd/cli/internal/config"
	"github.com/redbco/redb-open/cmd/cli/internal/httpclient"
	"golang.org/x/term"
)

type Database struct {
	TenantID              string   `json:"tenant_id"`
	WorkspaceID           string   `json:"workspace_id"`
	EnvironmentID         string   `json:"environment_id"`
	ConnectedToNodeID     string   `json:"connected_to_node_id"`
	InstanceID            string   `json:"instance_id"`
	InstanceName          string   `json:"instance_name"`
	DatabaseID            string   `json:"database_id"`
	DatabaseName          string   `json:"database_name"`
	DatabaseDescription   string   `json:"database_description"`
	DatabaseType          string   `json:"database_type"`
	DatabaseVendor        string   `json:"database_vendor"`
	DatabaseVersion       string   `json:"database_version"`
	DatabaseUsername      string   `json:"database_username"`
	DatabasePassword      string   `json:"database_password"`
	DatabaseDBName        string   `json:"database_db_name"`
	DatabaseEnabled       bool     `json:"database_enabled"`
	PolicyIDs             []string `json:"policy_ids"`
	OwnerID               string   `json:"owner_id"`
	DatabaseStatusMessage string   `json:"database_status_message"`
	Status                string   `json:"status"`
	Created               string   `json:"created"`
	Updated               string   `json:"updated"`
	DatabaseSchema        string   `json:"database_schema"`
	DatabaseTables        string   `json:"database_tables"`
	InstanceHost          string   `json:"instance_host"`
	InstancePort          int32    `json:"instance_port"`
	InstanceSSLMode       string   `json:"instance_ssl_mode"`
	InstanceSSLCert       string   `json:"instance_ssl_cert"`
	InstanceSSLKey        string   `json:"instance_ssl_key"`
	InstanceSSLRootCert   string   `json:"instance_ssl_root_cert"`
	InstanceSSL           bool     `json:"instance_ssl"`
	InstanceStatusMessage string   `json:"instance_status_message"`
	InstanceStatus        string   `json:"instance_status"`
}

type CreateDatabaseRequest struct {
	InstanceName        string  `json:"instance_name"`
	DatabaseName        string  `json:"database_name"`
	DatabaseDescription string  `json:"database_description"`
	DBName              string  `json:"db_name"`
	CreateWithUser      *bool   `json:"create_with_user,omitempty"`
	DatabaseUsername    *string `json:"database_username,omitempty"`
	DatabasePassword    *string `json:"database_password,omitempty"`
}

// Schema data structures
type SchemaColumn struct {
	Name            string `json:"name"`
	IsArray         bool   `json:"isArray"`
	DataType        string `json:"dataType"`
	IsUnique        bool   `json:"isUnique"`
	IsNullable      bool   `json:"isNullable"`
	IsGenerated     bool   `json:"isGenerated"`
	IsPrimaryKey    bool   `json:"isPrimaryKey"`
	ColumnDefault   string `json:"columnDefault"`
	IsAutoIncrement bool   `json:"isAutoIncrement"`
	VarcharLength   *int   `json:"varcharLength,omitempty"`
}

type SchemaTable struct {
	Name        string         `json:"name"`
	Schema      string         `json:"schema"`
	Columns     []SchemaColumn `json:"columns"`
	Indexes     interface{}    `json:"indexes"`
	TableType   string         `json:"tableType"`
	PrimaryKey  interface{}    `json:"primaryKey"`
	Constraints interface{}    `json:"constraints"`
}

type SchemaData struct {
	Tables     []SchemaTable `json:"tables"`
	Schemas    interface{}   `json:"schemas"`
	Triggers   interface{}   `json:"triggers"`
	EnumTypes  interface{}   `json:"enumTypes"`
	Functions  interface{}   `json:"functions"`
	Sequences  interface{}   `json:"sequences"`
	Extensions interface{}   `json:"extensions"`
}

// Tables data structures
type TableColumn struct {
	Name                  string  `json:"name"`
	Type                  string  `json:"type"`
	DataCategory          string  `json:"data_category,omitempty"`
	ColumnDefault         string  `json:"column_default,omitempty"`
	IsPrimaryKey          bool    `json:"is_primary_key,omitempty"`
	IsAutoIncrement       bool    `json:"is_auto_increment,omitempty"`
	IsPrivilegedData      bool    `json:"is_privileged_data,omitempty"`
	PrivilegedConfidence  float64 `json:"privileged_confidence,omitempty"`
	PrivilegedDescription string  `json:"privileged_description,omitempty"`
	VarcharLength         *int    `json:"varchar_length,omitempty"`
	IsNullable            bool    `json:"is_nullable,omitempty"`
}

type ClassificationScore struct {
	Score    float64 `json:"score"`
	Reason   string  `json:"reason"`
	Category string  `json:"category"`
}

type TableData struct {
	Name                     string                `json:"name"`
	Engine                   string                `json:"engine"`
	Schema                   string                `json:"schema"`
	Columns                  []TableColumn         `json:"columns"`
	TableType                string                `json:"table_type"`
	PrimaryCategory          string                `json:"primary_category"`
	ClassificationScores     []ClassificationScore `json:"classification_scores"`
	ClassificationConfidence float64               `json:"classification_confidence"`
}

type TablesData struct {
	Tables []TableData `json:"tables"`
}

// formatSchemaData formats the schema data as a well-structured table
func formatSchemaData(schemaJSON string) error {
	if schemaJSON == "" {
		fmt.Println("No schema data available")
		return nil
	}

	// Unescape the JSON string (remove the extra quotes and escape characters)
	unescaped := strings.Trim(schemaJSON, "\"")
	unescaped = strings.ReplaceAll(unescaped, "\\\"", "\"")
	unescaped = strings.ReplaceAll(unescaped, "\\\\", "\\")

	var schemaData SchemaData
	if err := json.Unmarshal([]byte(unescaped), &schemaData); err != nil {
		return fmt.Errorf("failed to parse schema JSON: %v", err)
	}

	if len(schemaData.Tables) == 0 {
		fmt.Println("No tables found in schema")
		return nil
	}

	fmt.Println("\nDatabase Schema")
	fmt.Println(strings.Repeat("=", 50))

	for _, table := range schemaData.Tables {
		fmt.Printf("\nTable: %s (Schema: %s, Type: %s)\n", table.Name, table.Schema, table.TableType)
		fmt.Println(strings.Repeat("-", 60))

		if len(table.Columns) == 0 {
			fmt.Println("No columns found")
			continue
		}

		// Create table for columns
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "Column\tType\tNullable\tPrimary Key\tAuto Inc\tDefault\tUnique")
		fmt.Fprintln(w, "------\t----\t--------\t-----------\t--------\t-------\t-----")

		for _, col := range table.Columns {
			nullable := "No"
			if col.IsNullable {
				nullable = "Yes"
			}

			primaryKey := "No"
			if col.IsPrimaryKey {
				primaryKey = "Yes"
			}

			autoInc := "No"
			if col.IsAutoIncrement {
				autoInc = "Yes"
			}

			unique := "No"
			if col.IsUnique {
				unique = "Yes"
			}

			dataType := col.DataType
			if col.VarcharLength != nil {
				dataType = fmt.Sprintf("%s(%d)", dataType, *col.VarcharLength)
			}

			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				col.Name,
				dataType,
				nullable,
				primaryKey,
				autoInc,
				col.ColumnDefault,
				unique)
		}
		_ = w.Flush()
		fmt.Println()
	}

	return nil
}

// formatTablesData formats the tables data as a well-structured table
func formatTablesData(tablesJSON string) error {
	if tablesJSON == "" {
		fmt.Println("No tables data available")
		return nil
	}

	// Unescape the JSON string (remove the extra quotes and escape characters)
	unescaped := strings.Trim(tablesJSON, "\"")
	unescaped = strings.ReplaceAll(unescaped, "\\\"", "\"")
	unescaped = strings.ReplaceAll(unescaped, "\\\\", "\\")

	var tablesData TablesData
	if err := json.Unmarshal([]byte(unescaped), &tablesData); err != nil {
		return fmt.Errorf("failed to parse tables JSON: %v", err)
	}

	if len(tablesData.Tables) == 0 {
		fmt.Println("No tables found")
		return nil
	}

	fmt.Println("\nDatabase Tables")
	fmt.Println(strings.Repeat("=", 50))

	for _, table := range tablesData.Tables {
		fmt.Printf("\nTable: %s (Engine: %s, Schema: %s, Type: %s)\n",
			table.Name, table.Engine, table.Schema, table.TableType)
		fmt.Printf("Primary Category: %s (Confidence: %.2f)\n",
			table.PrimaryCategory, table.ClassificationConfidence)
		fmt.Println(strings.Repeat("-", 80))

		if len(table.Columns) == 0 {
			fmt.Println("No columns found")
			continue
		}

		// Create table for columns
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "Column\tType\tData Category\tPrimary Key\tAuto Inc\tPrivileged\tConfidence")
		fmt.Fprintln(w, "------\t----\t-------------\t-----------\t--------\t----------\t----------")

		for _, col := range table.Columns {
			primaryKey := "No"
			if col.IsPrimaryKey {
				primaryKey = "Yes"
			}

			autoInc := "No"
			if col.IsAutoIncrement {
				autoInc = "Yes"
			}

			privileged := "No"
			// If confidence is greater than 0.7, set to yes
			if col.IsPrivilegedData && col.PrivilegedConfidence > 0.7 {
				privileged = "Yes"
			}

			dataType := col.Type
			if col.VarcharLength != nil {
				dataType = fmt.Sprintf("%s(%d)", dataType, *col.VarcharLength)
			}

			confidence := "-"
			if col.IsPrivilegedData {
				confidence = fmt.Sprintf("%.2f", col.PrivilegedConfidence)
			}

			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				col.Name,
				dataType,
				col.DataCategory,
				primaryKey,
				autoInc,
				privileged,
				confidence)
		}
		_ = w.Flush()

		// Show classification scores if available
		if len(table.ClassificationScores) > 0 {
			fmt.Println("\nClassification Scores:")
			scoreW := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
			fmt.Fprintln(scoreW, "Category\tScore\tReason")
			fmt.Fprintln(scoreW, "--------\t-----\t------")
			for _, score := range table.ClassificationScores {
				fmt.Fprintf(scoreW, "%s\t%.2f\t%s\n", score.Category, score.Score, score.Reason)
			}
			_ = scoreW.Flush()
		}
		fmt.Println()
	}

	return nil
}

// ListDatabases lists all databases
func ListDatabases() error {
	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	username, err := config.GetUsername()
	if err != nil {
		fmt.Println("Authentication Status: Not logged in")
		fmt.Println("No user credentials found in keyring")
		return nil
	}

	workspaceName, err := config.GetWorkspaceWithError(username)
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/workspaces/%s/databases", tenantURL, workspaceName)

	var databasesResponse struct {
		Databases []Database `json:"databases"`
	}
	if err := client.Get(url, &databasesResponse, true); err != nil {
		return fmt.Errorf("failed to list databases: %v", err)
	}

	if len(databasesResponse.Databases) == 0 {
		fmt.Println("No databases found.")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Println()
	fmt.Fprintln(w, "Name\tType\tVendor\tInstance\tStatus\tEnabled")
	fmt.Fprintln(w, "----\t----\t------\t--------\t------\t-------")
	for _, db := range databasesResponse.Databases {
		enabled := "Yes"
		if !db.DatabaseEnabled {
			enabled = "No"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
			db.DatabaseName,
			db.DatabaseType,
			db.DatabaseVendor,
			db.InstanceName,
			db.Status,
			enabled)
	}
	_ = w.Flush()
	fmt.Println()
	return nil
}

// ShowDatabase displays details of a specific database
func ShowDatabase(databaseName string, args []string) error {
	databaseName = strings.TrimSpace(databaseName)
	if databaseName == "" {
		return fmt.Errorf("database name is required")
	}
	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}
	username, err := config.GetUsername()
	if err != nil {
		fmt.Println("Authentication Status: Not logged in")
		fmt.Println("No user credentials found in keyring")
		return nil
	}
	workspaceName, err := config.GetWorkspaceWithError(username)
	if err != nil {
		return err
	}
	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/workspaces/%s/databases/%s", tenantURL, workspaceName, databaseName)
	var databaseResponse struct {
		Database Database `json:"database"`
	}
	if err := client.Get(url, &databaseResponse, true); err != nil {
		return fmt.Errorf("failed to get database details: %v", err)
	}
	db := databaseResponse.Database
	fmt.Println()
	fmt.Printf("Database Details for '%s'\n", db.DatabaseName)
	fmt.Println(strings.Repeat("=", 50))
	fmt.Printf("ID:                    %s\n", db.DatabaseID)
	fmt.Printf("Name:                  %s\n", db.DatabaseName)
	fmt.Printf("Description:           %s\n", db.DatabaseDescription)
	fmt.Printf("Type:                  %s\n", db.DatabaseType)
	fmt.Printf("Vendor:                %s\n", db.DatabaseVendor)
	fmt.Printf("Version:               %s\n", db.DatabaseVersion)
	fmt.Printf("Username:              %s\n", db.DatabaseUsername)
	fmt.Printf("DB Name:               %s\n", db.DatabaseDBName)
	fmt.Printf("Enabled:               %t\n", db.DatabaseEnabled)
	fmt.Printf("Status:                %s\n", db.Status)
	fmt.Printf("Status Message:        %s\n", db.DatabaseStatusMessage)
	fmt.Printf("Owner ID:              %s\n", db.OwnerID)
	fmt.Printf("Tenant ID:             %s\n", db.TenantID)
	fmt.Printf("Workspace ID:          %s\n", db.WorkspaceID)
	if db.EnvironmentID != "" {
		fmt.Printf("Environment ID:        %s\n", db.EnvironmentID)
	}
	fmt.Printf("Connected to Node ID:  %s\n", db.ConnectedToNodeID)
	fmt.Printf("Instance ID:           %s\n", db.InstanceID)
	fmt.Printf("Instance Name:         %s\n", db.InstanceName)
	fmt.Printf("Instance Host:         %s\n", db.InstanceHost)
	fmt.Printf("Instance Port:         %d\n", db.InstancePort)
	fmt.Printf("Instance SSL:          %t\n", db.InstanceSSL)
	fmt.Printf("Instance SSL Mode:     %s\n", db.InstanceSSLMode)
	fmt.Printf("Instance Status:       %s\n", db.InstanceStatus)
	fmt.Printf("Instance Status:       %s\n", db.InstanceStatusMessage)
	if db.InstanceSSLCert != "" {
		fmt.Printf("Instance SSL Certificate: %s\n", db.InstanceSSLCert)
	}
	if db.InstanceSSLKey != "" {
		fmt.Printf("Instance SSL Key:        %s\n", db.InstanceSSLKey)
	}
	if db.InstanceSSLRootCert != "" {
		fmt.Printf("Instance SSL Root Certificate: %s\n", db.InstanceSSLRootCert)
	}
	if len(db.PolicyIDs) > 0 {
		fmt.Printf("Policy IDs:            %s\n", strings.Join(db.PolicyIDs, ", "))
	}
	fmt.Printf("Created:               %s\n", db.Created)
	fmt.Printf("Updated:               %s\n", db.Updated)
	fmt.Println()

	// Check for schema and tables flags
	showSchema := false
	showTables := false

	for _, arg := range args {
		switch arg {
		case "--schema":
			showSchema = true
		case "--tables":
			showTables = true
		}
	}

	// Display schema if requested
	if showSchema {
		if err := formatSchemaData(db.DatabaseSchema); err != nil {
			return fmt.Errorf("failed to format schema data: %v", err)
		}
	}

	// Display tables if requested
	if showTables {
		if err := formatTablesData(db.DatabaseTables); err != nil {
			return fmt.Errorf("failed to format tables data: %v", err)
		}
	}

	return nil
}

// readPassword reads a password from stdin with masking
func readPassword() (string, error) {
	bytePassword, err := term.ReadPassword(int(syscall.Stdin))
	if err != nil {
		return "", err
	}
	fmt.Println() // Print newline after password input
	return string(bytePassword), nil
}

func CreateDatabase(args []string) error {
	reader := bufio.NewReader(os.Stdin)

	// Get database name
	var databaseName string
	if len(args) > 0 && strings.HasPrefix(args[0], "--name=") {
		databaseName = strings.TrimPrefix(args[0], "--name=")
	} else {
		fmt.Print("Database Name: ")
		databaseName, _ = reader.ReadString('\n')
		databaseName = strings.TrimSpace(databaseName)
	}

	if databaseName == "" {
		return fmt.Errorf("database name is required")
	}

	// Get database description
	var databaseDescription string
	if len(args) > 1 && strings.HasPrefix(args[1], "--description=") {
		databaseDescription = strings.TrimPrefix(args[1], "--description=")
	} else {
		fmt.Print("Database Description (optional): ")
		databaseDescription, _ = reader.ReadString('\n')
		databaseDescription = strings.TrimSpace(databaseDescription)
	}

	// Get database type
	var databaseType string
	if len(args) > 2 && strings.HasPrefix(args[2], "--type=") {
		databaseType = strings.TrimPrefix(args[2], "--type=")
	} else {
		fmt.Print("Database Type (e.g., postgres, mysql, mongodb): ")
		databaseType, _ = reader.ReadString('\n')
		databaseType = strings.TrimSpace(databaseType)
	}

	if databaseType == "" {
		return fmt.Errorf("database type is required")
	}

	// Database vendor is metadata only; optional. Default to "custom" if not provided via flag.
	var databaseVendor string
	if len(args) > 3 && strings.HasPrefix(args[3], "--vendor=") {
		databaseVendor = strings.TrimPrefix(args[3], "--vendor=")
	} else {
		databaseVendor = "custom"
	}

	// Get host
	var host string
	if len(args) > 4 && strings.HasPrefix(args[4], "--host=") {
		host = strings.TrimPrefix(args[4], "--host=")
	} else {
		fmt.Print("Host: ")
		host, _ = reader.ReadString('\n')
		host = strings.TrimSpace(host)
	}

	if host == "" {
		return fmt.Errorf("host is required")
	}

	// Get port
	var portStr string
	if len(args) > 5 && strings.HasPrefix(args[5], "--port=") {
		portStr = strings.TrimPrefix(args[5], "--port=")
	} else {
		fmt.Print("Port: ")
		portStr, _ = reader.ReadString('\n')
		portStr = strings.TrimSpace(portStr)
	}

	if portStr == "" {
		return fmt.Errorf("port is required")
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("invalid port. Must be an integer")
	}

	// Get username
	var username string
	if len(args) > 6 && strings.HasPrefix(args[6], "--username=") {
		username = strings.TrimPrefix(args[6], "--username=")
	} else {
		fmt.Print("Username: ")
		username, _ = reader.ReadString('\n')
		username = strings.TrimSpace(username)
	}

	// Username can be empty for some databases

	// Get password with masking
	var password string
	if len(args) > 7 && strings.HasPrefix(args[7], "--password=") {
		password = strings.TrimPrefix(args[7], "--password=")
	} else {
		fmt.Print("Password: ")
		password, err = readPassword()
		if err != nil {
			return fmt.Errorf("failed to read password: %v", err)
		}
	}

	// Password can be empty for some databases

	// Get database name (DB Name)
	var dbName string
	if len(args) > 8 && strings.HasPrefix(args[8], "--db-name=") {
		dbName = strings.TrimPrefix(args[8], "--db-name=")
	} else {
		fmt.Print("Database Name (DB Name): ")
		dbName, _ = reader.ReadString('\n')
		dbName = strings.TrimSpace(dbName)
	}

	if dbName == "" {
		return fmt.Errorf("database name (DB Name) is required")
	}

	// Get node ID (optional)
	var nodeID string
	if len(args) > 9 && strings.HasPrefix(args[9], "--node-id=") {
		nodeID = strings.TrimPrefix(args[9], "--node-id=")
	}

	// Get enabled status
	var enabledStr string
	if len(args) > 10 && strings.HasPrefix(args[10], "--enabled=") {
		enabledStr = strings.TrimPrefix(args[10], "--enabled=")
	} else {
		fmt.Print("Enabled (true/false): ")
		enabledStr, _ = reader.ReadString('\n')
		enabledStr = strings.TrimSpace(enabledStr)
	}

	if enabledStr == "" {
		return fmt.Errorf("enabled status is required")
	}

	if enabledStr != "true" && enabledStr != "false" {
		return fmt.Errorf("invalid enabled status. Must be one of: true, false")
	}

	enabled := enabledStr == "true"

	// Get SSL status
	var sslStr string
	if len(args) > 11 && strings.HasPrefix(args[11], "--ssl=") {
		sslStr = strings.TrimPrefix(args[11], "--ssl=")
	} else {
		fmt.Print("SSL (true/false): ")
		sslStr, _ = reader.ReadString('\n')
		sslStr = strings.TrimSpace(sslStr)
	}

	if sslStr == "" {
		return fmt.Errorf("SSL status is required")
	}

	if sslStr != "true" && sslStr != "false" {
		return fmt.Errorf("invalid SSL status. Must be one of: true, false")
	}

	ssl := sslStr == "true"

	// Get SSL mode
	var sslMode string
	if len(args) > 12 && strings.HasPrefix(args[12], "--ssl-mode=") {
		sslMode = strings.TrimPrefix(args[12], "--ssl-mode=")
	} else {
		if ssl {
			fmt.Print("SSL Mode (require, prefer, disable): ")
			sslMode, _ = reader.ReadString('\n')
			sslMode = strings.TrimSpace(sslMode)
		} else {
			// Automatically set SSL mode to disable when SSL is false
			sslMode = "disable"
		}
	}

	if sslMode == "" {
		return fmt.Errorf("SSL mode is required")
	}

	if sslMode != "require" && sslMode != "prefer" && sslMode != "disable" {
		return fmt.Errorf("invalid SSL mode. Must be one of: require, prefer, disable")
	}

	// Get SSL certificates if SSL mode is not disable
	var sslCert, sslKey, sslRootCert string
	if sslMode != "disable" {
		if len(args) > 13 && strings.HasPrefix(args[13], "--ssl-cert=") {
			sslCert = strings.TrimPrefix(args[13], "--ssl-cert=")
		} else {
			fmt.Print("SSL Certificate (optional): ")
			sslCert, _ = reader.ReadString('\n')
			sslCert = strings.TrimSpace(sslCert)
		}

		if len(args) > 14 && strings.HasPrefix(args[14], "--ssl-key=") {
			sslKey = strings.TrimPrefix(args[14], "--ssl-key=")
		} else {
			fmt.Print("SSL Private Key (optional): ")
			sslKey, _ = reader.ReadString('\n')
			sslKey = strings.TrimSpace(sslKey)
		}

		if len(args) > 15 && strings.HasPrefix(args[15], "--ssl-root-cert=") {
			sslRootCert = strings.TrimPrefix(args[15], "--ssl-root-cert=")
		} else {
			fmt.Print("SSL Root Certificate (optional): ")
			sslRootCert, _ = reader.ReadString('\n')
			sslRootCert = strings.TrimSpace(sslRootCert)
		}
	}

	// Get environment ID (optional)
	var environmentID string
	if len(args) > 16 && strings.HasPrefix(args[16], "--environment-id=") {
		environmentID = strings.TrimPrefix(args[16], "--environment-id=")
	}

	// Create the database connection request
	connectReq := struct {
		DatabaseName        string `json:"database_name"`
		DatabaseDescription string `json:"database_description,omitempty"`
		DatabaseType        string `json:"database_type"`
		DatabaseVendor      string `json:"database_vendor"`
		Host                string `json:"host"`
		Port                int    `json:"port"`
		Username            string `json:"username"`
		Password            string `json:"password"`
		DBName              string `json:"db_name"`
		NodeID              string `json:"node_id,omitempty"`
		Enabled             bool   `json:"enabled"`
		SSL                 bool   `json:"ssl"`
		SSLMode             string `json:"ssl_mode,omitempty"`
		SSLCert             string `json:"ssl_cert,omitempty"`
		SSLKey              string `json:"ssl_key,omitempty"`
		SSLRootCert         string `json:"ssl_root_cert,omitempty"`
		EnvironmentID       string `json:"environment_id,omitempty"`
	}{
		DatabaseName:        databaseName,
		DatabaseDescription: databaseDescription,
		DatabaseType:        databaseType,
		DatabaseVendor:      databaseVendor,
		Host:                host,
		Port:                port,
		Username:            username,
		Password:            password,
		DBName:              dbName,
		NodeID:              nodeID,
		Enabled:             enabled,
		SSL:                 ssl,
		SSLMode:             sslMode,
		SSLCert:             sslCert,
		SSLKey:              sslKey,
		SSLRootCert:         sslRootCert,
		EnvironmentID:       environmentID,
	}

	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	username, err = config.GetUsername()
	if err != nil {
		fmt.Println("Authentication Status: Not logged in")
		fmt.Println("No user credentials found in keyring")
		return nil
	}

	workspaceName, err := config.GetWorkspaceWithError(username)
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/workspaces/%s/databases/connect", tenantURL, workspaceName)

	var connectResponse struct {
		Message  string   `json:"message"`
		Success  bool     `json:"success"`
		Database Database `json:"database"`
		Status   string   `json:"status"`
	}
	if err := client.Post(url, connectReq, &connectResponse, true); err != nil {
		return fmt.Errorf("failed to create database: %v", err)
	}

	fmt.Printf("Successfully created database '%s' (ID: %s)\n", connectResponse.Database.DatabaseName, connectResponse.Database.DatabaseID)
	return nil
}

func ModifyDatabase(databaseName string, args []string) error {
	databaseName = strings.TrimSpace(databaseName)
	if databaseName == "" {
		return fmt.Errorf("database name is required")
	}

	// First find the database to get its details
	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	username, err := config.GetUsername()
	if err != nil {
		fmt.Println("Authentication Status: Not logged in")
		fmt.Println("No user credentials found in keyring")
		return nil
	}

	workspaceName, err := config.GetWorkspaceWithError(username)
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/workspaces/%s/databases/%s", tenantURL, workspaceName, databaseName)

	fmt.Println()

	var response struct {
		Database Database `json:"database"`
	}
	if err := client.Get(url, &response, true); err != nil {
		return fmt.Errorf("failed to get database: %v", err)
	}

	targetDatabase := response.Database

	reader := bufio.NewReader(os.Stdin)
	updateReq := make(map[string]interface{})
	hasChanges := false

	// Parse command line arguments or prompt for input
	for _, arg := range args {
		switch {
		case strings.HasPrefix(arg, "--name="):
			updateReq["database_name_new"] = strings.TrimPrefix(arg, "--name=")
			hasChanges = true
		case strings.HasPrefix(arg, "--description="):
			updateReq["database_description"] = strings.TrimPrefix(arg, "--description=")
			hasChanges = true
		case strings.HasPrefix(arg, "--type="):
			updateReq["database_type"] = strings.TrimPrefix(arg, "--type=")
			hasChanges = true
		case strings.HasPrefix(arg, "--vendor="):
			updateReq["database_vendor"] = strings.TrimPrefix(arg, "--vendor=")
			hasChanges = true
		case strings.HasPrefix(arg, "--host="):
			updateReq["host"] = strings.TrimPrefix(arg, "--host=")
			hasChanges = true
		case strings.HasPrefix(arg, "--port="):
			portInt, err := strconv.Atoi(strings.TrimPrefix(arg, "--port="))
			if err != nil {
				return fmt.Errorf("invalid port. Must be an integer")
			}
			updateReq["port"] = portInt
			hasChanges = true
		case strings.HasPrefix(arg, "--username="):
			updateReq["username"] = strings.TrimPrefix(arg, "--username=")
			hasChanges = true
		case strings.HasPrefix(arg, "--password="):
			updateReq["password"] = strings.TrimPrefix(arg, "--password=")
			hasChanges = true
		case strings.HasPrefix(arg, "--db-name="):
			updateReq["db_name"] = strings.TrimPrefix(arg, "--db-name=")
			hasChanges = true
		case strings.HasPrefix(arg, "--node-id="):
			updateReq["node_id"] = strings.TrimPrefix(arg, "--node-id=")
			hasChanges = true
		case strings.HasPrefix(arg, "--ssl="):
			updateReq["ssl"] = strings.TrimPrefix(arg, "--ssl=") == "true"
			hasChanges = true
		case strings.HasPrefix(arg, "--ssl-mode="):
			updateReq["ssl_mode"] = strings.TrimPrefix(arg, "--ssl-mode=")
			hasChanges = true
		case strings.HasPrefix(arg, "--ssl-cert="):
			updateReq["ssl_cert"] = strings.TrimPrefix(arg, "--ssl-cert=")
			hasChanges = true
		case strings.HasPrefix(arg, "--ssl-key="):
			updateReq["ssl_key"] = strings.TrimPrefix(arg, "--ssl-key=")
			hasChanges = true
		case strings.HasPrefix(arg, "--ssl-root-cert="):
			updateReq["ssl_root_cert"] = strings.TrimPrefix(arg, "--ssl-root-cert=")
			hasChanges = true
		case strings.HasPrefix(arg, "--environment-id="):
			updateReq["environment_id"] = strings.TrimPrefix(arg, "--environment-id=")
			hasChanges = true
		}
	}

	// If no arguments provided, prompt for input
	if !hasChanges {
		fmt.Printf("Modifying database '%s' (press Enter to keep current value):\n", databaseName)

		fmt.Printf("Name [%s]: ", targetDatabase.DatabaseName)
		newName, _ := reader.ReadString('\n')
		newName = strings.TrimSpace(newName)
		if newName != "" {
			updateReq["database_name_new"] = newName
			hasChanges = true
		}

		fmt.Printf("Description [%s]: ", targetDatabase.DatabaseDescription)
		newDescription, _ := reader.ReadString('\n')
		newDescription = strings.TrimSpace(newDescription)
		if newDescription != "" {
			updateReq["database_description"] = newDescription
			hasChanges = true
		}

		fmt.Printf("Type [%s]: ", targetDatabase.DatabaseType)
		newType, _ := reader.ReadString('\n')
		newType = strings.TrimSpace(newType)
		if newType != "" {
			updateReq["database_type"] = newType
			hasChanges = true
		}

		fmt.Printf("Vendor [%s]: ", targetDatabase.DatabaseVendor)
		newVendor, _ := reader.ReadString('\n')
		newVendor = strings.TrimSpace(newVendor)
		if newVendor != "" {
			updateReq["database_vendor"] = newVendor
			hasChanges = true
		}

		fmt.Printf("Host [%s]: ", targetDatabase.InstanceHost)
		newHost, _ := reader.ReadString('\n')
		newHost = strings.TrimSpace(newHost)
		if newHost != "" {
			updateReq["host"] = newHost
			hasChanges = true
		}

		fmt.Printf("Port [%d]: ", targetDatabase.InstancePort)
		newPort, _ := reader.ReadString('\n')
		newPort = strings.TrimSpace(newPort)
		if newPort != "" {
			portInt, err := strconv.Atoi(newPort)
			if err != nil {
				return fmt.Errorf("invalid port. Must be an integer")
			}
			updateReq["port"] = portInt
			hasChanges = true
		}

		fmt.Printf("Username [%s]: ", targetDatabase.DatabaseUsername)
		newUsername, _ := reader.ReadString('\n')
		newUsername = strings.TrimSpace(newUsername)
		if newUsername != "" {
			updateReq["username"] = newUsername
			hasChanges = true
		}

		fmt.Print("Password (leave blank to keep current): ")
		newPassword, err := readPassword()
		if err != nil {
			return fmt.Errorf("failed to read password: %v", err)
		}
		if newPassword != "" {
			updateReq["password"] = newPassword
			hasChanges = true
		}

		fmt.Printf("Database Name (DB Name) [%s]: ", targetDatabase.DatabaseDBName)
		newDBName, _ := reader.ReadString('\n')
		newDBName = strings.TrimSpace(newDBName)
		if newDBName != "" {
			updateReq["db_name"] = newDBName
			hasChanges = true
		}

		fmt.Printf("Connected to Node ID [%s]: ", targetDatabase.ConnectedToNodeID)
		newNodeID, _ := reader.ReadString('\n')
		newNodeID = strings.TrimSpace(newNodeID)
		if newNodeID != "" {
			updateReq["node_id"] = newNodeID
			hasChanges = true
		}

		fmt.Printf("SSL [%t]: ", targetDatabase.InstanceSSL)
		newSSL, _ := reader.ReadString('\n')
		newSSL = strings.TrimSpace(newSSL)
		currentSSL := targetDatabase.InstanceSSL
		if newSSL != "" {
			currentSSL = newSSL == "true"
			updateReq["ssl"] = currentSSL
			hasChanges = true
		}

		// Only prompt for SSL details if SSL is enabled
		if currentSSL {
			fmt.Printf("SSL Mode [%s]: ", targetDatabase.InstanceSSLMode)
			newSSLMode, _ := reader.ReadString('\n')
			newSSLMode = strings.TrimSpace(newSSLMode)
			if newSSLMode != "" {
				updateReq["ssl_mode"] = newSSLMode
				hasChanges = true
			}

			fmt.Printf("SSL Certificate [%s]: ", targetDatabase.InstanceSSLCert)
			newSSLCert, _ := reader.ReadString('\n')
			newSSLCert = strings.TrimSpace(newSSLCert)
			if newSSLCert != "" {
				updateReq["ssl_cert"] = newSSLCert
				hasChanges = true
			}

			fmt.Printf("SSL Key [%s]: ", targetDatabase.InstanceSSLKey)
			newSSLKey, _ := reader.ReadString('\n')
			newSSLKey = strings.TrimSpace(newSSLKey)
			if newSSLKey != "" {
				updateReq["ssl_key"] = newSSLKey
				hasChanges = true
			}

			fmt.Printf("SSL Root Certificate [%s]: ", targetDatabase.InstanceSSLRootCert)
			newSSLRootCert, _ := reader.ReadString('\n')
			newSSLRootCert = strings.TrimSpace(newSSLRootCert)
			if newSSLRootCert != "" {
				updateReq["ssl_root_cert"] = newSSLRootCert
				hasChanges = true
			}
		} else {
			// If SSL is disabled, set SSL mode to disable
			updateReq["ssl_mode"] = "disable"
			hasChanges = true
		}

		if targetDatabase.EnvironmentID != "" {
			fmt.Printf("Environment ID [%s]: ", targetDatabase.EnvironmentID)
		} else {
			fmt.Print("Environment ID (optional): ")
		}
		newEnvironmentID, _ := reader.ReadString('\n')
		newEnvironmentID = strings.TrimSpace(newEnvironmentID)
		if newEnvironmentID != "" {
			updateReq["environment_id"] = newEnvironmentID
			hasChanges = true
		}
	}

	if !hasChanges {
		fmt.Println("No changes made")
		return nil
	}

	// Update the database
	updateURL := fmt.Sprintf("%s/api/v1/workspaces/%s/databases/%s", tenantURL, workspaceName, databaseName)

	var updateResponse struct {
		Message  string   `json:"message"`
		Success  bool     `json:"success"`
		Database Database `json:"database"`
		Status   string   `json:"status"`
	}
	if err := client.Put(updateURL, updateReq, &updateResponse, true); err != nil {
		return fmt.Errorf("failed to update database: %v", err)
	}

	fmt.Printf("Successfully updated database '%s'\n", updateResponse.Database.DatabaseName)
	fmt.Println()
	return nil
}

func DeleteDatabase(databaseName string, args []string) error {
	databaseName = strings.TrimSpace(databaseName)
	if databaseName == "" {
		return fmt.Errorf("database name is required")
	}

	// Check for force flag and delete flags
	force := false
	deleteDatabaseObject := false
	deleteRepo := false
	for _, arg := range args {
		switch {
		case arg == "--force" || arg == "-f":
			force = true
		case strings.HasPrefix(arg, "--delete-database-object="):
			deleteDatabaseObject = strings.TrimPrefix(arg, "--delete-database-object=") == "true"
		case strings.HasPrefix(arg, "--delete-repo="):
			deleteRepo = strings.TrimPrefix(arg, "--delete-repo=") == "true"
		}
	}

	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	username, err := config.GetUsername()
	if err != nil {
		fmt.Println("Authentication Status: Not logged in")
		fmt.Println("No user credentials found in keyring")
		return nil
	}

	workspaceName, err := config.GetWorkspaceWithError(username)
	if err != nil {
		return err
	}

	client := httpclient.GetClient()

	// Confirm deletion unless force flag is used
	if !force {
		reader := bufio.NewReader(os.Stdin)
		fmt.Println()
		fmt.Printf("Are you sure you want to disconnect database '%s'? This action cannot be undone. (y/N): ", databaseName)
		confirmation, _ := reader.ReadString('\n')
		confirmation = strings.TrimSpace(strings.ToLower(confirmation))

		if confirmation != "y" && confirmation != "yes" {
			fmt.Println("Operation cancelled")
			fmt.Println()
			return nil
		}

		// Ask about deleting the database object
		fmt.Print("Delete the database object as well? (y/N): ")
		deleteObjectConfirmation, _ := reader.ReadString('\n')
		deleteObjectConfirmation = strings.TrimSpace(strings.ToLower(deleteObjectConfirmation))

		if deleteObjectConfirmation == "y" || deleteObjectConfirmation == "yes" {
			deleteDatabaseObject = true
		}

		// Ask about deleting the repo
		fmt.Print("Delete the repository as well? (y/N): ")
		deleteRepoConfirmation, _ := reader.ReadString('\n')
		deleteRepoConfirmation = strings.TrimSpace(strings.ToLower(deleteRepoConfirmation))

		if deleteRepoConfirmation == "y" || deleteRepoConfirmation == "yes" {
			deleteRepo = true
		}
	}

	// Create disconnect request
	disconnectReq := map[string]interface{}{
		"delete_database_object": deleteDatabaseObject,
		"delete_repo":            deleteRepo,
	}

	// Disconnect the database
	disconnectURL := fmt.Sprintf("%s/api/v1/workspaces/%s/databases/%s/disconnect", tenantURL, workspaceName, databaseName)

	var disconnectResponse struct {
		Message string `json:"message"`
		Success bool   `json:"success"`
		Status  string `json:"status"`
	}
	if err := client.Post(disconnectURL, disconnectReq, &disconnectResponse, true); err != nil {
		return fmt.Errorf("failed to delete database: %v", err)
	}

	fmt.Printf("Successfully disconnected database '%s'\n", databaseName)
	if deleteDatabaseObject {
		fmt.Println("Database object has been deleted")
	}
	if deleteRepo {
		fmt.Println("Repository has been deleted")
	}
	fmt.Println()
	return nil
}

func withInstanceName(reader *bufio.Reader, argsMap map[argKey]string,
	instanceName, databaseName, databaseDescription, dbName string) error {
	// Existing instance path
	// Get username (optional)
	dbLogin, dbPassword, err := usernameAndPassword(reader, argsMap)
	if err != nil {
		return err
	}

	// Get node ID (optional)
	nodeID := getArgOrPrompt(reader, argsMap, nodeIdKey, "", false)

	// Get enabled status
	enabled, err := enabledParam(reader, argsMap)
	if err != nil {
		return err
	}

	// Get environment ID (optional)
	environmentID := getArgOrPrompt(reader, argsMap, environmentIdKey, "", false)

	connectReq := map[string]interface{}{
		"instance_name":        instanceName,
		"database_name":        databaseName,
		"database_description": databaseDescription,
		"db_name":              dbName,
		"username":             dbLogin,
		"password":             dbPassword,
		"node_id":              nodeID,
		"enabled":              enabled,
		"environment_id":       environmentID,
	}

	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}
	username, err := config.GetUsername()
	if err != nil {
		fmt.Println("Authentication Status: Not logged in")
		fmt.Println("No user credentials found in keyring")
		return nil
	}
	workspaceName, err := config.GetWorkspaceWithError(username)
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/workspaces/%s/databases/connect-with-instance", tenantURL, workspaceName)

	var connectResponse struct {
		Message  string   `json:"message"`
		Success  bool     `json:"success"`
		Database Database `json:"database"`
		Status   string   `json:"status"`
	}
	if err := client.Post(url, connectReq, &connectResponse, true); err != nil {
		return fmt.Errorf("failed to connect database: %w", err)
	}

	fmt.Printf("Successfully connected database '%s' to instance '%s' (ID: %s)\n", connectResponse.Database.DatabaseName, instanceName, connectResponse.Database.DatabaseID)
	return nil
}

func ConnectDatabase(databaseName string, args []string) error {
	databaseName = strings.TrimSpace(databaseName)
	if databaseName == "" {
		return fmt.Errorf("database name is required")
	}

	reader := bufio.NewReader(os.Stdin)
	argsMap := scanArgs(args)

	// Get instance name (optional). If empty, we'll create a new instance implicitly.
	instanceName := instanceParam(reader, argsMap)

	// Get database description
	databaseDescription := descriptionParam(reader, argsMap)

	// Get database name (DB Name)
	dbName, err := dbNameParam(reader, argsMap)
	if err != nil {
		return err
	}

	// Branch based on whether user provided an instance
	if instanceName != "" {
		return withInstanceName(reader, argsMap, instanceName, databaseName, databaseDescription, dbName)
	}

	// No instance provided: create instance connection implicitly and connect database
	// Collect detailed connection info (or parse from flags)
	databaseType, err := dbTypeParam(reader, argsMap)
	if err != nil {
		return err
	}

	// Database vendor is metadata only; optional. Default to "custom" if not provided via flag.
	databaseVendor := dbVendorParam(reader, argsMap)

	host, err := hostParam(reader, argsMap)
	if err != nil {
		return err
	}

	port, err := portParam(reader, argsMap)
	if err != nil {
		return err
	}

	dbLogin, dbPassword, err := usernameAndPassword(reader, argsMap)
	if err != nil {
		return err
	}

	nodeID := getArgOrPrompt(reader, argsMap, nodeIdKey, "", false)

	enabled, err := enabledParam(reader, argsMap)
	if err != nil {
		return err
	}

	ssl, sslMode, err := sslSetup(reader, argsMap)
	if err != nil {
		return err
	}

	var sslCert, sslStrKey, sslRootCert string
	if sslMode != "disable" {
		sslCert = getArgOrPrompt(reader, argsMap, sslCertPathKey, "SSL Certificate (optional): ", true)
		sslStrKey = getArgOrPrompt(reader, argsMap, sslKeyPathKey, "SSL Private Key (optional): ", true)
		sslRootCert = getArgOrPrompt(reader, argsMap, sslRootCertPathKey, "SSL Root Certificate (optional): ", true)
	}

	environmentID := getArgOrPrompt(reader, argsMap, environmentIdKey, "", false)

	connectReq := struct {
		DatabaseName        string `json:"database_name"`
		DatabaseDescription string `json:"database_description,omitempty"`
		DatabaseType        string `json:"database_type"`
		DatabaseVendor      string `json:"database_vendor"`
		Host                string `json:"host"`
		Port                int    `json:"port"`
		Username            string `json:"username"`
		Password            string `json:"password"`
		DBName              string `json:"db_name"`
		NodeID              string `json:"node_id,omitempty"`
		Enabled             bool   `json:"enabled"`
		SSL                 bool   `json:"ssl"`
		SSLMode             string `json:"ssl_mode,omitempty"`
		SSLCert             string `json:"ssl_cert,omitempty"`
		SSLKey              string `json:"ssl_key,omitempty"`
		SSLRootCert         string `json:"ssl_root_cert,omitempty"`
		EnvironmentID       string `json:"environment_id,omitempty"`
	}{
		DatabaseName:        databaseName,
		DatabaseDescription: databaseDescription,
		DatabaseType:        databaseType,
		DatabaseVendor:      databaseVendor,
		Host:                host,
		Port:                port,
		Username:            dbLogin,
		Password:            dbPassword,
		DBName:              dbName,
		NodeID:              nodeID,
		Enabled:             enabled,
		SSL:                 ssl,
		SSLMode:             sslMode,
		SSLCert:             sslCert,
		SSLKey:              sslStrKey,
		SSLRootCert:         sslRootCert,
		EnvironmentID:       environmentID,
	}

	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}
	username, err := config.GetUsername()
	if err != nil {
		fmt.Println("Authentication Status: Not logged in")
		fmt.Println("No user credentials found in keyring")
		return nil
	}
	workspaceName, err := config.GetWorkspaceWithError(username)
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/workspaces/%s/databases/connect", tenantURL, workspaceName)

	var connectResponse struct {
		Message  string   `json:"message"`
		Success  bool     `json:"success"`
		Database Database `json:"database"`
		Status   string   `json:"status"`
	}
	if err := client.Post(url, connectReq, &connectResponse, true); err != nil {
		return fmt.Errorf("failed to connect database: %v", err)
	}

	fmt.Printf("Successfully connected database '%s' (ID: %s)\n", connectResponse.Database.DatabaseName, connectResponse.Database.DatabaseID)
	return nil
}

func ReconnectDatabase(databaseName string, _ []string) error {
	databaseName = strings.TrimSpace(databaseName)
	if databaseName == "" {
		return fmt.Errorf("database name is required")
	}

	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	username, err := config.GetUsername()
	if err != nil {
		fmt.Println("Authentication Status: Not logged in")
		fmt.Println("No user credentials found in keyring")
		return nil
	}

	workspaceName, err := config.GetWorkspaceWithError(username)
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/workspaces/%s/databases/%s/reconnect", tenantURL, workspaceName, databaseName)

	var response struct {
		Message  string   `json:"message"`
		Success  bool     `json:"success"`
		Database Database `json:"database"`
		Status   string   `json:"status"`
	}
	if err := client.Post(url, nil, &response, true); err != nil {
		return fmt.Errorf("failed to reconnect database: %v", err)
	}

	fmt.Printf("Successfully reconnected database '%s'\n", databaseName)
	return nil
}

func DisconnectDatabase(databaseName string, args []string) error {
	// For CLI, this can be an alias for DeleteDatabase
	return DeleteDatabase(databaseName, args)
}

func WipeDatabase(databaseName string, _ []string) error {
	databaseName = strings.TrimSpace(databaseName)
	if databaseName == "" {
		return fmt.Errorf("database name is required")
	}

	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	username, err := config.GetUsername()
	if err != nil {
		fmt.Println("Authentication Status: Not logged in")
		fmt.Println("No user credentials found in keyring")
		return nil
	}

	workspaceName, err := config.GetWorkspaceWithError(username)
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/workspaces/%s/databases/%s/wipe", tenantURL, workspaceName, databaseName)

	var response struct {
		Message string `json:"message"`
		Success bool   `json:"success"`
		Status  string `json:"status"`
	}
	if err := client.Post(url, nil, &response, true); err != nil {
		return fmt.Errorf("failed to wipe database: %v", err)
	}

	fmt.Printf("Successfully wiped database '%s'\n", databaseName)
	return nil
}

func DropDatabase(databaseName string, _ []string) error {
	databaseName = strings.TrimSpace(databaseName)
	if databaseName == "" {
		return fmt.Errorf("database name is required")
	}

	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	username, err := config.GetUsername()
	if err != nil {
		fmt.Println("Authentication Status: Not logged in")
		fmt.Println("No user credentials found in keyring")
		return nil
	}

	workspaceName, err := config.GetWorkspaceWithError(username)
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/workspaces/%s/databases/%s/drop", tenantURL, workspaceName, databaseName)

	var response struct {
		Message string `json:"message"`
		Success bool   `json:"success"`
		Status  string `json:"status"`
	}
	if err := client.Post(url, nil, &response, true); err != nil {
		return fmt.Errorf("failed to drop database: %v", err)
	}

	fmt.Printf("Successfully dropped database '%s'\n", databaseName)
	return nil
}

func CloneTableData(mappingName string, _ []string) error {
	mappingName = strings.TrimSpace(mappingName)
	if mappingName == "" {
		return fmt.Errorf("mapping name is required")
	}

	// Set mode to append
	mode := "append"

	// Set batch size to 1000
	batchSize := 1000

	// Set timeout to 300 seconds
	timeout := 300

	// Create the transform data request
	transformReq := struct {
		MappingName string                 `json:"mapping_name"`
		Mode        string                 `json:"mode"`
		Options     map[string]interface{} `json:"options,omitempty"`
	}{
		MappingName: mappingName,
		Mode:        mode,
		Options: map[string]interface{}{
			"batch_size": batchSize,
			"timeout":    timeout,
		},
	}

	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	username, err := config.GetUsername()
	if err != nil {
		fmt.Println("Authentication Status: Not logged in")
		fmt.Println("No user credentials found in keyring")
		return nil
	}

	workspaceName, err := config.GetWorkspaceWithError(username)
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/workspaces/%s/databases/transform", tenantURL, workspaceName)

	var response struct {
		Message string `json:"message"`
		Success bool   `json:"success"`
		Status  string `json:"status"`
	}
	if err := client.Post(url, transformReq, &response, true); err != nil {
		return fmt.Errorf("failed to clone table data: %v", err)
	}

	fmt.Printf("Successfully executed table data transformation using mapping '%s'\n", mappingName)
	return nil
}
