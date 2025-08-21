package databases

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
)

func instanceParam(r *bufio.Reader, argsMap map[argKey]string) string {
	return getArgOrPrompt(r, argsMap, instanceKey,
		"Instance Name (leave empty to create new instance connection): ", true)
}

func descriptionParam(r *bufio.Reader, argsMap map[argKey]string) string {
	return getArgOrPrompt(r, argsMap, descriptionKey,
		"Database Description (optional): ", true)
}

func dbNameParam(r *bufio.Reader, argsMap map[argKey]string) (string, error) {
	s := getArgOrPrompt(r, argsMap, dbNameKey, "Database Name (DB Name): ", true)
	if s == "" {
		return "", fmt.Errorf("database name (DB Name) is required")
	}
	return s, nil
}

func usernameAndPassword(r *bufio.Reader, argsMap map[argKey]string) (string, string, error) {
	username := getArgOrPrompt(r, argsMap, usernameKey, "Username (optional): ", true)
	password := getArgOrPrompt(r, argsMap, passwordKey, "Password (optional): ", false)

	if password == "" && username != "" {
		fmt.Print("Password: ")
		pw, pwErr := readPassword()
		if pwErr != nil {
			return username, password, fmt.Errorf("failed to read password: %v", pwErr)
		}
		password = pw
	}

	return username, password, nil
}

func enabledParam(r *bufio.Reader, argsMap map[argKey]string) (bool, error) {
	s := getArgOrPrompt(r, argsMap, enabledKey, "Enabled (true/false): ", true)
	if s == "" {
		return false, fmt.Errorf("enabled status is required")
	}

	s = strings.ToLower(s)
	b, err := strconv.ParseBool(s)

	if err != nil {
		return false, fmt.Errorf("invalid enabled status. Must be one of: true, false")
	}

	return b, nil
}

func dbTypeParam(r *bufio.Reader, argsMap map[argKey]string) (string, error) {
	databaseType := getArgOrPrompt(r, argsMap, dbTypeKey,
		"Database Type (e.g., postgres, mysql, mongodb): ", true)
	if databaseType == "" {
		return "", fmt.Errorf("database type is required")
	}
	return databaseType, nil
}

func dbVendorParam(r *bufio.Reader, argsMap map[argKey]string) string {
	databaseVendor := getArgOrPrompt(r, argsMap, dbVendorKey, "", false)
	if databaseVendor == "" {
		databaseVendor = "custom"
	}
	return databaseVendor
}

func hostParam(r *bufio.Reader, argsMap map[argKey]string) (string, error) {
	host := getArgOrPrompt(r, argsMap, hostKey, "Host: ", true)
	if host == "" {
		return "", fmt.Errorf("host is required")
	}
	return host, nil
}

func portParam(r *bufio.Reader, argsMap map[argKey]string) (int, error) {
	portStr := getArgOrPrompt(r, argsMap, portKey, "Port: ", true)
	if portStr == "" {
		return 0, fmt.Errorf("port is required")
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 0, fmt.Errorf("invalid port. Must be an integer")
	}

	if port < 1 || port > 65535 {
		return 0, fmt.Errorf("invalid port: must be in [1..65535]")
	}

	return port, nil
}

func sslSetup(r *bufio.Reader, argsMap map[argKey]string) (bool, string, error) {
	ssl, err := sslParam(r, argsMap)
	if err != nil {
		return false, "", err
	}

	if !ssl {
		return false, "disable", nil
	}

	mode, err := sslModeParam(r, argsMap)
	if err != nil {
		return false, "", err
	}

	return ssl, mode, nil
}

func sslParam(r *bufio.Reader, argsMap map[argKey]string) (bool, error) {
	s := getArgOrPrompt(r, argsMap, sslEnabledKey, "SSL (true/false): ", true)
	if s == "" {
		return false, fmt.Errorf("SSL status is required")
	}

	s = strings.ToLower(s)
	b, err := strconv.ParseBool(s)

	if err != nil {
		return false, fmt.Errorf("invalid SSL status. Must be one of: true, false")
	}

	return b, nil
}

func sslModeParam(r *bufio.Reader, argsMap map[argKey]string) (string, error) {
	s := getArgOrPrompt(r, argsMap, sslModeKey, "SSL Mode (require, prefer, disable): ", true)
	if s == "" {
		return "", fmt.Errorf("SSL mode is required")
	}

	s = strings.ToLower(s)
	switch s {
	case "require", "prefer", "disable":
	default:
		return "", fmt.Errorf("invalid SSL mode. Must be one of: require, prefer, disable")
	}

	return s, nil
}
