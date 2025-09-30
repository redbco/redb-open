package dbcapabilities

import (
	"testing"
)

func TestParseConnectionString(t *testing.T) {
	tests := []struct {
		name             string
		connectionStr    string
		expectedType     string
		expectedHost     string
		expectedPort     int32
		expectedUser     string
		expectedPass     string
		expectedDB       string
		expectedSSL      bool
		expectedSSLMode  string
		expectedSystemDB bool
		expectError      bool
	}{
		{
			name:             "PostgreSQL with system database",
			connectionStr:    "postgresql://user:pass@localhost:5432/postgres?sslmode=require",
			expectedType:     "postgres",
			expectedHost:     "localhost",
			expectedPort:     5432,
			expectedUser:     "user",
			expectedPass:     "pass",
			expectedDB:       "postgres",
			expectedSSL:      true,
			expectedSSLMode:  "require",
			expectedSystemDB: true,
			expectError:      false,
		},
		{
			name:             "PostgreSQL with custom database",
			connectionStr:    "postgresql://user:pass@localhost:5432/myapp?sslmode=disable",
			expectedType:     "postgres",
			expectedHost:     "localhost",
			expectedPort:     5432,
			expectedUser:     "user",
			expectedPass:     "pass",
			expectedDB:       "myapp",
			expectedSSL:      false,
			expectedSSLMode:  "disable",
			expectedSystemDB: false,
			expectError:      false,
		},
		{
			name:             "MySQL with default port",
			connectionStr:    "mysql://root:password@db.example.com/mysql?tls=true",
			expectedType:     "mysql",
			expectedHost:     "db.example.com",
			expectedPort:     3306,
			expectedUser:     "root",
			expectedPass:     "password",
			expectedDB:       "mysql",
			expectedSSL:      true,
			expectedSSLMode:  "require",
			expectedSystemDB: true,
			expectError:      false,
		},
		{
			name:             "MongoDB with SSL",
			connectionStr:    "mongodb://user:pass@mongo.example.com:27017/mydb?tls=true",
			expectedType:     "mongodb",
			expectedHost:     "mongo.example.com",
			expectedPort:     27017,
			expectedUser:     "user",
			expectedPass:     "pass",
			expectedDB:       "mydb",
			expectedSSL:      true,
			expectedSSLMode:  "require",
			expectedSystemDB: false,
			expectError:      false,
		},
		{
			name:             "Redis connection",
			connectionStr:    "redis://user:pass@redis.example.com:6379/0?ssl=false",
			expectedType:     "redis",
			expectedHost:     "redis.example.com",
			expectedPort:     6379,
			expectedUser:     "user",
			expectedPass:     "pass",
			expectedDB:       "0",
			expectedSSL:      false,
			expectedSSLMode:  "disable",
			expectedSystemDB: false,
			expectError:      false,
		},
		{
			name:             "SQL Server with encryption",
			connectionStr:    "sqlserver://sa:password@sqlserver.example.com:1433/master?encrypt=true&trustservercertificate=true",
			expectedType:     "mssql",
			expectedHost:     "sqlserver.example.com",
			expectedPort:     1433,
			expectedUser:     "sa",
			expectedPass:     "password",
			expectedDB:       "master",
			expectedSSL:      true,
			expectedSSLMode:  "prefer",
			expectedSystemDB: true,
			expectError:      false,
		},
		{
			name:             "ClickHouse with secure connection",
			connectionStr:    "clickhouse://user:pass@clickhouse.example.com:8123/default?secure=true",
			expectedType:     "clickhouse",
			expectedHost:     "clickhouse.example.com",
			expectedPort:     8123,
			expectedUser:     "user",
			expectedPass:     "pass",
			expectedDB:       "default",
			expectedSSL:      true,
			expectedSSLMode:  "require",
			expectedSystemDB: false,
			expectError:      false,
		},
		{
			name:          "Invalid connection string - no scheme",
			connectionStr: "user:pass@localhost:5432/postgres",
			expectError:   true,
		},
		{
			name:          "Invalid connection string - unsupported database",
			connectionStr: "unsupported://user:pass@localhost:5432/db",
			expectError:   true,
		},
		{
			name:          "Invalid connection string - no host",
			connectionStr: "postgresql://user:pass@:5432/postgres",
			expectError:   true,
		},
		{
			name:          "Invalid connection string - no username",
			connectionStr: "postgresql://:pass@localhost:5432/postgres",
			expectError:   true,
		},
		{
			name:          "Empty connection string",
			connectionStr: "",
			expectError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			details, err := ParseConnectionString(tt.connectionStr)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if details.DatabaseType != tt.expectedType {
				t.Errorf("expected database type %s, got %s", tt.expectedType, details.DatabaseType)
			}

			if details.Host != tt.expectedHost {
				t.Errorf("expected host %s, got %s", tt.expectedHost, details.Host)
			}

			if details.Port != tt.expectedPort {
				t.Errorf("expected port %d, got %d", tt.expectedPort, details.Port)
			}

			if details.Username != tt.expectedUser {
				t.Errorf("expected username %s, got %s", tt.expectedUser, details.Username)
			}

			if details.Password != tt.expectedPass {
				t.Errorf("expected password %s, got %s", tt.expectedPass, details.Password)
			}

			if details.DatabaseName != tt.expectedDB {
				t.Errorf("expected database name %s, got %s", tt.expectedDB, details.DatabaseName)
			}

			if details.SSL != tt.expectedSSL {
				t.Errorf("expected SSL %t, got %t", tt.expectedSSL, details.SSL)
			}

			if details.SSLMode != tt.expectedSSLMode {
				t.Errorf("expected SSL mode %s, got %s", tt.expectedSSLMode, details.SSLMode)
			}

			if details.IsSystemDB != tt.expectedSystemDB {
				t.Errorf("expected system DB %t, got %t", tt.expectedSystemDB, details.IsSystemDB)
			}
		})
	}
}

func TestGetSystemDatabaseName(t *testing.T) {
	tests := []struct {
		name         string
		databaseType string
		expected     string
		expectError  bool
	}{
		{
			name:         "PostgreSQL system database",
			databaseType: "postgres",
			expected:     "postgres",
			expectError:  false,
		},
		{
			name:         "MySQL system database",
			databaseType: "mysql",
			expected:     "mysql",
			expectError:  false,
		},
		{
			name:         "SQL Server system database",
			databaseType: "mssql",
			expected:     "master",
			expectError:  false,
		},
		{
			name:         "MongoDB system database",
			databaseType: "mongodb",
			expected:     "admin",
			expectError:  false,
		},
		{
			name:         "Redis - no system database",
			databaseType: "redis",
			expected:     "",
			expectError:  true,
		},
		{
			name:         "Invalid database type",
			databaseType: "invalid",
			expected:     "",
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := GetSystemDatabaseName(tt.databaseType)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if result != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestValidateConnectionString(t *testing.T) {
	tests := []struct {
		name          string
		connectionStr string
		expectError   bool
	}{
		{
			name:          "Valid PostgreSQL connection string",
			connectionStr: "postgresql://user:pass@localhost:5432/postgres",
			expectError:   false,
		},
		{
			name:          "Valid MySQL connection string",
			connectionStr: "mysql://root:password@localhost:3306/mysql",
			expectError:   false,
		},
		{
			name:          "Invalid connection string - no scheme",
			connectionStr: "user:pass@localhost:5432/postgres",
			expectError:   true,
		},
		{
			name:          "Invalid connection string - unsupported database",
			connectionStr: "unsupported://user:pass@localhost:5432/db",
			expectError:   true,
		},
		{
			name:          "Empty connection string",
			connectionStr: "",
			expectError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateConnectionString(tt.connectionStr)

			if tt.expectError && err == nil {
				t.Errorf("expected error but got none")
			}

			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}
