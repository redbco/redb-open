package mariadb

// MariaDBReplicationSourceDetails contains details about a MariaDB replication source
type MariaDBReplicationSourceDetails struct {
	BinlogFile     string `json:"binlog_file"`
	BinlogPosition uint32 `json:"binlog_position"`
	TableName      string `json:"table_name"`
	DatabaseID     string `json:"database_id"`
}

// MariaDBReplicationChange represents a change in MariaDB replication
type MariaDBReplicationChange struct {
	Operation string
	Data      map[string]interface{}
	OldData   map[string]interface{}
}
