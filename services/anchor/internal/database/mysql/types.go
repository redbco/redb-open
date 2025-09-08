package mysql

// MySQLReplicationSourceDetails contains details about a MySQL replication source
type MySQLReplicationSourceDetails struct {
	BinlogFile     string `json:"binlog_file"`
	BinlogPosition uint32 `json:"binlog_position"`
	TableName      string `json:"table_name"`
	DatabaseID     string `json:"database_id"`
}

// MySQLReplicationChange represents a change in MySQL replication
type MySQLReplicationChange struct {
	Operation string
	Data      map[string]interface{}
	OldData   map[string]interface{}
}
