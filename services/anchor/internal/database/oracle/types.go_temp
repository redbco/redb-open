package oracle

// OracleReplicationSourceDetails holds information about a replication source
type OracleReplicationSourceDetails struct {
	LogMinerSessionID string `json:"logminer_session_id"`
	TableName         string `json:"table_name"`
	DatabaseID        string `json:"database_id"`
	LastSCN           int64  `json:"last_scn"`
}

// OracleReplicationChange represents a change in the database
type OracleReplicationChange struct {
	Operation string
	Data      map[string]interface{}
	OldData   map[string]interface{}
}
