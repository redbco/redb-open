//go:build enterprise
// +build enterprise

package oracle

// OracleReplicationSourceDetails holds information about an Oracle replication source using LogMiner
type OracleReplicationSourceDetails struct {
	LogMinerSessionID string `json:"logminer_session_id"`
	TableName         string `json:"table_name"`
	DatabaseID        string `json:"database_id"`
	LastSCN           int64  `json:"last_scn"` // System Change Number for positioning
}

// OracleReplicationChange represents a change in the Oracle database
type OracleReplicationChange struct {
	Operation string                 `json:"operation"`
	Data      map[string]interface{} `json:"data"`
	OldData   map[string]interface{} `json:"old_data,omitempty"`
}
