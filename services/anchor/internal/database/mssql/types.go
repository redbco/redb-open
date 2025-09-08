package mssql

type MSSQLReplicationSourceDetails struct {
	PublicationName string `json:"publication_name"`
	TableName       string `json:"table_name"`
	DatabaseID      string `json:"database_id"`
	SubscriptionID  string `json:"subscription_id"`
}

type MSSQLReplicationChange struct {
	Operation string
	Data      map[string]interface{}
	OldData   map[string]interface{}
}
