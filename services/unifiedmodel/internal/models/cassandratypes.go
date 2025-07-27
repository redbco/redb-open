package models

// CassandraSchema represents the schema of a Cassandra database
type CassandraSchema struct {
	SchemaType        string           `json:"schemaType"`
	Keyspaces         []KeyspaceInfo   `json:"keyspaces"`
	Tables            []CassandraTable `json:"tables"`
	Types             []CassandraType  `json:"types"`
	Functions         []Function       `json:"functions"`
	Aggregates        []AggregateInfo  `json:"aggregates"`
	MaterializedViews []CassandraView  `json:"materializedViews"`
}

// KeyspaceInfo represents a Cassandra keyspace
type KeyspaceInfo struct {
	Name                string            `json:"name"`
	ReplicationStrategy string            `json:"replicationStrategy"`
	ReplicationOptions  map[string]string `json:"replicationOptions"`
	DurableWrites       bool              `json:"durableWrites"`
}

// CassandraTable represents a Cassandra table
type CassandraTable struct {
	Name       string            `json:"name"`
	Keyspace   string            `json:"keyspace"`
	Columns    []CassandraColumn `json:"columns"`
	PrimaryKey []string          `json:"primaryKey"`
	Clustering []string          `json:"clustering"`
	Properties map[string]string `json:"properties"`
}

// CassandraColumn represents a column in a Cassandra table
type CassandraColumn struct {
	Name       string `json:"name"`
	DataType   string `json:"dataType"`
	IsNullable bool   `json:"isNullable"`
	IsStatic   bool   `json:"isStatic"`
	IsPrimary  bool   `json:"isPrimary"`
}

// CassandraType represents a Cassandra user-defined type
type CassandraType struct {
	Keyspace string               `json:"keyspace"`
	Name     string               `json:"name"`
	Fields   []CassandraTypeField `json:"fields"`
}

// CassandraTypeField represents a field in a Cassandra user-defined type
type CassandraTypeField struct {
	Name     string `json:"name"`
	DataType string `json:"dataType"`
}

// AggregateInfo represents a Cassandra aggregate function
type AggregateInfo struct {
	Keyspace      string   `json:"keyspace"`
	Name          string   `json:"name"`
	ArgumentTypes []string `json:"argumentTypes"`
	StateType     string   `json:"stateType"`
	FinalFunc     string   `json:"finalFunc"`
	InitCond      string   `json:"initCond"`
	ReturnType    string   `json:"returnType"`
	StateFunc     string   `json:"stateFunc"`
}

// CassandraView represents a Cassandra materialized view
type CassandraView struct {
	Name        string            `json:"name"`
	Keyspace    string            `json:"keyspace"`
	BaseTable   string            `json:"baseTable"`
	Columns     []string          `json:"columns"`
	WhereClause string            `json:"whereClause"`
	Properties  map[string]string `json:"properties"`
}
