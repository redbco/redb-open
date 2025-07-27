package features

import (
	"math"
	"strings"

	unifiedmodel "github.com/redbco/redb-open/api/proto/unifiedmodel/v1"
)

// FeatureVector represents extracted features from table metadata
type FeatureVector struct {
	// Schema shape
	ColumnCount    float64 `json:"column_count"`
	IntegerColumns float64 `json:"integer_columns"`
	StringColumns  float64 `json:"string_columns"`
	DateColumns    float64 `json:"date_columns"`
	JSONColumns    float64 `json:"json_columns"`
	VectorColumns  float64 `json:"vector_columns"`

	// Data modeling hints
	HasPrimaryKey   float64 `json:"has_primary_key"`
	HasForeignKeys  float64 `json:"has_foreign_keys"`
	ForeignKeyRatio float64 `json:"foreign_key_ratio"`

	// Indexing & storage
	HasBTreeIndex    float64 `json:"has_btree_index"`
	HasFullTextIndex float64 `json:"has_fulltext_index"`
	HasVectorIndex   float64 `json:"has_vector_index"`
	HasTTL           float64 `json:"has_ttl"`
	IsColumnar       float64 `json:"is_columnar"`

	// Temporal markers
	HasTimestamps    float64 `json:"has_timestamps"`
	HasTimePartition float64 `json:"has_time_partition"`
	IsTimeRanged     float64 `json:"is_time_ranged"`

	// Vector descriptors
	VectorDimension   float64 `json:"vector_dimension"`
	HasDistanceMetric float64 `json:"has_distance_metric"`

	// Access patterns
	IsReadHeavy  float64 `json:"is_read_heavy"`
	IsWriteHeavy float64 `json:"is_write_heavy"`
	IsAppendOnly float64 `json:"is_append_only"`

	// System vs user
	IsSystemTable float64 `json:"is_system_table"`

	// Document flexibility
	HasVariableSchema float64 `json:"has_variable_schema"`
	HasJSONSchema     float64 `json:"has_json_schema"`

	// Graph topology
	HasGraphPattern float64 `json:"has_graph_pattern"`
	HasEdgePattern  float64 `json:"has_edge_pattern"`
}

// Extractor extracts features from table metadata
type Extractor struct {
	timeKeywords []string
	vectorTypes  []string
}

// NewExtractor creates a new feature extractor
func NewExtractor() *Extractor {
	return &Extractor{
		timeKeywords: []string{
			"timestamp", "created_at", "updated_at", "time", "date",
			"datetime", "event_time", "occurred_at", "logged_at",
		},
		vectorTypes: []string{
			"vector", "embedding", "embeddings", "float[]", "real[]",
		},
	}
}

// Extract extracts features from table metadata
func (e *Extractor) Extract(metadata *unifiedmodel.TableMetadata) *FeatureVector {
	fv := &FeatureVector{}

	if len(metadata.Columns) == 0 {
		return fv
	}

	colCount := float64(len(metadata.Columns))
	fv.ColumnCount = e.normalize(colCount, 1, 100)

	// Schema shape analysis
	var intCols, stringCols, dateCols, jsonCols, vectorCols float64
	var pkCount, fkCount float64

	for _, col := range metadata.Columns {
		// Type analysis
		colType := strings.ToLower(col.Type)

		if e.isIntegerType(colType) {
			intCols++
		} else if e.isStringType(colType) {
			stringCols++
		} else if e.isDateType(colType) {
			dateCols++
		} else if e.isJSONType(colType) {
			jsonCols++
		} else if e.isVectorType(colType) {
			vectorCols++
			if col.VectorDimension > 0 {
				fv.VectorDimension = e.normalize(float64(col.VectorDimension), 1, 4096)
			}
			if col.VectorDistanceMetric != "" {
				fv.HasDistanceMetric = 1.0
			}
		}

		// Key analysis
		if col.IsPrimaryKey {
			pkCount++
		}
		if col.IsForeignKey {
			fkCount++
		}

		// Temporal analysis
		if e.isTemporalColumn(col.Name) {
			fv.HasTimestamps = 1.0
		}

		// Index analysis
		for _, idx := range col.Indexes {
			idxType := strings.ToLower(idx)
			if strings.Contains(idxType, "btree") {
				fv.HasBTreeIndex = 1.0
			} else if strings.Contains(idxType, "fulltext") || strings.Contains(idxType, "gin") {
				fv.HasFullTextIndex = 1.0
			} else if strings.Contains(idxType, "vector") || strings.Contains(idxType, "ivf") {
				fv.HasVectorIndex = 1.0
			}
		}
	}

	// Normalize column type ratios
	fv.IntegerColumns = intCols / colCount
	fv.StringColumns = stringCols / colCount
	fv.DateColumns = dateCols / colCount
	fv.JSONColumns = jsonCols / colCount
	fv.VectorColumns = vectorCols / colCount

	// Key analysis
	fv.HasPrimaryKey = e.boolToFloat(pkCount > 0)
	fv.HasForeignKeys = e.boolToFloat(fkCount > 0)
	fv.ForeignKeyRatio = fkCount / colCount

	// Properties analysis
	for key, value := range metadata.Properties {
		keyLower := strings.ToLower(key)
		valueLower := strings.ToLower(value)

		if strings.Contains(keyLower, "ttl") || strings.Contains(keyLower, "expire") {
			fv.HasTTL = 1.0
		}
		if strings.Contains(keyLower, "partition") && e.isTemporalValue(valueLower) {
			fv.HasTimePartition = 1.0
		}
		if strings.Contains(keyLower, "columnar") || strings.Contains(valueLower, "columnar") {
			fv.IsColumnar = 1.0
		}
	}

	// Engine-specific analysis
	e.analyzeEngine(metadata, fv)

	// Access pattern analysis
	switch strings.ToLower(metadata.AccessPattern) {
	case "read_heavy":
		fv.IsReadHeavy = 1.0
	case "write_heavy":
		fv.IsWriteHeavy = 1.0
	case "append_only":
		fv.IsAppendOnly = 1.0
	}

	// System table detection
	fv.IsSystemTable = e.boolToFloat(metadata.IsSystemTable || e.isSystemSchema(metadata.Schema))

	// Document flexibility (mainly for NoSQL)
	if e.isDocumentDB(metadata.Engine) {
		fv.HasVariableSchema = 1.0
		if fv.JSONColumns > 0 {
			fv.HasJSONSchema = 1.0
		}
	}

	// Graph patterns
	if e.isGraphDB(metadata.Engine) {
		fv.HasGraphPattern = 1.0
		if e.hasEdgePattern(metadata) {
			fv.HasEdgePattern = 1.0
		}
	}

	return fv
}

func (e *Extractor) isIntegerType(colType string) bool {
	return strings.Contains(colType, "int") || strings.Contains(colType, "serial") ||
		strings.Contains(colType, "bigserial") || colType == "number"
}

func (e *Extractor) isStringType(colType string) bool {
	return strings.Contains(colType, "varchar") || strings.Contains(colType, "char") ||
		strings.Contains(colType, "text") || strings.Contains(colType, "string")
}

func (e *Extractor) isDateType(colType string) bool {
	return strings.Contains(colType, "timestamp") || strings.Contains(colType, "date") ||
		strings.Contains(colType, "time") || strings.Contains(colType, "datetime")
}

func (e *Extractor) isJSONType(colType string) bool {
	return strings.Contains(colType, "json") || strings.Contains(colType, "jsonb")
}

func (e *Extractor) isVectorType(colType string) bool {
	for _, vt := range e.vectorTypes {
		if strings.Contains(colType, vt) {
			return true
		}
	}
	return false
}

func (e *Extractor) isTemporalColumn(name string) bool {
	nameLower := strings.ToLower(name)
	for _, keyword := range e.timeKeywords {
		if strings.Contains(nameLower, keyword) {
			return true
		}
	}
	return false
}

func (e *Extractor) isTemporalValue(value string) bool {
	return strings.Contains(value, "time") || strings.Contains(value, "date") ||
		strings.Contains(value, "day") || strings.Contains(value, "hour")
}

func (e *Extractor) isSystemSchema(schema string) bool {
	systemSchemas := []string{
		"information_schema", "pg_catalog", "sys", "system",
		"performance_schema", "mysql", "__system__",
	}
	schemaLower := strings.ToLower(schema)
	for _, sys := range systemSchemas {
		if schemaLower == sys {
			return true
		}
	}
	return false
}

func (e *Extractor) isDocumentDB(engine string) bool {
	docDBs := []string{"mongodb", "cosmosdb", "dynamodb"}
	engineLower := strings.ToLower(engine)
	for _, db := range docDBs {
		if strings.Contains(engineLower, db) {
			return true
		}
	}
	return false
}

func (e *Extractor) isGraphDB(engine string) bool {
	graphDBs := []string{"neo4j", "edgedb"}
	engineLower := strings.ToLower(engine)
	for _, db := range graphDBs {
		if strings.Contains(engineLower, db) {
			return true
		}
	}
	return false
}

func (e *Extractor) hasEdgePattern(metadata *unifiedmodel.TableMetadata) bool {
	nameLower := strings.ToLower(metadata.Name)
	return strings.Contains(nameLower, "edge") || strings.Contains(nameLower, "relationship") ||
		strings.Contains(nameLower, "link") || len(metadata.Columns) == 3 // typical edge table
}

func (e *Extractor) analyzeEngine(metadata *unifiedmodel.TableMetadata, fv *FeatureVector) {
	engineLower := strings.ToLower(metadata.Engine)

	switch {
	case strings.Contains(engineLower, "clickhouse"):
		fv.IsColumnar = 1.0
		fv.IsAppendOnly = 1.0
	case strings.Contains(engineLower, "cassandra"):
		fv.HasTimePartition = 1.0
		fv.IsWriteHeavy = 1.0
	case strings.Contains(engineLower, "elasticsearch"):
		fv.HasFullTextIndex = 1.0
		fv.HasJSONSchema = 1.0
	case strings.Contains(engineLower, "pinecone") || strings.Contains(engineLower, "milvus") ||
		strings.Contains(engineLower, "weaviate") || strings.Contains(engineLower, "chroma"):
		fv.HasVectorIndex = 1.0
		fv.VectorColumns = 1.0
	case strings.Contains(engineLower, "redis"):
		fv.IsReadHeavy = 1.0
		fv.HasTTL = 1.0
	}
}

func (e *Extractor) normalize(value, min, max float64) float64 {
	if max == min {
		return 0
	}
	normalized := (value - min) / (max - min)
	return math.Max(0, math.Min(1, normalized))
}

func (e *Extractor) boolToFloat(b bool) float64 {
	if b {
		return 1.0
	}
	return 0.0
}
