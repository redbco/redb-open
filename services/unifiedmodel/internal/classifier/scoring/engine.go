package scoring

import (
	"math"
	"sort"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/classifier/features"
)

// Category represents a functional category
type Category string

const (
	EntityStorage       Category = "entity_storage"
	RelationshipStorage Category = "relationship_storage"
	SchemaFlexible      Category = "schema_flexible"
	TimeSeries          Category = "time_series"
	FullTextSearch      Category = "full_text_search"
	VectorSimilarity    Category = "vector_similarity"
	MetadataSystem      Category = "metadata_system"
)

// CategoryScore represents a category with its score
type CategoryScore struct {
	Category   Category
	Score      float64
	Confidence float64
	Reason     string
}

// WeightMatrix defines weights for each category-feature combination
type WeightMatrix map[Category]map[string]float64

// ScoringEngine performs table classification
type ScoringEngine struct {
	weights WeightMatrix
}

// NewScoringEngine creates a new scoring engine with default weights
func NewScoringEngine() *ScoringEngine {
	return &ScoringEngine{
		weights: getDefaultWeights(),
	}
}

// NewScoringEngineWithWeights creates a scoring engine with custom weights
func NewScoringEngineWithWeights(weights WeightMatrix) *ScoringEngine {
	return &ScoringEngine{
		weights: weights,
	}
}

// Score calculates scores for all categories
func (e *ScoringEngine) Score(fv *features.FeatureVector) []CategoryScore {
	scores := make([]CategoryScore, 0, len(e.weights))

	for category, categoryWeights := range e.weights {
		score := e.calculateCategoryScore(fv, categoryWeights)
		reason := e.generateReason(category, fv)

		scores = append(scores, CategoryScore{
			Category:   category,
			Score:      score,
			Confidence: e.calculateConfidence(score, scores),
			Reason:     reason,
		})
	}

	// Sort by score descending
	sort.Slice(scores, func(i, j int) bool {
		return scores[i].Score > scores[j].Score
	})

	// Update confidence based on ranking
	for i := range scores {
		scores[i].Confidence = e.calculateConfidence(scores[i].Score, scores)
	}

	return scores
}

func (e *ScoringEngine) calculateCategoryScore(fv *features.FeatureVector, weights map[string]float64) float64 {
	score := 0.0

	// Use reflection-like approach to get feature values
	featureMap := map[string]float64{
		"column_count":        fv.ColumnCount,
		"integer_columns":     fv.IntegerColumns,
		"string_columns":      fv.StringColumns,
		"date_columns":        fv.DateColumns,
		"json_columns":        fv.JSONColumns,
		"vector_columns":      fv.VectorColumns,
		"has_primary_key":     fv.HasPrimaryKey,
		"has_foreign_keys":    fv.HasForeignKeys,
		"foreign_key_ratio":   fv.ForeignKeyRatio,
		"has_btree_index":     fv.HasBTreeIndex,
		"has_fulltext_index":  fv.HasFullTextIndex,
		"has_vector_index":    fv.HasVectorIndex,
		"has_ttl":             fv.HasTTL,
		"is_columnar":         fv.IsColumnar,
		"has_timestamps":      fv.HasTimestamps,
		"has_time_partition":  fv.HasTimePartition,
		"is_time_ranged":      fv.IsTimeRanged,
		"vector_dimension":    fv.VectorDimension,
		"has_distance_metric": fv.HasDistanceMetric,
		"is_read_heavy":       fv.IsReadHeavy,
		"is_write_heavy":      fv.IsWriteHeavy,
		"is_append_only":      fv.IsAppendOnly,
		"is_system_table":     fv.IsSystemTable,
		"has_variable_schema": fv.HasVariableSchema,
		"has_json_schema":     fv.HasJSONSchema,
		"has_graph_pattern":   fv.HasGraphPattern,
		"has_edge_pattern":    fv.HasEdgePattern,
	}

	for feature, weight := range weights {
		if value, exists := featureMap[feature]; exists {
			score += value * weight
		}
	}

	// Normalize score to [0, 1]
	return math.Max(0, math.Min(1, score))
}

func (e *ScoringEngine) calculateConfidence(score float64, allScores []CategoryScore) float64 {
	if len(allScores) < 2 {
		return score
	}

	// Confidence is based on the gap between this score and the next highest
	maxOtherScore := 0.0
	for _, other := range allScores {
		if other.Score != score && other.Score > maxOtherScore {
			maxOtherScore = other.Score
		}
	}

	gap := score - maxOtherScore
	return math.Max(0, math.Min(1, score+gap*0.5))
}

func (e *ScoringEngine) generateReason(category Category, fv *features.FeatureVector) string {
	switch category {
	case EntityStorage:
		if fv.HasPrimaryKey > 0 && fv.ForeignKeyRatio < 0.5 {
			return "Has primary key with low foreign key ratio, typical entity table"
		}
		return "Standard relational table structure"

	case RelationshipStorage:
		if fv.ForeignKeyRatio > 0.5 {
			return "High foreign key ratio indicates relationship/junction table"
		}
		return "Multiple foreign keys suggest associative storage"

	case SchemaFlexible:
		if fv.HasVariableSchema > 0 || fv.JSONColumns > 0.3 {
			return "Variable schema or high JSON column ratio"
		}
		return "Schema flexibility indicators present"

	case TimeSeries:
		if fv.HasTimestamps > 0 && (fv.HasTimePartition > 0 || fv.IsAppendOnly > 0) {
			return "Timestamp columns with time partitioning or append-only pattern"
		}
		return "Temporal data patterns detected"

	case FullTextSearch:
		if fv.HasFullTextIndex > 0 {
			return "Full-text search indexes present"
		}
		return "Text search capabilities indicated"

	case VectorSimilarity:
		if fv.VectorColumns > 0 && fv.HasVectorIndex > 0 {
			return "Vector columns with specialized vector indexes"
		}
		return "Vector similarity search features detected"

	case MetadataSystem:
		if fv.IsSystemTable > 0 {
			return "System table in reserved schema"
		}
		return "Metadata or system configuration characteristics"

	default:
		return "Classification based on feature analysis"
	}
}

// getDefaultWeights returns the default weight matrix
func getDefaultWeights() WeightMatrix {
	return WeightMatrix{
		EntityStorage: {
			"has_primary_key":     0.8,
			"integer_columns":     0.4,
			"string_columns":      0.4,
			"has_btree_index":     0.3,
			"foreign_key_ratio":   -0.5, // negative weight - fewer FKs = more entity-like
			"is_system_table":     -0.6,
			"has_variable_schema": -0.4,
		},
		RelationshipStorage: {
			"foreign_key_ratio": 0.9,
			"has_foreign_keys":  0.7,
			"column_count":      -0.3, // typically fewer columns
			"has_primary_key":   0.4,
			"integer_columns":   0.5,
			"is_system_table":   -0.5,
		},
		SchemaFlexible: {
			"has_variable_schema": 0.9,
			"json_columns":        0.8,
			"has_json_schema":     0.6,
			"has_primary_key":     -0.3,
			"has_btree_index":     -0.2,
		},
		TimeSeries: {
			"has_timestamps":     0.9,
			"has_time_partition": 0.8,
			"is_append_only":     0.7,
			"is_columnar":        0.6,
			"date_columns":       0.5,
			"has_ttl":            0.4,
			"is_write_heavy":     0.3,
		},
		FullTextSearch: {
			"has_fulltext_index": 0.9,
			"string_columns":     0.6,
			"has_json_schema":    0.4,
			"is_read_heavy":      0.3,
		},
		VectorSimilarity: {
			"vector_columns":      0.9,
			"has_vector_index":    0.8,
			"vector_dimension":    0.7,
			"has_distance_metric": 0.6,
			"is_read_heavy":       0.4,
		},
		MetadataSystem: {
			"is_system_table": 0.9,
			"string_columns":  0.4,
			"has_primary_key": 0.3,
			"column_count":    -0.2,
		},
	}
}
