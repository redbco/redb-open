package unifiedmodel

import (
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// ParadigmConversionStrategy defines high-level conversion approaches between paradigms
type ParadigmConversionStrategy struct {
	SourceParadigm     dbcapabilities.DataParadigm `json:"source_paradigm"`
	TargetParadigm     dbcapabilities.DataParadigm `json:"target_paradigm"`
	Strategy           ConversionStrategy          `json:"strategy"`
	RequiresEnrichment bool                        `json:"requires_enrichment"`
	EnrichmentTypes    []EnrichmentType            `json:"enrichment_types,omitempty"`

	// Common conversion patterns
	TableToCollectionRules []TableToCollectionRule `json:"table_to_collection_rules,omitempty"`
	RelationalToGraphRules []RelationalToGraphRule `json:"relational_to_graph_rules,omitempty"`
	DocumentToVectorRules  []DocumentToVectorRule  `json:"document_to_vector_rules,omitempty"`

	// User guidance
	UserGuidance     []string `json:"user_guidance"`
	CommonPitfalls   []string `json:"common_pitfalls"`
	RecommendedTools []string `json:"recommended_tools,omitempty"`

	// Success metrics
	TypicalSuccessRate float64 `json:"typical_success_rate,omitempty"`
	AverageComplexity  string  `json:"average_complexity,omitempty"`
}

// EnrichmentType defines types of enrichment data needed for conversions
type EnrichmentType string

const (
	EnrichmentTypeDataClassification EnrichmentType = "data_classification" // Table/column purpose
	EnrichmentTypeRelationships      EnrichmentType = "relationships"       // Entity relationships
	EnrichmentTypeAccessPatterns     EnrichmentType = "access_patterns"     // How data is accessed
	EnrichmentTypeBusinessRules      EnrichmentType = "business_rules"      // Business logic
	EnrichmentTypePerformanceHints   EnrichmentType = "performance_hints"   // Performance characteristics
	EnrichmentTypeDataFlow           EnrichmentType = "data_flow"           // Data flow patterns
)

// TableToCollectionRule defines how relational tables convert to document collections
type TableToCollectionRule struct {
	Condition          string             `json:"condition"`           // When to apply this rule
	CollectionStrategy CollectionStrategy `json:"collection_strategy"` // How to structure the collection
	EmbeddingStrategy  EmbeddingStrategy  `json:"embedding_strategy"`  // How to handle relationships
	RequiredEnrichment []EnrichmentType   `json:"required_enrichment,omitempty"`
	UserDecisions      []UserDecision     `json:"user_decisions,omitempty"`
}

// CollectionStrategy defines how to structure document collections
type CollectionStrategy string

const (
	CollectionStrategyOneToOne     CollectionStrategy = "one_to_one"   // One table = one collection
	CollectionStrategyAggregated   CollectionStrategy = "aggregated"   // Multiple tables = one collection
	CollectionStrategyNormalized   CollectionStrategy = "normalized"   // Maintain table structure
	CollectionStrategyDenormalized CollectionStrategy = "denormalized" // Flatten related data
	CollectionStrategyHierarchical CollectionStrategy = "hierarchical" // Create nested documents
)

// EmbeddingStrategy defines how to handle foreign key relationships
type EmbeddingStrategy string

const (
	EmbeddingStrategyEmbed     EmbeddingStrategy = "embed"     // Embed related documents
	EmbeddingStrategyReference EmbeddingStrategy = "reference" // Use document references
	EmbeddingStrategyHybrid    EmbeddingStrategy = "hybrid"    // Mix of embedding and references
	EmbeddingStrategyFlatten   EmbeddingStrategy = "flatten"   // Flatten into single document
)

// RelationalToGraphRule defines how relational schemas convert to graph structures
type RelationalToGraphRule struct {
	Condition          string            `json:"condition"`            // When to apply this rule
	TableConversion    GraphNodeStrategy `json:"table_conversion"`     // How tables become nodes
	ForeignKeyStrategy GraphEdgeStrategy `json:"foreign_key_strategy"` // How FKs become edges
	RequiredEnrichment []EnrichmentType  `json:"required_enrichment,omitempty"`
	UserDecisions      []UserDecision    `json:"user_decisions,omitempty"`
}

// GraphNodeStrategy defines how tables become graph nodes
type GraphNodeStrategy string

const (
	NodeStrategyEntityTables    GraphNodeStrategy = "entity_tables"    // Main entity tables → nodes
	NodeStrategyAllTables       GraphNodeStrategy = "all_tables"       // All tables → nodes
	NodeStrategyEnrichmentBased GraphNodeStrategy = "enrichment_based" // Use enrichment to decide
	NodeStrategyJunctionSplit   GraphNodeStrategy = "junction_split"   // Junction tables → edges
)

// GraphEdgeStrategy defines how foreign keys become graph edges
type GraphEdgeStrategy string

const (
	EdgeStrategyForeignKeys     GraphEdgeStrategy = "foreign_keys"     // FK relationships → edges
	EdgeStrategyJunctionTables  GraphEdgeStrategy = "junction_tables"  // Junction tables → edges
	EdgeStrategyEnrichmentBased GraphEdgeStrategy = "enrichment_based" // Use enrichment data
	EdgeStrategyHybrid          GraphEdgeStrategy = "hybrid"           // Multiple strategies
)

// DocumentToVectorRule defines how documents convert to vector representations
type DocumentToVectorRule struct {
	Condition          string           `json:"condition"`        // When to apply this rule
	VectorStrategy     VectorStrategy   `json:"vector_strategy"`  // How to create vectors
	EmbeddingSource    EmbeddingSource  `json:"embedding_source"` // What to embed
	RequiredEnrichment []EnrichmentType `json:"required_enrichment,omitempty"`
	UserDecisions      []UserDecision   `json:"user_decisions,omitempty"`
}

// VectorStrategy defines how to create vector representations
type VectorStrategy string

const (
	VectorStrategyTextEmbedding      VectorStrategy = "text_embedding"      // Embed text fields
	VectorStrategyNumericFeatures    VectorStrategy = "numeric_features"    // Use numeric features
	VectorStrategyMixedEmbedding     VectorStrategy = "mixed_embedding"     // Combine text and numeric
	VectorStrategySemanticContent    VectorStrategy = "semantic_content"    // Focus on semantic meaning
	VectorStrategyStructuralFeatures VectorStrategy = "structural_features" // Use document structure
)

// EmbeddingSource defines what content to use for embeddings
type EmbeddingSource string

const (
	EmbeddingSourceFullDocument EmbeddingSource = "full_document" // Entire document
	EmbeddingSourceKeyFields    EmbeddingSource = "key_fields"    // Important fields only
	EmbeddingSourceTextFields   EmbeddingSource = "text_fields"   // Text fields only
	EmbeddingSourceMetadata     EmbeddingSource = "metadata"      // Document metadata
	EmbeddingSourceCustom       EmbeddingSource = "custom"        // User-defined fields
)

// ParadigmPair represents a source-target paradigm pair
type ParadigmPair struct {
	Source dbcapabilities.DataParadigm
	Target dbcapabilities.DataParadigm
}

// ParadigmConversionRegistry is deprecated - use dynamic generation instead
// Kept for backward compatibility during transition
var ParadigmConversionRegistry = map[ParadigmPair]ParadigmConversionStrategy{}

// GetParadigmConversionStrategy returns conversion strategy for paradigm pair
func GetParadigmConversionStrategy(source, target dbcapabilities.DataParadigm) (ParadigmConversionStrategy, bool) {
	pair := ParadigmPair{Source: source, Target: target}
	strategy, exists := ParadigmConversionRegistry[pair]
	return strategy, exists
}

// GetRequiredEnrichmentTypes returns enrichment types needed for paradigm conversion
func GetRequiredEnrichmentTypes(source, target dbcapabilities.DataParadigm) []EnrichmentType {
	strategy, exists := GetParadigmConversionStrategy(source, target)
	if !exists {
		return nil
	}
	return strategy.EnrichmentTypes
}

// IsParadigmConversionSupported checks if conversion between paradigms is supported
func IsParadigmConversionSupported(source, target dbcapabilities.DataParadigm) bool {
	_, exists := GetParadigmConversionStrategy(source, target)
	return exists
}

// GetConversionGuidance returns user guidance for paradigm conversion
func GetConversionGuidance(source, target dbcapabilities.DataParadigm) []string {
	strategy, exists := GetParadigmConversionStrategy(source, target)
	if !exists {
		return []string{"Conversion between these paradigms is not currently supported"}
	}
	return strategy.UserGuidance
}

// GetCommonPitfalls returns common pitfalls for paradigm conversion
func GetCommonPitfalls(source, target dbcapabilities.DataParadigm) []string {
	strategy, exists := GetParadigmConversionStrategy(source, target)
	if !exists {
		return nil
	}
	return strategy.CommonPitfalls
}

// GetRecommendedTools returns recommended tools for paradigm conversion
func GetRecommendedTools(source, target dbcapabilities.DataParadigm) []string {
	strategy, exists := GetParadigmConversionStrategy(source, target)
	if !exists {
		return nil
	}
	return strategy.RecommendedTools
}

// Helper functions for creating paradigm conversion strategies

// CreateRelationalToDocumentStrategy creates strategy for relational to document conversion
func CreateRelationalToDocumentStrategy() ParadigmConversionStrategy {
	return ParadigmConversionStrategy{
		SourceParadigm:     dbcapabilities.ParadigmRelational,
		TargetParadigm:     dbcapabilities.ParadigmDocument,
		Strategy:           StrategyDenormalization,
		RequiresEnrichment: true,
		EnrichmentTypes: []EnrichmentType{
			EnrichmentTypeDataClassification,
			EnrichmentTypeRelationships,
			EnrichmentTypeAccessPatterns,
		},
		TableToCollectionRules: []TableToCollectionRule{
			{
				Condition:          "entity_table",
				CollectionStrategy: CollectionStrategyDenormalized,
				EmbeddingStrategy:  EmbeddingStrategyEmbed,
				RequiredEnrichment: []EnrichmentType{EnrichmentTypeDataClassification},
			},
			{
				Condition:          "junction_table",
				CollectionStrategy: CollectionStrategyAggregated,
				EmbeddingStrategy:  EmbeddingStrategyReference,
			},
		},
		UserGuidance: []string{
			"Consider denormalizing related data for better document structure",
			"Identify entity tables vs junction tables for optimal collection design",
			"Plan for eventual consistency when embedding related documents",
		},
		CommonPitfalls: []string{
			"Over-embedding can lead to large documents and update anomalies",
			"Under-embedding can result in too many queries",
			"Not considering access patterns can lead to poor performance",
		},
		TypicalSuccessRate: 0.85,
		AverageComplexity:  "moderate",
	}
}

// CreateRelationalToGraphStrategy creates strategy for relational to graph conversion
func CreateRelationalToGraphStrategy() ParadigmConversionStrategy {
	return ParadigmConversionStrategy{
		SourceParadigm:     dbcapabilities.ParadigmRelational,
		TargetParadigm:     dbcapabilities.ParadigmGraph,
		Strategy:           StrategyDecomposition,
		RequiresEnrichment: true,
		EnrichmentTypes: []EnrichmentType{
			EnrichmentTypeDataClassification,
			EnrichmentTypeRelationships,
			EnrichmentTypeBusinessRules,
		},
		RelationalToGraphRules: []RelationalToGraphRule{
			{
				Condition:          "entity_table",
				TableConversion:    NodeStrategyEntityTables,
				ForeignKeyStrategy: EdgeStrategyForeignKeys,
				RequiredEnrichment: []EnrichmentType{EnrichmentTypeDataClassification},
			},
			{
				Condition:          "junction_table",
				TableConversion:    NodeStrategyJunctionSplit,
				ForeignKeyStrategy: EdgeStrategyJunctionTables,
			},
		},
		UserGuidance: []string{
			"Identify entity tables that should become nodes",
			"Map foreign key relationships to graph edges",
			"Consider junction tables as either nodes or edge properties",
		},
		CommonPitfalls: []string{
			"Creating too many node types can complicate queries",
			"Not properly modeling many-to-many relationships",
			"Ignoring the semantic meaning of relationships",
		},
		TypicalSuccessRate: 0.75,
		AverageComplexity:  "complex",
	}
}

// CreateDocumentToVectorStrategy creates strategy for document to vector conversion
func CreateDocumentToVectorStrategy() ParadigmConversionStrategy {
	return ParadigmConversionStrategy{
		SourceParadigm:     dbcapabilities.ParadigmDocument,
		TargetParadigm:     dbcapabilities.ParadigmVector,
		Strategy:           StrategyAggregation,
		RequiresEnrichment: true,
		EnrichmentTypes: []EnrichmentType{
			EnrichmentTypeDataClassification,
			EnrichmentTypeAccessPatterns,
			EnrichmentTypePerformanceHints,
		},
		DocumentToVectorRules: []DocumentToVectorRule{
			{
				Condition:       "text_heavy_documents",
				VectorStrategy:  VectorStrategyTextEmbedding,
				EmbeddingSource: EmbeddingSourceTextFields,
			},
			{
				Condition:       "structured_documents",
				VectorStrategy:  VectorStrategyMixedEmbedding,
				EmbeddingSource: EmbeddingSourceKeyFields,
			},
		},
		UserGuidance: []string{
			"Identify which document fields contain meaningful semantic content",
			"Consider the intended use case for vector similarity search",
			"Plan for vector dimensionality and storage requirements",
		},
		CommonPitfalls: []string{
			"Including too many irrelevant fields in embeddings",
			"Not considering the semantic meaning of the data",
			"Choosing inappropriate vector dimensions",
		},
		TypicalSuccessRate: 0.70,
		AverageComplexity:  "complex",
	}
}
