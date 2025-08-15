// Package unifiedmodel enrichment provides metadata analysis and context for schema objects.
// This package handles enrichment properties that are derived from analysis but are not
// part of the core schema definition. Enrichments include:
//
// - Privileged data detection (PII, financial, medical data classification)
// - Table classification (analytical, transactional, time-series patterns)
// - Performance and access pattern analysis
// - Compliance and risk assessment
// - Conversion hints and recommendations

package unifiedmodel

import (
	"time"
)

// ConversionPriority indicates the importance level of a conversion hint
type ConversionPriority string

const (
	ConversionPriorityHigh   ConversionPriority = "high"
	ConversionPriorityMedium ConversionPriority = "medium"
	ConversionPriorityLow    ConversionPriority = "low"
)

// RiskLevel indicates the risk level associated with data exposure
type RiskLevel string

const (
	RiskLevelCritical RiskLevel = "critical"
	RiskLevelHigh     RiskLevel = "high"
	RiskLevelMedium   RiskLevel = "medium"
	RiskLevelLow      RiskLevel = "low"
	RiskLevelMinimal  RiskLevel = "minimal"
)

// DataCategory represents the type of data detected in a column
type DataCategory string

const (
	// Personal Information
	DataCategoryPII       DataCategory = "pii"
	DataCategoryEmail     DataCategory = "email"
	DataCategoryPhone     DataCategory = "phone"
	DataCategoryAddress   DataCategory = "address"
	DataCategoryName      DataCategory = "name"
	DataCategoryBirthDate DataCategory = "birth_date"

	// Government IDs
	DataCategorySSN            DataCategory = "ssn"
	DataCategoryPassport       DataCategory = "passport"
	DataCategoryDriversLicense DataCategory = "drivers_license"
	DataCategoryNationalID     DataCategory = "national_id"

	// Financial Information
	DataCategoryFinancial     DataCategory = "financial"
	DataCategoryCreditCard    DataCategory = "credit_card"
	DataCategoryBankAccount   DataCategory = "bank_account"
	DataCategoryRoutingNumber DataCategory = "routing_number"
	DataCategoryIBAN          DataCategory = "iban"
	DataCategorySWIFT         DataCategory = "swift"

	// Medical Information
	DataCategoryMedical       DataCategory = "medical"
	DataCategoryMedicalRecord DataCategory = "medical_record"
	DataCategoryInsuranceID   DataCategory = "insurance_id"
	DataCategoryNPI           DataCategory = "npi"

	// Technical/Network
	DataCategoryTechnical  DataCategory = "technical"
	DataCategoryIPAddress  DataCategory = "ip_address"
	DataCategoryMACAddress DataCategory = "mac_address"
	DataCategoryURL        DataCategory = "url"

	// Authentication/Security
	DataCategoryAuth     DataCategory = "auth"
	DataCategoryPassword DataCategory = "password"
	DataCategoryToken    DataCategory = "token"
	DataCategoryAPIKey   DataCategory = "api_key"

	// Business Data
	DataCategoryBusiness    DataCategory = "business"
	DataCategoryCustomerID  DataCategory = "customer_id"
	DataCategoryOrderID     DataCategory = "order_id"
	DataCategoryProductCode DataCategory = "product_code"
)

// TableCategory represents the classification of a table's purpose and usage pattern
type TableCategory string

const (
	TableCategoryTransactional TableCategory = "transactional"
	TableCategoryAnalytical    TableCategory = "analytical"
	TableCategoryTimeSeries    TableCategory = "time_series"
	TableCategoryReference     TableCategory = "reference"
	TableCategoryLog           TableCategory = "log"
	TableCategoryAudit         TableCategory = "audit"
	TableCategoryCache         TableCategory = "cache"
	TableCategoryStaging       TableCategory = "staging"
	TableCategoryArchive       TableCategory = "archive"
	TableCategoryConfiguration TableCategory = "configuration"
	TableCategoryMetadata      TableCategory = "metadata"
	TableCategorySearch        TableCategory = "search"
	TableCategoryQueue         TableCategory = "queue"
	TableCategorySession       TableCategory = "session"
)

// AccessPattern represents how a table is primarily accessed
type AccessPattern string

const (
	AccessPatternReadHeavy  AccessPattern = "read_heavy"
	AccessPatternWriteHeavy AccessPattern = "write_heavy"
	AccessPatternAppendOnly AccessPattern = "append_only"
	AccessPatternReadWrite  AccessPattern = "read_write"
	AccessPatternBatch      AccessPattern = "batch"
	AccessPatternRealTime   AccessPattern = "real_time"
)

// ComplianceFramework represents regulatory compliance requirements
type ComplianceFramework string

const (
	ComplianceGDPR     ComplianceFramework = "gdpr"
	ComplianceHIPAA    ComplianceFramework = "hipaa"
	CompliancePCI      ComplianceFramework = "pci"
	ComplianceSOX      ComplianceFramework = "sox"
	ComplianceCCPA     ComplianceFramework = "ccpa"
	ComplianceFERPA    ComplianceFramework = "ferpa"
	ComplianceISO27001 ComplianceFramework = "iso27001"
)

// UnifiedModelEnrichment contains analysis-derived metadata for a UnifiedModel
// This structure is separate from the core schema to enable:
// - Exclusion from structural comparisons
// - Independent versioning of analysis algorithms
// - Contextual guidance for schema conversion
type UnifiedModelEnrichment struct {
	// Metadata
	SchemaID          string    `json:"schema_id"`          // Links to UnifiedModel instance
	EnrichmentVersion string    `json:"enrichment_version"` // Version of enrichment algorithms used
	GeneratedAt       time.Time `json:"generated_at"`       // When enrichment was computed
	GeneratedBy       string    `json:"generated_by"`       // Service/component that generated enrichment

	// Object-level enrichments (keyed by object name)
	TableEnrichments  map[string]TableEnrichment  `json:"table_enrichments"`
	ColumnEnrichments map[string]ColumnEnrichment `json:"column_enrichments"` // Key format: "table_name.column_name"
	IndexEnrichments  map[string]IndexEnrichment  `json:"index_enrichments"`
	ViewEnrichments   map[string]ViewEnrichment   `json:"view_enrichments"`

	// Graph and document enrichments
	NodeEnrichments         map[string]NodeEnrichment         `json:"node_enrichments"`
	RelationshipEnrichments map[string]RelationshipEnrichment `json:"relationship_enrichments"`
	CollectionEnrichments   map[string]CollectionEnrichment   `json:"collection_enrichments"`
	DocumentEnrichments     map[string]DocumentEnrichment     `json:"document_enrichments"`

	// Global analysis results
	ComplianceSummary ComplianceSummary `json:"compliance_summary"`
	RiskAssessment    RiskAssessment    `json:"risk_assessment"`
	Recommendations   []Recommendation  `json:"recommendations"`
	PerformanceHints  []PerformanceHint `json:"performance_hints"`
}

// TableEnrichment contains analysis metadata for a table
type TableEnrichment struct {
	// Classification
	PrimaryCategory          TableCategory   `json:"primary_category"`
	ClassificationConfidence float64         `json:"classification_confidence"` // 0.0-1.0
	ClassificationScores     []CategoryScore `json:"classification_scores"`     // All category scores

	// Access patterns and performance
	AccessPattern   AccessPattern `json:"access_pattern"`
	EstimatedRows   *int64        `json:"estimated_rows,omitempty"`
	GrowthRate      *float64      `json:"growth_rate,omitempty"`      // Estimated rows per day
	QueryComplexity *float64      `json:"query_complexity,omitempty"` // 0.0-1.0, simple to complex

	// Data characteristics
	HasPrivilegedData bool     `json:"has_privileged_data"`
	PrivilegedColumns []string `json:"privileged_columns,omitempty"`
	DataSensitivity   float64  `json:"data_sensitivity"` // 0.0-1.0, low to high

	// Optimization hints
	RecommendedIndexes    []IndexRecommendation     `json:"recommended_indexes,omitempty"`
	RecommendedPartitions []PartitionRecommendation `json:"recommended_partitions,omitempty"`

	// Relationships
	RelatedTables   []string `json:"related_tables,omitempty"`   // Tables with FK relationships
	DependentTables []string `json:"dependent_tables,omitempty"` // Tables that depend on this one

	// Metadata
	BusinessPurpose string            `json:"business_purpose,omitempty"`
	DataRetention   *time.Duration    `json:"data_retention,omitempty"`
	Tags            []string          `json:"tags,omitempty"`
	Context         map[string]string `json:"context,omitempty"`
}

// ColumnEnrichment contains analysis metadata for a column
type ColumnEnrichment struct {
	// Privileged data detection
	IsPrivilegedData      bool                  `json:"is_privileged_data"`
	DataCategory          DataCategory          `json:"data_category,omitempty"`
	SubCategory           string                `json:"sub_category,omitempty"`
	PrivilegedConfidence  float64               `json:"privileged_confidence"` // 0.0-1.0
	PrivilegedDescription string                `json:"privileged_description,omitempty"`
	RiskLevel             RiskLevel             `json:"risk_level,omitempty"`
	ComplianceImpact      []ComplianceFramework `json:"compliance_impact,omitempty"`

	// Data characteristics
	Cardinality      *int64   `json:"cardinality,omitempty"`       // Estimated unique values
	NullPercentage   *float64 `json:"null_percentage,omitempty"`   // 0.0-1.0
	DataDistribution *string  `json:"data_distribution,omitempty"` // uniform, skewed, normal
	ValuePatterns    []string `json:"value_patterns,omitempty"`    // Regex patterns found in data
	SampleValues     []string `json:"sample_values,omitempty"`     // Anonymized sample values

	// Usage patterns
	IsSearchable   bool     `json:"is_searchable"`             // Frequently used in WHERE clauses
	IsFilterable   bool     `json:"is_filterable"`             // Used for filtering/grouping
	IsSortable     bool     `json:"is_sortable"`               // Used in ORDER BY
	QueryFrequency *float64 `json:"query_frequency,omitempty"` // Queries per day

	// Optimization hints
	RecommendedIndexType IndexType `json:"recommended_index_type,omitempty"`
	ShouldEncrypt        bool      `json:"should_encrypt"`
	ShouldMask           bool      `json:"should_mask"`

	// Relationships
	IsForeignKey      bool     `json:"is_foreign_key"`
	ReferencedTable   string   `json:"referenced_table,omitempty"`
	ReferencedColumn  string   `json:"referenced_column,omitempty"`
	ReferencingTables []string `json:"referencing_tables,omitempty"` // Tables that reference this column

	// Quality metrics
	DataQualityScore  *float64 `json:"data_quality_score,omitempty"` // 0.0-1.0
	CompletenessScore *float64 `json:"completeness_score,omitempty"` // 0.0-1.0
	ConsistencyScore  *float64 `json:"consistency_score,omitempty"`  // 0.0-1.0

	// Metadata
	BusinessMeaning string            `json:"business_meaning,omitempty"`
	Tags            []string          `json:"tags,omitempty"`
	Context         map[string]string `json:"context,omitempty"`
}

// IndexEnrichment contains analysis metadata for an index
type IndexEnrichment struct {
	// Usage statistics
	UsageFrequency  *float64 `json:"usage_frequency,omitempty"`  // Queries per day using this index
	Selectivity     *float64 `json:"selectivity,omitempty"`      // 0.0-1.0, low to high selectivity
	EfficiencyScore *float64 `json:"efficiency_score,omitempty"` // 0.0-1.0

	// Performance characteristics
	ScanRatio       *float64 `json:"scan_ratio,omitempty"`       // Percentage of scans vs seeks
	MaintenanceCost *float64 `json:"maintenance_cost,omitempty"` // Cost of index maintenance
	StorageOverhead *int64   `json:"storage_overhead,omitempty"` // Bytes of storage overhead

	// Recommendations
	IsRedundant       bool     `json:"is_redundant"` // Covered by another index
	ShouldDrop        bool     `json:"should_drop"`  // Recommendation to drop
	OptimizationHints []string `json:"optimization_hints,omitempty"`

	// Context
	CreatedFor string            `json:"created_for,omitempty"` // Query pattern this was created for
	Context    map[string]string `json:"context,omitempty"`
}

// ViewEnrichment contains analysis metadata for views
type ViewEnrichment struct {
	// Complexity analysis
	ComplexityScore   float64  `json:"complexity_score"`   // 0.0-1.0
	QueryDepth        int      `json:"query_depth"`        // Nested query levels
	TableDependencies []string `json:"table_dependencies"` // Tables referenced
	ViewDependencies  []string `json:"view_dependencies"`  // Other views referenced

	// Performance characteristics
	EstimatedRowsReturned *int64   `json:"estimated_rows_returned,omitempty"`
	ExecutionCost         *float64 `json:"execution_cost,omitempty"`
	IsOptimizable         bool     `json:"is_optimizable"`

	// Usage patterns
	AccessFrequency  *float64 `json:"access_frequency,omitempty"` // Queries per day
	IsMaterializable bool     `json:"is_materializable"`          // Good candidate for materialization

	// Context
	BusinessPurpose string            `json:"business_purpose,omitempty"`
	Context         map[string]string `json:"context,omitempty"`
}

// NodeEnrichment contains analysis metadata for graph nodes
type NodeEnrichment struct {
	// Graph characteristics
	InDegree           int      `json:"in_degree"`                     // Number of incoming relationships
	OutDegree          int      `json:"out_degree"`                    // Number of outgoing relationships
	Centrality         *float64 `json:"centrality,omitempty"`          // Centrality score in graph
	ClusterCoefficient *float64 `json:"cluster_coefficient,omitempty"` // Local clustering coefficient

	// Data characteristics
	HasPrivilegedData    bool                  `json:"has_privileged_data"`
	PrivilegedProperties []string              `json:"privileged_properties,omitempty"`
	ComplianceImpact     []ComplianceFramework `json:"compliance_impact,omitempty"`

	// Performance hints
	IsFrequentlyQueried bool     `json:"is_frequently_queried"`
	ShouldIndex         []string `json:"should_index,omitempty"` // Properties to index

	// Context
	BusinessRole string            `json:"business_role,omitempty"`
	Context      map[string]string `json:"context,omitempty"`
}

// RelationshipEnrichment contains analysis metadata for graph relationships
type RelationshipEnrichment struct {
	// Relationship characteristics
	Frequency     *int64   `json:"frequency,omitempty"` // Number of instances
	Strength      *float64 `json:"strength,omitempty"`  // 0.0-1.0, weak to strong
	IsDirectional bool     `json:"is_directional"`

	// Performance characteristics
	QueryFrequency *float64 `json:"query_frequency,omitempty"` // Traversals per day
	TraversalCost  *float64 `json:"traversal_cost,omitempty"`  // Cost of traversing

	// Context
	BusinessMeaning string            `json:"business_meaning,omitempty"`
	Context         map[string]string `json:"context,omitempty"`
}

// CollectionEnrichment contains analysis metadata for document collections
type CollectionEnrichment struct {
	// Document characteristics
	DocumentCount       *int64   `json:"document_count,omitempty"`
	AverageDocumentSize *int64   `json:"average_document_size,omitempty"` // Bytes
	SchemaFlexibility   *float64 `json:"schema_flexibility,omitempty"`    // 0.0-1.0, rigid to flexible

	// Field analysis
	CommonFields      []string `json:"common_fields,omitempty"` // Fields present in >90% of docs
	RareFields        []string `json:"rare_fields,omitempty"`   // Fields present in <10% of docs
	HasPrivilegedData bool     `json:"has_privileged_data"`
	PrivilegedFields  []string `json:"privileged_fields,omitempty"`

	// Performance characteristics
	QueryPatterns      []string `json:"query_patterns,omitempty"` // Common query types
	RecommendedIndexes []string `json:"recommended_indexes,omitempty"`

	// Context
	BusinessPurpose string            `json:"business_purpose,omitempty"`
	Context         map[string]string `json:"context,omitempty"`
}

// DocumentEnrichment contains analysis metadata for specific documents
type DocumentEnrichment struct {
	// Content analysis
	Size              int64                 `json:"size"`         // Document size in bytes
	FieldCount        int                   `json:"field_count"`  // Number of fields
	NestedDepth       int                   `json:"nested_depth"` // Maximum nesting level
	HasPrivilegedData bool                  `json:"has_privileged_data"`
	PrivilegedFields  []string              `json:"privileged_fields,omitempty"`
	ComplianceImpact  []ComplianceFramework `json:"compliance_impact,omitempty"`

	// Context
	DocumentType string            `json:"document_type,omitempty"`
	Context      map[string]string `json:"context,omitempty"`
}

// CategoryScore represents a classification category and its confidence score
type CategoryScore struct {
	Category string   `json:"category"`           // Category name
	Score    float64  `json:"score"`              // Confidence score 0.0-1.0
	Reason   string   `json:"reason"`             // Explanation for the score
	Evidence []string `json:"evidence,omitempty"` // Supporting evidence
}

// ComplianceSummary provides regulatory compliance analysis
type ComplianceSummary struct {
	// Framework-specific findings
	GDPRFindings  []ComplianceFinding `json:"gdpr_findings,omitempty"`
	HIPAAFindings []ComplianceFinding `json:"hipaa_findings,omitempty"`
	PCIFindings   []ComplianceFinding `json:"pci_findings,omitempty"`
	SOXFindings   []ComplianceFinding `json:"sox_findings,omitempty"`
	CCPAFindings  []ComplianceFinding `json:"ccpa_findings,omitempty"`

	// Overall assessment
	OverallRiskLevel   RiskLevel             `json:"overall_risk_level"`
	RequiredFrameworks []ComplianceFramework `json:"required_frameworks"`
	RecommendedActions []string              `json:"recommended_actions"`
	ComplianceScore    float64               `json:"compliance_score"` // 0.0-1.0
}

// ComplianceFinding represents a specific compliance-related finding
type ComplianceFinding struct {
	Framework   ComplianceFramework `json:"framework"`
	Severity    RiskLevel           `json:"severity"`
	ObjectType  string              `json:"object_type"` // table, column, index, etc.
	ObjectName  string              `json:"object_name"`
	Finding     string              `json:"finding"`     // Description of the issue
	Requirement string              `json:"requirement"` // Specific compliance requirement
	Remediation string              `json:"remediation"` // How to fix the issue
}

// RiskAssessment provides overall risk analysis
type RiskAssessment struct {
	OverallRiskScore     float64      `json:"overall_risk_score"`    // 0.0-1.0
	PrivacyRiskScore     float64      `json:"privacy_risk_score"`    // 0.0-1.0
	SecurityRiskScore    float64      `json:"security_risk_score"`   // 0.0-1.0
	ComplianceRiskScore  float64      `json:"compliance_risk_score"` // 0.0-1.0
	HighRiskObjects      []RiskObject `json:"high_risk_objects"`
	CriticalFindings     []string     `json:"critical_findings"`
	MitigationStrategies []string     `json:"mitigation_strategies"`
}

// RiskObject represents an object with associated risk
type RiskObject struct {
	ObjectType  string    `json:"object_type"`
	ObjectName  string    `json:"object_name"`
	RiskLevel   RiskLevel `json:"risk_level"`
	RiskFactors []string  `json:"risk_factors"`
	ImpactScore float64   `json:"impact_score"` // 0.0-1.0
}

// Recommendation represents an actionable recommendation
type Recommendation struct {
	ID                 string             `json:"id"`
	Type               RecommendationType `json:"type"`
	Priority           ConversionPriority `json:"priority"`
	Title              string             `json:"title"`
	Description        string             `json:"description"`
	ObjectType         string             `json:"object_type,omitempty"`
	ObjectName         string             `json:"object_name,omitempty"`
	Action             string             `json:"action"`           // Specific action to take
	Rationale          string             `json:"rationale"`        // Why this is recommended
	EstimatedImpact    string             `json:"estimated_impact"` // Expected benefit
	ImplementationHint string             `json:"implementation_hint,omitempty"`
}

// RecommendationType categorizes recommendations
type RecommendationType string

const (
	RecommendationTypePerformance  RecommendationType = "performance"
	RecommendationTypeSecurity     RecommendationType = "security"
	RecommendationTypeCompliance   RecommendationType = "compliance"
	RecommendationTypeOptimization RecommendationType = "optimization"
	RecommendationTypeMigration    RecommendationType = "migration"
	RecommendationTypeDataQuality  RecommendationType = "data_quality"
)

// PerformanceHint provides database-specific performance optimization suggestions
type PerformanceHint struct {
	TargetDatabase   string             `json:"target_database"` // Database technology this applies to
	Category         string             `json:"category"`        // indexing, partitioning, etc.
	Priority         ConversionPriority `json:"priority"`
	Hint             string             `json:"hint"`              // The performance suggestion
	ObjectPath       string             `json:"object_path"`       // Path to the object (e.g., "tables.users.columns.email")
	EstimatedBenefit string             `json:"estimated_benefit"` // Expected performance improvement
	Context          map[string]string  `json:"context,omitempty"`
}

// IndexRecommendation suggests creating an index
type IndexRecommendation struct {
	Columns          []string           `json:"columns"`
	IndexType        IndexType          `json:"index_type"`
	Reason           string             `json:"reason"`
	EstimatedBenefit string             `json:"estimated_benefit"`
	Priority         ConversionPriority `json:"priority"`
}

// PartitionRecommendation suggests partitioning a table
type PartitionRecommendation struct {
	PartitionType    string             `json:"partition_type"` // range, hash, list
	PartitionKey     []string           `json:"partition_key"`
	Reason           string             `json:"reason"`
	EstimatedBenefit string             `json:"estimated_benefit"`
	Priority         ConversionPriority `json:"priority"`
}
