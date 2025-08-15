package unifiedmodel

import (
	"fmt"
	"time"
)

// DetectionLevel represents the three levels of privileged data detection
type DetectionLevel string

const (
	// DetectionLevelSchema uses only schema information (column names, types, constraints)
	DetectionLevelSchema DetectionLevel = "schema"

	// DetectionLevelEnriched uses schema + enrichment metadata (data classification, purpose, context)
	DetectionLevelEnriched DetectionLevel = "enriched"

	// DetectionLevelFull uses schema + enrichment + sample data (actual values for pattern analysis)
	DetectionLevelFull DetectionLevel = "full"
)

// DetectionRequest defines parameters for privileged data detection
type DetectionRequest struct {
	// Core inputs
	Schema     *UnifiedModel           `json:"schema"`
	Enrichment *UnifiedModelEnrichment `json:"enrichment,omitempty"`
	SampleData *UnifiedModelSampleData `json:"sample_data,omitempty"`

	// Detection configuration
	Level               DetectionLevel  `json:"level"`
	ConfidenceThreshold float64         `json:"confidence_threshold"` // Minimum confidence to report finding
	IncludeContext      bool            `json:"include_context"`      // Include surrounding column context
	FastMode            bool            `json:"fast_mode"`            // Skip expensive analysis
	CustomPatterns      []CustomPattern `json:"custom_patterns,omitempty"`
	ExcludePatterns     []string        `json:"exclude_patterns,omitempty"`

	// Privacy and compliance
	ComplianceFrameworks  []string `json:"compliance_frameworks,omitempty"` // GDPR, HIPAA, PCI-DSS, etc.
	RedactFindings        bool     `json:"redact_findings"`                 // Redact sensitive examples
	MaxExamplesPerFinding int      `json:"max_examples_per_finding"`

	// Metadata
	RequestID   string    `json:"request_id"`
	RequestedBy string    `json:"requested_by"`
	RequestedAt time.Time `json:"requested_at"`
}

// DetectionResponse contains the results of privileged data detection
type DetectionResponse struct {
	// Request metadata
	RequestID      string         `json:"request_id"`
	Level          DetectionLevel `json:"level"`
	ProcessedAt    time.Time      `json:"processed_at"`
	ProcessingTime time.Duration  `json:"processing_time"`

	// Core results
	Findings []PrivilegedDataFinding `json:"findings"`
	Summary  DetectionSummary        `json:"summary"`
	Warnings []string                `json:"warnings"`
	Errors   []string                `json:"errors,omitempty"`

	// Analysis insights
	Recommendations []DetectionRecommendation `json:"recommendations"`
	ComplianceMap   map[string][]string       `json:"compliance_map"` // Framework -> affected findings
	RiskAssessment  RiskAssessment            `json:"risk_assessment"`

	// Detection metadata
	TotalObjectsScanned int      `json:"total_objects_scanned"`
	ScanCoverage        float64  `json:"scan_coverage"`                // Percentage of schema covered
	DetectionAccuracy   *float64 `json:"detection_accuracy,omitempty"` // If validation data available
}

// PrivilegedDataFinding represents a detected instance of privileged data
type PrivilegedDataFinding struct {
	// Location information
	ObjectType string `json:"object_type"` // "table", "collection", "key_space", etc.
	ObjectName string `json:"object_name"`
	FieldName  string `json:"field_name"` // Column, field, or key name
	FieldPath  string `json:"field_path"` // Full path for nested structures

	// Classification
	DataCategory string  `json:"data_category"` // "personal_info", "financial", "health", etc.
	SubCategory  string  `json:"sub_category"`  // More specific classification
	PiiType      string  `json:"pii_type"`      // "email", "ssn", "credit_card", etc.
	Confidence   float64 `json:"confidence"`    // 0.0-1.0

	// Evidence and context
	DetectionMethod string     `json:"detection_method"` // "schema", "enrichment", "sample_data"
	Evidence        []Evidence `json:"evidence"`         // Supporting evidence for the finding
	Context         []string   `json:"context"`          // Contextual information (related fields, etc.)

	// Risk and compliance
	RiskLevel        string   `json:"risk_level"`        // "low", "medium", "high", "critical"
	ComplianceImpact []string `json:"compliance_impact"` // Affected regulations
	Sensitivity      string   `json:"sensitivity"`       // "public", "internal", "confidential", "restricted"

	// Sample data (if available and not redacted)
	ExampleValues []string `json:"example_values,omitempty"`
	ValuePatterns []string `json:"value_patterns,omitempty"`

	// Recommendations
	RecommendedActions    []string `json:"recommended_actions"`
	EncryptionRequired    bool     `json:"encryption_required"`
	AccessControlRequired bool     `json:"access_control_required"`

	// Metadata
	DetectedAt    time.Time  `json:"detected_at"`
	LastVerified  *time.Time `json:"last_verified,omitempty"`
	FalsePositive *bool      `json:"false_positive,omitempty"`
}

// Evidence represents supporting evidence for a privileged data finding
type Evidence struct {
	Type         string  `json:"type"`                    // "name_pattern", "data_pattern", "enrichment", "sample_analysis"
	Source       string  `json:"source"`                  // Which detection component provided this evidence
	Pattern      string  `json:"pattern"`                 // Regex pattern or rule that matched
	MatchedValue *string `json:"matched_value,omitempty"` // Redacted if privacy mode enabled
	Confidence   float64 `json:"confidence"`              // Confidence for this specific evidence
	Description  string  `json:"description"`
}

// DetectionSummary provides high-level statistics about the detection results
type DetectionSummary struct {
	TotalFindings        int            `json:"total_findings"`
	FindingsByCategory   map[string]int `json:"findings_by_category"`
	FindingsByRisk       map[string]int `json:"findings_by_risk"`
	FindingsByConfidence map[string]int `json:"findings_by_confidence"` // High (>0.8), Medium (0.5-0.8), Low (<0.5)

	// Coverage metrics
	ObjectsWithFindings int `json:"objects_with_findings"`
	ObjectsScanned      int `json:"objects_scanned"`
	FieldsWithFindings  int `json:"fields_with_findings"`
	FieldsScanned       int `json:"fields_scanned"`

	// Compliance summary
	AffectedFrameworks []string `json:"affected_frameworks"`
	HighRiskFindings   int      `json:"high_risk_findings"`
	RequiredActions    []string `json:"required_actions"`

	// Detection quality
	AverageConfidence      float64  `json:"average_confidence"`
	HighConfidenceFindings int      `json:"high_confidence_findings"`
	SampleDataCoverage     *float64 `json:"sample_data_coverage,omitempty"` // If sample data was used
}

// DetectionRecommendation provides actionable recommendations based on findings
type DetectionRecommendation struct {
	Type                 string   `json:"type"`     // "encryption", "access_control", "data_masking", "compliance", "monitoring"
	Priority             string   `json:"priority"` // "immediate", "high", "medium", "low"
	Title                string   `json:"title"`
	Description          string   `json:"description"`
	Actions              []string `json:"actions"`          // Specific action items
	AffectedObjects      []string `json:"affected_objects"` // Objects this recommendation applies to
	ComplianceFrameworks []string `json:"compliance_frameworks,omitempty"`
	EstimatedEffort      string   `json:"estimated_effort,omitempty"` // "low", "medium", "high"
}

// CustomPattern allows users to define custom detection patterns
type CustomPattern struct {
	Name                 string   `json:"name"`
	Pattern              string   `json:"pattern"` // Regex pattern
	DataCategory         string   `json:"data_category"`
	Confidence           float64  `json:"confidence"` // Base confidence for matches
	Description          string   `json:"description"`
	ComplianceFrameworks []string `json:"compliance_frameworks,omitempty"`
}

// DetectionCapabilities defines what detection features are available at each level
type DetectionCapabilities struct {
	Schema   SchemaDetectionCapabilities   `json:"schema"`
	Enriched EnrichedDetectionCapabilities `json:"enriched"`
	Full     FullDetectionCapabilities     `json:"full"`
}

// SchemaDetectionCapabilities defines what can be detected using only schema information
type SchemaDetectionCapabilities struct {
	ColumnNameAnalysis   bool       `json:"column_name_analysis"`   // Analyze column names for PII patterns
	DataTypeAnalysis     bool       `json:"data_type_analysis"`     // Infer sensitivity from data types
	ConstraintAnalysis   bool       `json:"constraint_analysis"`    // Analyze constraints for hints
	TableContextAnalysis bool       `json:"table_context_analysis"` // Consider table name and related columns
	ForeignKeyAnalysis   bool       `json:"foreign_key_analysis"`   // Analyze relationships
	IndexAnalysis        bool       `json:"index_analysis"`         // Consider which columns are indexed
	SupportedPatterns    []string   `json:"supported_patterns"`     // List of built-in detection patterns
	ConfidenceRange      [2]float64 `json:"confidence_range"`       // Min/max confidence achievable
}

// EnrichedDetectionCapabilities defines additional capabilities when enrichment data is available
type EnrichedDetectionCapabilities struct {
	SchemaDetectionCapabilities      // Inherits all schema capabilities
	DataClassificationAnalysis  bool `json:"data_classification_analysis"` // Use existing data classifications
	PurposeAnalysis             bool `json:"purpose_analysis"`             // Consider intended data usage
	AccessPatternAnalysis       bool `json:"access_pattern_analysis"`      // Use access pattern hints
	ComplianceMapping           bool `json:"compliance_mapping"`           // Map to specific compliance requirements
	ContextualAnalysis          bool `json:"contextual_analysis"`          // Use business context for better accuracy
	ImprovedConfidence          bool `json:"improved_confidence"`          // Higher confidence due to additional context
	RiskScoring                 bool `json:"risk_scoring"`                 // Enhanced risk assessment
}

// FullDetectionCapabilities defines additional capabilities when sample data is available
type FullDetectionCapabilities struct {
	EnrichedDetectionCapabilities      // Inherits all enriched capabilities
	ValuePatternAnalysis          bool `json:"value_pattern_analysis"`   // Analyze actual data patterns
	FormatDetection               bool `json:"format_detection"`         // Detect specific data formats
	ContentAnalysis               bool `json:"content_analysis"`         // Analyze content for PII
	StatisticalAnalysis           bool `json:"statistical_analysis"`     // Statistical analysis of values
	EntropyAnalysis               bool `json:"entropy_analysis"`         // Measure data randomness/structure
	FalsePositiveReduction        bool `json:"false_positive_reduction"` // Reduce false positives through sampling
	ValidationCapability          bool `json:"validation_capability"`    // Validate pattern assumptions
	HighestConfidence             bool `json:"highest_confidence"`       // Achieve highest possible confidence
	ComprehensiveCoverage         bool `json:"comprehensive_coverage"`   // Most thorough detection possible
}

// GetDetectionCapabilities returns the capabilities available at each detection level
func GetDetectionCapabilities() DetectionCapabilities {
	return DetectionCapabilities{
		Schema: SchemaDetectionCapabilities{
			ColumnNameAnalysis:   true,
			DataTypeAnalysis:     true,
			ConstraintAnalysis:   true,
			TableContextAnalysis: true,
			ForeignKeyAnalysis:   true,
			IndexAnalysis:        true,
			SupportedPatterns: []string{
				"email_patterns", "phone_patterns", "ssn_patterns", "credit_card_patterns",
				"name_patterns", "address_patterns", "id_patterns", "date_patterns",
			},
			ConfidenceRange: [2]float64{0.3, 0.8}, // Lower confidence without sample data
		},
		Enriched: EnrichedDetectionCapabilities{
			SchemaDetectionCapabilities: SchemaDetectionCapabilities{
				ColumnNameAnalysis:   true,
				DataTypeAnalysis:     true,
				ConstraintAnalysis:   true,
				TableContextAnalysis: true,
				ForeignKeyAnalysis:   true,
				IndexAnalysis:        true,
				SupportedPatterns: []string{
					"email_patterns", "phone_patterns", "ssn_patterns", "credit_card_patterns",
					"name_patterns", "address_patterns", "id_patterns", "date_patterns",
					"enriched_business_patterns", "compliance_patterns",
				},
				ConfidenceRange: [2]float64{0.4, 0.9}, // Higher confidence with enrichment
			},
			DataClassificationAnalysis: true,
			PurposeAnalysis:            true,
			AccessPatternAnalysis:      true,
			ComplianceMapping:          true,
			ContextualAnalysis:         true,
			ImprovedConfidence:         true,
			RiskScoring:                true,
		},
		Full: FullDetectionCapabilities{
			EnrichedDetectionCapabilities: EnrichedDetectionCapabilities{
				SchemaDetectionCapabilities: SchemaDetectionCapabilities{
					ColumnNameAnalysis:   true,
					DataTypeAnalysis:     true,
					ConstraintAnalysis:   true,
					TableContextAnalysis: true,
					ForeignKeyAnalysis:   true,
					IndexAnalysis:        true,
					SupportedPatterns: []string{
						"email_patterns", "phone_patterns", "ssn_patterns", "credit_card_patterns",
						"name_patterns", "address_patterns", "id_patterns", "date_patterns",
						"enriched_business_patterns", "compliance_patterns", "value_content_patterns",
					},
					ConfidenceRange: [2]float64{0.6, 0.99}, // Highest confidence with sample data
				},
				DataClassificationAnalysis: true,
				PurposeAnalysis:            true,
				AccessPatternAnalysis:      true,
				ComplianceMapping:          true,
				ContextualAnalysis:         true,
				ImprovedConfidence:         true,
				RiskScoring:                true,
			},
			ValuePatternAnalysis:   true,
			FormatDetection:        true,
			ContentAnalysis:        true,
			StatisticalAnalysis:    true,
			EntropyAnalysis:        true,
			FalsePositiveReduction: true,
			ValidationCapability:   true,
			HighestConfidence:      true,
			ComprehensiveCoverage:  true,
		},
	}
}

// NewDetectionRequest creates a new detection request with defaults
func NewDetectionRequest(schema *UnifiedModel, level DetectionLevel) *DetectionRequest {
	return &DetectionRequest{
		Schema:                schema,
		Level:                 level,
		ConfidenceThreshold:   0.5,
		IncludeContext:        true,
		FastMode:              false,
		RedactFindings:        true,
		MaxExamplesPerFinding: 3,
		RequestID:             fmt.Sprintf("detection_%d", time.Now().Unix()),
		RequestedAt:           time.Now(),
	}
}

// WithEnrichment adds enrichment data to the detection request
func (r *DetectionRequest) WithEnrichment(enrichment *UnifiedModelEnrichment) *DetectionRequest {
	r.Enrichment = enrichment
	if r.Level == DetectionLevelSchema {
		r.Level = DetectionLevelEnriched
	}
	return r
}

// WithSampleData adds sample data to the detection request
func (r *DetectionRequest) WithSampleData(sampleData *UnifiedModelSampleData) *DetectionRequest {
	r.SampleData = sampleData
	r.Level = DetectionLevelFull
	return r
}

// WithComplianceFrameworks sets the compliance frameworks to check against
func (r *DetectionRequest) WithComplianceFrameworks(frameworks ...string) *DetectionRequest {
	r.ComplianceFrameworks = frameworks
	return r
}

// WithCustomPatterns adds custom detection patterns
func (r *DetectionRequest) WithCustomPatterns(patterns ...CustomPattern) *DetectionRequest {
	r.CustomPatterns = append(r.CustomPatterns, patterns...)
	return r
}

// Validate checks if the detection request is properly configured
func (r *DetectionRequest) Validate() []ValidationError {
	var errors []ValidationError

	if r.Schema == nil {
		errors = append(errors, ValidationError{
			Type:    ValidationErrorCritical,
			Message: "Schema is required for detection",
			Field:   "schema",
		})
	}

	if r.Level == DetectionLevelEnriched && r.Enrichment == nil {
		errors = append(errors, ValidationError{
			Type:    ValidationErrorWarning,
			Message: "Enrichment data not provided for enriched detection level",
			Field:   "enrichment",
		})
	}

	if r.Level == DetectionLevelFull && r.SampleData == nil {
		errors = append(errors, ValidationError{
			Type:    ValidationErrorWarning,
			Message: "Sample data not provided for full detection level",
			Field:   "sample_data",
		})
	}

	if r.ConfidenceThreshold < 0.0 || r.ConfidenceThreshold > 1.0 {
		errors = append(errors, ValidationError{
			Type:    ValidationErrorCritical,
			Message: "Confidence threshold must be between 0.0 and 1.0",
			Field:   "confidence_threshold",
		})
	}

	if r.MaxExamplesPerFinding < 0 {
		errors = append(errors, ValidationError{
			Type:    ValidationErrorWarning,
			Message: "MaxExamplesPerFinding should be non-negative",
			Field:   "max_examples_per_finding",
		})
	}

	return errors
}

// DetectionLevelRecommendation provides guidance on which detection level to use
type DetectionLevelRecommendation struct {
	RecommendedLevel DetectionLevel     `json:"recommended_level"`
	Reasoning        string             `json:"reasoning"`
	Alternatives     []AlternativeLevel `json:"alternatives"`
	Considerations   []string           `json:"considerations"`
}

// AlternativeLevel describes an alternative detection level option
type AlternativeLevel struct {
	Level   DetectionLevel `json:"level"`
	Pros    []string       `json:"pros"`
	Cons    []string       `json:"cons"`
	UseCase string         `json:"use_case"`
}

// RecommendDetectionLevel analyzes the available data and recommends the best detection level
func RecommendDetectionLevel(schema *UnifiedModel, enrichment *UnifiedModelEnrichment, sampleData *UnifiedModelSampleData, useCase string) DetectionLevelRecommendation {
	// Analyze available data
	hasEnrichment := enrichment != nil
	hasSampleData := sampleData != nil && sampleData.HasSampleData()

	// Base recommendation on available data and use case
	if hasSampleData && (useCase == "production" || useCase == "compliance_audit") {
		return DetectionLevelRecommendation{
			RecommendedLevel: DetectionLevelFull,
			Reasoning:        "Full detection recommended for production compliance with sample data available for highest accuracy",
			Alternatives: []AlternativeLevel{
				{
					Level:   DetectionLevelEnriched,
					Pros:    []string{"Faster execution", "Lower resource usage", "Good accuracy"},
					Cons:    []string{"May miss content-based PII", "Lower confidence scores"},
					UseCase: "Initial assessment or resource-constrained environments",
				},
			},
			Considerations: []string{
				"Ensure sample data is properly redacted if needed",
				"Full detection may take longer but provides highest confidence",
				"Consider privacy implications of sample data analysis",
			},
		}
	}

	if hasEnrichment && (useCase == "assessment" || useCase == "monitoring") {
		return DetectionLevelRecommendation{
			RecommendedLevel: DetectionLevelEnriched,
			Reasoning:        "Enriched detection provides good balance of accuracy and performance with business context",
			Alternatives: []AlternativeLevel{
				{
					Level:   DetectionLevelFull,
					Pros:    []string{"Highest accuracy", "Content validation", "Best false positive reduction"},
					Cons:    []string{"Requires sample data collection", "Higher resource usage", "Privacy considerations"},
					UseCase: "When highest accuracy is required and sample data is available",
				},
				{
					Level:   DetectionLevelSchema,
					Pros:    []string{"Fast execution", "No additional data required", "Safe for sensitive environments"},
					Cons:    []string{"Lower accuracy", "More false positives", "Limited context"},
					UseCase: "Quick scans or when enrichment data is not available",
				},
			},
			Considerations: []string{
				"Enrichment data significantly improves accuracy over schema-only",
				"Good balance for regular monitoring and assessment workflows",
				"Consider upgrading to full detection for critical compliance checks",
			},
		}
	}

	// Fallback to schema-only if minimal data available
	return DetectionLevelRecommendation{
		RecommendedLevel: DetectionLevelSchema,
		Reasoning:        "Schema-only detection recommended as baseline when limited data is available",
		Alternatives: []AlternativeLevel{
			{
				Level:   DetectionLevelEnriched,
				Pros:    []string{"Better accuracy", "Business context", "Improved risk assessment"},
				Cons:    []string{"Requires enrichment data collection", "Additional setup"},
				UseCase: "When enrichment metadata can be collected",
			},
			{
				Level:   DetectionLevelFull,
				Pros:    []string{"Maximum accuracy", "Content validation", "Comprehensive analysis"},
				Cons:    []string{"Requires both enrichment and sample data", "Highest resource usage"},
				UseCase: "For comprehensive audits and compliance verification",
			},
		},
		Considerations: []string{
			"Schema-only provides baseline detection capabilities",
			"Consider collecting enrichment data for improved accuracy",
			"May have higher false positive rates without additional context",
			"Good starting point for initial privacy assessments",
		},
	}
}
