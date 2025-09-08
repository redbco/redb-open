package core

import (
	"context"
	"time"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// TranslationContext holds context information for a translation operation
type TranslationContext struct {
	// Request information
	RequestID   string    `json:"request_id"`
	RequestedBy string    `json:"requested_by"`
	RequestedAt time.Time `json:"requested_at"`

	// Database information
	SourceDatabase dbcapabilities.DatabaseType `json:"source_database"`
	TargetDatabase dbcapabilities.DatabaseType `json:"target_database"`

	// Schema information
	SourceSchema *unifiedmodel.UnifiedModel `json:"source_schema,omitempty"`
	TargetSchema *unifiedmodel.UnifiedModel `json:"target_schema,omitempty"`

	// Enhancement data
	Enrichment *unifiedmodel.UnifiedModelEnrichment `json:"enrichment,omitempty"`
	SampleData *unifiedmodel.UnifiedModelSampleData `json:"sample_data,omitempty"`

	// Translation configuration
	Preferences TranslationPreferences  `json:"preferences"`
	Analysis    *ParadigmAnalysisResult `json:"analysis,omitempty"`

	// Processing state
	Warnings      []TranslationWarning  `json:"warnings"`
	UserDecisions []PendingUserDecision `json:"user_decisions"`
	Metrics       TranslationMetrics    `json:"metrics"`

	// Context for cancellation and timeouts
	Context context.Context `json:"-"`
}

// TranslationMetrics tracks metrics during translation
type TranslationMetrics struct {
	StartTime      time.Time     `json:"start_time"`
	EndTime        time.Time     `json:"end_time,omitempty"`
	ProcessingTime time.Duration `json:"processing_time"`

	// Object processing counts
	ObjectsProcessed int `json:"objects_processed"`
	ObjectsConverted int `json:"objects_converted"`
	ObjectsSkipped   int `json:"objects_skipped"`
	ObjectsDropped   int `json:"objects_dropped"`

	// Type conversion counts
	TypesConverted   int `json:"types_converted"`
	LossyConversions int `json:"lossy_conversions"`

	// Memory and performance metrics
	PeakMemoryUsage int64   `json:"peak_memory_usage,omitempty"`
	CacheHitRate    float64 `json:"cache_hit_rate,omitempty"`
}

// NewTranslationContext creates a new translation context
func NewTranslationContext(ctx context.Context, request *TranslationRequest) *TranslationContext {
	return &TranslationContext{
		RequestID:      request.RequestID,
		RequestedBy:    request.RequestedBy,
		RequestedAt:    request.RequestedAt,
		SourceDatabase: request.SourceDatabase,
		TargetDatabase: request.TargetDatabase,
		Enrichment:     request.Enrichment,
		SampleData:     request.SampleData,
		Preferences:    request.Preferences,
		Warnings:       make([]TranslationWarning, 0),
		UserDecisions:  make([]PendingUserDecision, 0),
		Context:        ctx,
		Metrics: TranslationMetrics{
			StartTime: time.Now(),
		},
	}
}

// AddWarning adds a warning to the translation context
func (tc *TranslationContext) AddWarning(warningType WarningType, objectType, objectName, message, severity, suggestion string) {
	warning := TranslationWarning{
		WarningType: warningType,
		ObjectType:  objectType,
		ObjectName:  objectName,
		Message:     message,
		Severity:    severity,
		Suggestion:  suggestion,
	}
	tc.Warnings = append(tc.Warnings, warning)
}

// AddUserDecision adds a pending user decision to the translation context
func (tc *TranslationContext) AddUserDecision(decisionID, objectType, objectName string, decisionType DecisionType, context string, options []string, recommended string) {
	decision := PendingUserDecision{
		DecisionID:   decisionID,
		ObjectType:   objectType,
		ObjectName:   objectName,
		DecisionType: decisionType,
		Context:      context,
		Options:      options,
		Recommended:  recommended,
	}
	tc.UserDecisions = append(tc.UserDecisions, decision)
}

// IncrementObjectProcessed increments the processed object count
func (tc *TranslationContext) IncrementObjectProcessed() {
	tc.Metrics.ObjectsProcessed++
}

// IncrementObjectConverted increments the converted object count
func (tc *TranslationContext) IncrementObjectConverted() {
	tc.Metrics.ObjectsConverted++
}

// IncrementObjectSkipped increments the skipped object count
func (tc *TranslationContext) IncrementObjectSkipped() {
	tc.Metrics.ObjectsSkipped++
}

// IncrementObjectDropped increments the dropped object count
func (tc *TranslationContext) IncrementObjectDropped() {
	tc.Metrics.ObjectsDropped++
}

// IncrementTypeConverted increments the type converted count
func (tc *TranslationContext) IncrementTypeConverted() {
	tc.Metrics.TypesConverted++
}

// IncrementLossyConversion increments the lossy conversion count
func (tc *TranslationContext) IncrementLossyConversion() {
	tc.Metrics.LossyConversions++
}

// SetAnalysis sets the paradigm analysis result
func (tc *TranslationContext) SetAnalysis(analysis *ParadigmAnalysisResult) {
	tc.Analysis = analysis
}

// SetSourceSchema sets the source schema
func (tc *TranslationContext) SetSourceSchema(schema *unifiedmodel.UnifiedModel) {
	tc.SourceSchema = schema
}

// SetTargetSchema sets the target schema
func (tc *TranslationContext) SetTargetSchema(schema *unifiedmodel.UnifiedModel) {
	tc.TargetSchema = schema
}

// FinishProcessing marks the translation as finished and calculates final metrics
func (tc *TranslationContext) FinishProcessing() {
	tc.Metrics.EndTime = time.Now()
	tc.Metrics.ProcessingTime = tc.Metrics.EndTime.Sub(tc.Metrics.StartTime)
}

// GetSuccessRate calculates the success rate of the translation
func (tc *TranslationContext) GetSuccessRate() float64 {
	if tc.Metrics.ObjectsProcessed == 0 {
		return 0.0
	}
	return float64(tc.Metrics.ObjectsConverted) / float64(tc.Metrics.ObjectsProcessed)
}

// HasWarnings returns true if there are any warnings
func (tc *TranslationContext) HasWarnings() bool {
	return len(tc.Warnings) > 0
}

// HasUserDecisions returns true if there are pending user decisions
func (tc *TranslationContext) HasUserDecisions() bool {
	return len(tc.UserDecisions) > 0
}

// IsInteractive returns true if the translation is in interactive mode
func (tc *TranslationContext) IsInteractive() bool {
	return tc.Preferences.InteractiveMode
}

// ShouldAutoApproveSimple returns true if simple decisions should be auto-approved
func (tc *TranslationContext) ShouldAutoApproveSimple() bool {
	return tc.Preferences.AutoApproveSimple
}

// AcceptsDataLoss returns true if data loss is acceptable
func (tc *TranslationContext) AcceptsDataLoss() bool {
	return tc.Preferences.AcceptDataLoss
}

// OptimizeForPerformance returns true if optimization for performance is enabled
func (tc *TranslationContext) OptimizeForPerformance() bool {
	return tc.Preferences.OptimizeForPerformance
}

// OptimizeForStorage returns true if optimization for storage is enabled
func (tc *TranslationContext) OptimizeForStorage() bool {
	return tc.Preferences.OptimizeForStorage
}

// PreserveRelationships returns true if relationships should be preserved
func (tc *TranslationContext) PreserveRelationships() bool {
	return tc.Preferences.PreserveRelationships
}

// IncludeMetadata returns true if metadata should be included
func (tc *TranslationContext) IncludeMetadata() bool {
	return tc.Preferences.IncludeMetadata
}

// GetCustomMapping returns a custom mapping for the given key, if any
func (tc *TranslationContext) GetCustomMapping(key string) (string, bool) {
	if tc.Preferences.CustomMappings == nil {
		return "", false
	}
	value, exists := tc.Preferences.CustomMappings[key]
	return value, exists
}

// IsObjectExcluded returns true if the given object should be excluded
func (tc *TranslationContext) IsObjectExcluded(objectName string) bool {
	for _, excluded := range tc.Preferences.ExcludeObjects {
		if excluded == objectName {
			return true
		}
	}
	return false
}

// ShouldGenerateComments returns true if comments should be generated
func (tc *TranslationContext) ShouldGenerateComments() bool {
	return tc.Preferences.GenerateComments
}

// ShouldIncludeOriginalNames returns true if original names should be included
func (tc *TranslationContext) ShouldIncludeOriginalNames() bool {
	return tc.Preferences.IncludeOriginalNames
}

// ShouldUseQualifiedNames returns true if qualified names should be used
func (tc *TranslationContext) ShouldUseQualifiedNames() bool {
	return tc.Preferences.UseQualifiedNames
}

// ShouldPreserveCaseStyle returns true if case style should be preserved
func (tc *TranslationContext) ShouldPreserveCaseStyle() bool {
	return tc.Preferences.PreserveCaseStyle
}

// IsCancelled returns true if the context has been cancelled
func (tc *TranslationContext) IsCancelled() bool {
	select {
	case <-tc.Context.Done():
		return true
	default:
		return false
	}
}

// GetDeadline returns the deadline for the translation, if any
func (tc *TranslationContext) GetDeadline() (time.Time, bool) {
	return tc.Context.Deadline()
}

// Clone creates a copy of the translation context
func (tc *TranslationContext) Clone() *TranslationContext {
	clone := *tc

	// Deep copy slices
	clone.Warnings = make([]TranslationWarning, len(tc.Warnings))
	copy(clone.Warnings, tc.Warnings)

	clone.UserDecisions = make([]PendingUserDecision, len(tc.UserDecisions))
	copy(clone.UserDecisions, tc.UserDecisions)

	// Deep copy preferences
	clone.Preferences = tc.Preferences
	if tc.Preferences.CustomMappings != nil {
		clone.Preferences.CustomMappings = make(map[string]string)
		for k, v := range tc.Preferences.CustomMappings {
			clone.Preferences.CustomMappings[k] = v
		}
	}

	if tc.Preferences.ExcludeObjects != nil {
		clone.Preferences.ExcludeObjects = make([]string, len(tc.Preferences.ExcludeObjects))
		copy(clone.Preferences.ExcludeObjects, tc.Preferences.ExcludeObjects)
	}

	return &clone
}
