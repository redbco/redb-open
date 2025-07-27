package detection

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/adapters"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

// PrivilegedDataDetector handles the detection of privileged data in database schemas
type PrivilegedDataDetector struct {
	patterns           map[string]*regexp.Regexp
	namePatterns       map[string][]string
	typePatterns       map[string][]string
	complianceRules    map[string][]string
	contextualPatterns map[string]*ContextualPattern
}

// ContextualPattern represents advanced pattern matching with context
type ContextualPattern struct {
	NameKeywords    []string
	TypeHints       []string
	ValuePattern    *regexp.Regexp
	RequiredContext []string // Other column names that should exist in the same table
	ExcludedContext []string // Column names that if present, reduce confidence
}

// DetectionResult represents the result of privileged data detection
type DetectionResult struct {
	Findings           []PrivilegedDataFinding `json:"findings"`
	Warnings           []string                `json:"warnings"`
	ComplianceSummary  ComplianceSummary       `json:"complianceSummary"`
	RiskScore          float64                 `json:"riskScore"`
	RecommendedActions []string                `json:"recommendedActions"`
}

// ComplianceSummary provides regulatory compliance information
type ComplianceSummary struct {
	GDPRFindings   []string `json:"gdprFindings"`
	HIPAAFindings  []string `json:"hipaaFindings"`
	PCIDSSFindings []string `json:"pcidssFindings"`
	SOXFindings    []string `json:"soxFindings"`
	CCPAFindings   []string `json:"ccpaFindings"`
}

// PrivilegedDataFinding represents a single finding of privileged data
type PrivilegedDataFinding struct {
	TableName         string            `json:"tableName"`
	ColumnName        string            `json:"columnName"`
	DataType          string            `json:"dataType"`
	DataCategory      string            `json:"dataCategory"`
	SubCategory       string            `json:"subCategory"`
	Confidence        float64           `json:"confidence"`
	Description       string            `json:"description"`
	ExampleValue      string            `json:"exampleValue,omitempty"`
	RiskLevel         string            `json:"riskLevel"`
	ComplianceImpact  []string          `json:"complianceImpact"`
	RecommendedAction string            `json:"recommendedAction"`
	Context           map[string]string `json:"context"`
}

// NewPrivilegedDataDetector creates a new PrivilegedDataDetector with comprehensive patterns
func NewPrivilegedDataDetector() *PrivilegedDataDetector {
	detector := &PrivilegedDataDetector{
		patterns:           make(map[string]*regexp.Regexp),
		namePatterns:       make(map[string][]string),
		typePatterns:       make(map[string][]string),
		complianceRules:    make(map[string][]string),
		contextualPatterns: make(map[string]*ContextualPattern),
	}

	// Initialize regex patterns for data validation
	detector.initializeRegexPatterns()

	// Initialize column name patterns
	detector.initializeNamePatterns()

	// Initialize data type patterns
	detector.initializeTypePatterns()

	// Initialize compliance mappings
	detector.initializeComplianceRules()

	// Initialize contextual patterns
	detector.initializeContextualPatterns()

	return detector
}

// initializeRegexPatterns sets up regex patterns for data validation
func (d *PrivilegedDataDetector) initializeRegexPatterns() {
	// Contact Information
	d.patterns["email"] = regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)
	d.patterns["phone_us"] = regexp.MustCompile(`^\+?1?[-.\s]?\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})$`)
	d.patterns["phone_intl"] = regexp.MustCompile(`^\+?[1-9]\d{1,14}$`)

	// Government IDs
	d.patterns["ssn"] = regexp.MustCompile(`^\d{3}-?\d{2}-?\d{4}$`)
	d.patterns["ein"] = regexp.MustCompile(`^\d{2}-?\d{7}$`)
	d.patterns["passport_us"] = regexp.MustCompile(`^[A-Z]\d{8}$`)
	d.patterns["drivers_license"] = regexp.MustCompile(`^[A-Z0-9]{8,12}$`)
	d.patterns["national_id"] = regexp.MustCompile(`^[A-Z0-9]{6,20}$`)

	// Financial Information
	d.patterns["credit_card"] = regexp.MustCompile(`^\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}$`)
	d.patterns["bank_account"] = regexp.MustCompile(`^\d{8,17}$`)
	d.patterns["routing_number"] = regexp.MustCompile(`^\d{9}$`)
	d.patterns["iban"] = regexp.MustCompile(`^[A-Z]{2}\d{2}[A-Z0-9]{4}\d{7}([A-Z0-9]?){0,16}$`)
	d.patterns["swift_bic"] = regexp.MustCompile(`^[A-Z]{6}[A-Z0-9]{2}([A-Z0-9]{3})?$`)

	// Medical Information
	d.patterns["medical_record"] = regexp.MustCompile(`^(MR|MED|PAT)\d{6,10}$`)
	d.patterns["insurance_id"] = regexp.MustCompile(`^[A-Z0-9]{6,20}$`)
	d.patterns["npi"] = regexp.MustCompile(`^\d{10}$`)

	// Network/Technical
	d.patterns["ip_address"] = regexp.MustCompile(`^(\d{1,3}\.){3}\d{1,3}$`)
	d.patterns["ipv6"] = regexp.MustCompile(`^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$`)
	d.patterns["mac_address"] = regexp.MustCompile(`^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$`)
	d.patterns["url"] = regexp.MustCompile(`^https?://[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}`)

	// Biometric Identifiers
	d.patterns["fingerprint_hash"] = regexp.MustCompile(`^[A-Fa-f0-9]{32,128}$`)
	d.patterns["facial_encoding"] = regexp.MustCompile(`^[A-Fa-f0-9]{64,256}$`)

	// Location Data
	d.patterns["coordinates"] = regexp.MustCompile(`^-?\d{1,3}\.\d+,-?\d{1,3}\.\d+$`)
	d.patterns["postal_code_us"] = regexp.MustCompile(`^\d{5}(-\d{4})?$`)
	d.patterns["postal_code_ca"] = regexp.MustCompile(`^[A-Z]\d[A-Z]\s?\d[A-Z]\d$`)

	// Authentication/Security
	d.patterns["api_key"] = regexp.MustCompile(`^[A-Za-z0-9]{20,128}$`)
	d.patterns["token"] = regexp.MustCompile(`^[A-Za-z0-9._-]{20,}$`)
	d.patterns["password_hash"] = regexp.MustCompile(`^\$[a-zA-Z0-9]+\$[a-zA-Z0-9$./]+$`)
}

// initializeNamePatterns sets up column name patterns for different data categories
func (d *PrivilegedDataDetector) initializeNamePatterns() {
	// Personal Identifiers
	d.namePatterns["personal_name"] = []string{"first_name", "last_name", "full_name", "given_name", "surname", "middle_name", "maiden_name", "nickname"}
	d.namePatterns["birth_info"] = []string{"birth_date", "birthday", "dob", "date_of_birth", "birth_year", "birth_month", "birth_day", "age"}
	d.namePatterns["gender"] = []string{"gender", "sex", "gender_identity"}
	d.namePatterns["marital_status"] = []string{"marital_status", "married", "spouse", "partner"}

	// Contact Information
	d.namePatterns["email"] = []string{"email", "mail", "email_address", "e_mail", "contact_email", "primary_email", "work_email"}
	d.namePatterns["phone"] = []string{"phone", "telephone", "tel", "mobile", "cell", "contact_number", "phone_number", "home_phone", "work_phone"}
	d.namePatterns["address"] = []string{"address", "street", "home_address", "billing_address", "shipping_address", "mailing_address", "residence"}
	d.namePatterns["postal_code"] = []string{"zip", "postal", "zipcode", "postal_code", "postcode"}

	// Government IDs
	d.namePatterns["ssn"] = []string{"ssn", "social", "social_security", "social_security_number", "sin", "social_insurance_number"}
	d.namePatterns["tax_id"] = []string{"tax_id", "tin", "taxpayer_id", "federal_tax_id", "ein", "employer_id"}
	d.namePatterns["passport"] = []string{"passport", "passport_number", "passport_id"}
	d.namePatterns["drivers_license"] = []string{"license", "licence", "drivers_license", "driver_license", "dl_number", "driving_license"}
	d.namePatterns["national_id"] = []string{"national_id", "citizen_id", "id_number", "government_id", "state_id"}

	// Financial Information
	d.namePatterns["credit_card"] = []string{"credit_card", "card", "card_number", "cc_number", "payment_card", "debit_card"}
	d.namePatterns["bank_account"] = []string{"account", "bank_account", "account_number", "checking_account", "savings_account"}
	d.namePatterns["routing"] = []string{"routing", "routing_number", "aba", "transit_number"}
	d.namePatterns["financial_info"] = []string{"salary", "income", "wage", "compensation", "bonus", "commission", "net_worth", "assets"}

	// Medical Information
	d.namePatterns["medical_record"] = []string{"medical", "patient", "medical_record", "patient_id", "mrn", "chart_number"}
	d.namePatterns["health_info"] = []string{"diagnosis", "condition", "symptom", "treatment", "medication", "prescription", "allergy", "medical_history"}
	d.namePatterns["insurance"] = []string{"insurance", "policy", "coverage", "plan", "member_id", "subscriber_id", "group_number"}

	// Biometric Data
	d.namePatterns["biometric"] = []string{"fingerprint", "facial", "iris", "retina", "voice", "dna", "biometric", "scan", "recognition"}

	// Behavioral/Digital
	d.namePatterns["browsing"] = []string{"url", "website", "browser", "search", "query", "history", "activity", "session"}
	d.namePatterns["device"] = []string{"device", "ip", "mac", "imei", "udid", "device_id", "hardware_id"}
	d.namePatterns["location"] = []string{"location", "gps", "coordinates", "latitude", "longitude", "geolocation", "position"}

	// Authentication/Security
	d.namePatterns["credentials"] = []string{"password", "pass", "pwd", "secret", "key", "token", "hash", "salt", "api_key", "access_token"}
	d.namePatterns["security"] = []string{"security_question", "challenge", "verification", "otp", "mfa", "two_factor"}
}

// initializeTypePatterns sets up data type patterns
func (d *PrivilegedDataDetector) initializeTypePatterns() {
	d.typePatterns["string_types"] = []string{"varchar", "char", "text", "string", "nvarchar", "nchar", "clob"}
	d.typePatterns["numeric_types"] = []string{"int", "bigint", "number", "numeric", "decimal", "float", "double", "smallint", "tinyint"}
	d.typePatterns["date_types"] = []string{"date", "datetime", "timestamp", "time"}
	d.typePatterns["binary_types"] = []string{"blob", "binary", "varbinary", "bytea", "image"}
	d.typePatterns["json_types"] = []string{"json", "jsonb", "xml"}
}

// initializeComplianceRules sets up regulatory compliance mappings
func (d *PrivilegedDataDetector) initializeComplianceRules() {
	// GDPR - European Union General Data Protection Regulation
	d.complianceRules["gdpr"] = []string{
		"personal_name", "birth_info", "email", "phone", "address", "national_id",
		"biometric", "location", "browsing", "device", "health_info", "gender",
		"marital_status", "financial_info",
	}

	// HIPAA - Health Insurance Portability and Accountability Act
	d.complianceRules["hipaa"] = []string{
		"medical_record", "health_info", "insurance", "personal_name", "birth_info",
		"ssn", "address", "phone", "email", "biometric", "location",
	}

	// PCI DSS - Payment Card Industry Data Security Standard
	d.complianceRules["pcidss"] = []string{
		"credit_card", "bank_account", "financial_info", "credentials",
	}

	// SOX - Sarbanes-Oxley Act
	d.complianceRules["sox"] = []string{
		"financial_info", "bank_account", "credit_card", "tax_id",
	}

	// CCPA - California Consumer Privacy Act
	d.complianceRules["ccpa"] = []string{
		"personal_name", "birth_info", "email", "phone", "address", "ssn",
		"browsing", "device", "location", "biometric", "financial_info",
	}
}

// initializeContextualPatterns sets up advanced contextual detection patterns
func (d *PrivilegedDataDetector) initializeContextualPatterns() {
	d.contextualPatterns["user_profile"] = &ContextualPattern{
		NameKeywords:    []string{"user", "profile", "account", "member"},
		RequiredContext: []string{"id", "email"},
		ExcludedContext: []string{"log", "audit", "temp"},
	}

	d.contextualPatterns["payment_info"] = &ContextualPattern{
		NameKeywords:    []string{"payment", "billing", "card", "transaction"},
		RequiredContext: []string{"amount", "date"},
		ExcludedContext: []string{"test", "sample"},
	}

	d.contextualPatterns["medical_record"] = &ContextualPattern{
		NameKeywords:    []string{"patient", "medical", "health", "clinical"},
		RequiredContext: []string{"id", "date"},
		ExcludedContext: []string{"template", "example"},
	}
}

// DetectPrivilegedData analyzes a schema for potential privileged data
func (d *PrivilegedDataDetector) DetectPrivilegedData(schemaType string, schema json.RawMessage) (*DetectionResult, error) {
	// Convert schema to unified model
	model, warnings, err := d.convertToUnifiedModel(schemaType, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to convert schema: %w", err)
	}

	result := &DetectionResult{
		Findings:           make([]PrivilegedDataFinding, 0),
		Warnings:           warnings,
		ComplianceSummary:  ComplianceSummary{},
		RecommendedActions: make([]string, 0),
	}

	// Analyze each table and column
	for _, table := range model.Tables {
		tableContext := d.buildTableContext(table)
		for _, column := range table.Columns {
			findings := d.analyzeColumn(table.Name, column, tableContext)
			result.Findings = append(result.Findings, findings...)
		}
	}

	// Calculate risk score and compliance impact
	result.RiskScore = d.calculateRiskScore(result.Findings)
	result.ComplianceSummary = d.buildComplianceSummary(result.Findings)
	result.RecommendedActions = d.generateRecommendations(result.Findings)

	return result, nil
}

// buildTableContext creates a context map for a table to help with contextual analysis
func (d *PrivilegedDataDetector) buildTableContext(table models.Table) map[string]string {
	context := make(map[string]string)
	context["table_name"] = strings.ToLower(table.Name)

	// Build a list of all column names for contextual analysis
	columnNames := make([]string, len(table.Columns))
	for i, col := range table.Columns {
		columnNames[i] = strings.ToLower(col.Name)
	}
	context["all_columns"] = strings.Join(columnNames, ",")

	return context
}

// analyzeColumn performs comprehensive analysis of a single column
func (d *PrivilegedDataDetector) analyzeColumn(tableName string, column models.Column, tableContext map[string]string) []PrivilegedDataFinding {
	findings := make([]PrivilegedDataFinding, 0)
	columnName := strings.ToLower(column.Name)

	// Check name patterns with enhanced confidence scoring
	for category, patterns := range d.namePatterns {
		for _, pattern := range patterns {
			if strings.Contains(columnName, pattern) {
				confidence := d.calculateNameConfidence(columnName, pattern, tableContext)
				if confidence >= 0.5 { // Only include findings with reasonable confidence
					finding := PrivilegedDataFinding{
						TableName:         tableName,
						ColumnName:        column.Name,
						DataType:          column.DataType.Name,
						DataCategory:      category,
						SubCategory:       d.getSubCategory(category, pattern),
						Confidence:        confidence,
						Description:       d.generateDescription(category, pattern, "name"),
						RiskLevel:         d.calculateRiskLevel(category, confidence),
						ComplianceImpact:  d.getComplianceImpact(category),
						RecommendedAction: d.getRecommendedAction(category),
						Context:           tableContext,
					}
					findings = append(findings, finding)
				}
			}
		}
	}

	// If no name-based findings, check type-based patterns
	if len(findings) == 0 {
		findings = append(findings, d.analyzeDataType(tableName, column, tableContext)...)
	}

	// Check contextual patterns
	contextualFindings := d.analyzeContextualPatterns(tableName, column, tableContext)
	findings = append(findings, contextualFindings...)

	return findings
}

// calculateNameConfidence calculates confidence based on name pattern matching
func (d *PrivilegedDataDetector) calculateNameConfidence(columnName, pattern string, context map[string]string) float64 {
	baseConfidence := 0.7

	// Exact match gets higher confidence
	if columnName == pattern {
		baseConfidence = 0.95
	}

	// Adjust based on table context
	tableName := context["table_name"]
	if strings.Contains(tableName, "user") || strings.Contains(tableName, "person") || strings.Contains(tableName, "customer") {
		baseConfidence += 0.1
	}

	// Reduce confidence for test/temp tables
	if strings.Contains(tableName, "test") || strings.Contains(tableName, "temp") || strings.Contains(tableName, "sample") {
		baseConfidence -= 0.3
	}

	// Ensure confidence is within bounds
	if baseConfidence > 1.0 {
		baseConfidence = 1.0
	}
	if baseConfidence < 0.0 {
		baseConfidence = 0.0
	}

	return baseConfidence
}

// analyzeDataType analyzes column based on data type patterns
func (d *PrivilegedDataDetector) analyzeDataType(tableName string, column models.Column, context map[string]string) []PrivilegedDataFinding {
	findings := make([]PrivilegedDataFinding, 0)
	dataTypeName := strings.ToLower(column.DataType.Name)

	// Check for potentially sensitive data types
	switch {
	case d.isStringType(dataTypeName):
		if column.DataType.Length > 0 {
			findings = append(findings, d.analyzeStringColumn(tableName, column, context)...)
		} else {
			findings = append(findings, PrivilegedDataFinding{
				TableName:         tableName,
				ColumnName:        column.Name,
				DataType:          column.DataType.Name,
				DataCategory:      "text_data",
				SubCategory:       "unstructured_text",
				Confidence:        0.3,
				Description:       "Text data type could potentially contain privileged information",
				RiskLevel:         "low",
				ComplianceImpact:  []string{"gdpr", "ccpa"},
				RecommendedAction: "Review data content and implement appropriate access controls",
				Context:           context,
			})
		}

	case d.isNumericType(dataTypeName):
		findings = append(findings, PrivilegedDataFinding{
			TableName:         tableName,
			ColumnName:        column.Name,
			DataType:          column.DataType.Name,
			DataCategory:      "numeric_identifier",
			SubCategory:       "potential_id",
			Confidence:        0.25,
			Description:       "Numeric data type could potentially contain identifiers or sensitive numbers",
			RiskLevel:         "low",
			ComplianceImpact:  []string{},
			RecommendedAction: "Verify if this column contains sensitive numeric identifiers",
			Context:           context,
		})

	case d.isBinaryType(dataTypeName):
		findings = append(findings, PrivilegedDataFinding{
			TableName:         tableName,
			ColumnName:        column.Name,
			DataType:          column.DataType.Name,
			DataCategory:      "binary_data",
			SubCategory:       "potential_biometric",
			Confidence:        0.4,
			Description:       "Binary data type could contain biometric data, images, or other sensitive binary information",
			RiskLevel:         "medium",
			ComplianceImpact:  []string{"gdpr", "hipaa", "ccpa"},
			RecommendedAction: "Verify binary data content and implement appropriate encryption",
			Context:           context,
		})
	}

	return findings
}

// analyzeStringColumn provides detailed analysis for string columns based on length
func (d *PrivilegedDataDetector) analyzeStringColumn(tableName string, column models.Column, context map[string]string) []PrivilegedDataFinding {
	findings := make([]PrivilegedDataFinding, 0)
	length := column.DataType.Length

	// Analyze based on common lengths for specific data types
	switch {
	case length == 9 || length == 11: // SSN with/without dashes
		findings = append(findings, PrivilegedDataFinding{
			TableName:         tableName,
			ColumnName:        column.Name,
			DataType:          column.DataType.Name,
			DataCategory:      "potential_ssn",
			SubCategory:       "government_id",
			Confidence:        0.6,
			Description:       fmt.Sprintf("String length (%d) matches SSN format", length),
			RiskLevel:         "high",
			ComplianceImpact:  []string{"gdpr", "ccpa"},
			RecommendedAction: "Verify if this contains SSN data and implement strong encryption",
			Context:           context,
		})

	case length >= 15 && length <= 19: // Credit card numbers
		findings = append(findings, PrivilegedDataFinding{
			TableName:         tableName,
			ColumnName:        column.Name,
			DataType:          column.DataType.Name,
			DataCategory:      "potential_credit_card",
			SubCategory:       "financial_data",
			Confidence:        0.5,
			Description:       fmt.Sprintf("String length (%d) matches credit card number format", length),
			RiskLevel:         "high",
			ComplianceImpact:  []string{"pcidss"},
			RecommendedAction: "Verify if this contains credit card data and implement PCI DSS compliance",
			Context:           context,
		})

	case length >= 20 && length <= 34: // IBAN
		findings = append(findings, PrivilegedDataFinding{
			TableName:         tableName,
			ColumnName:        column.Name,
			DataType:          column.DataType.Name,
			DataCategory:      "potential_iban",
			SubCategory:       "financial_data",
			Confidence:        0.4,
			Description:       fmt.Sprintf("String length (%d) matches IBAN format", length),
			RiskLevel:         "high",
			ComplianceImpact:  []string{"gdpr", "sox"},
			RecommendedAction: "Verify if this contains IBAN data and implement appropriate financial data controls",
			Context:           context,
		})
	}

	return findings
}

// analyzeContextualPatterns performs contextual analysis using advanced patterns
func (d *PrivilegedDataDetector) analyzeContextualPatterns(tableName string, column models.Column, context map[string]string) []PrivilegedDataFinding {
	findings := make([]PrivilegedDataFinding, 0)

	for patternName, pattern := range d.contextualPatterns {
		if d.matchesContextualPattern(column.Name, context, pattern) {
			confidence := d.calculateContextualConfidence(column.Name, context, pattern)
			if confidence >= 0.5 {
				finding := PrivilegedDataFinding{
					TableName:         tableName,
					ColumnName:        column.Name,
					DataType:          column.DataType.Name,
					DataCategory:      patternName,
					SubCategory:       "contextual_match",
					Confidence:        confidence,
					Description:       fmt.Sprintf("Column matches contextual pattern for %s", patternName),
					RiskLevel:         d.calculateRiskLevel(patternName, confidence),
					ComplianceImpact:  d.getComplianceImpact(patternName),
					RecommendedAction: d.getRecommendedAction(patternName),
					Context:           context,
				}
				findings = append(findings, finding)
			}
		}
	}

	return findings
}

// Helper functions for type checking
func (d *PrivilegedDataDetector) isStringType(dataType string) bool {
	for _, stringType := range d.typePatterns["string_types"] {
		if strings.Contains(dataType, stringType) {
			return true
		}
	}
	return false
}

func (d *PrivilegedDataDetector) isNumericType(dataType string) bool {
	for _, numericType := range d.typePatterns["numeric_types"] {
		if strings.Contains(dataType, numericType) {
			return true
		}
	}
	return false
}

func (d *PrivilegedDataDetector) isBinaryType(dataType string) bool {
	for _, binaryType := range d.typePatterns["binary_types"] {
		if strings.Contains(dataType, binaryType) {
			return true
		}
	}
	return false
}

// matchesContextualPattern checks if a column matches a contextual pattern
func (d *PrivilegedDataDetector) matchesContextualPattern(columnName string, context map[string]string, pattern *ContextualPattern) bool {
	columnLower := strings.ToLower(columnName)
	allColumns := context["all_columns"]

	// Check if column name contains any of the required keywords
	hasKeyword := false
	for _, keyword := range pattern.NameKeywords {
		if strings.Contains(columnLower, keyword) {
			hasKeyword = true
			break
		}
	}

	if !hasKeyword {
		return false
	}

	// Check if required context columns are present
	for _, required := range pattern.RequiredContext {
		if !strings.Contains(allColumns, required) {
			return false
		}
	}

	// Check if excluded context columns are present (reduces match quality)
	for _, excluded := range pattern.ExcludedContext {
		if strings.Contains(allColumns, excluded) {
			return false
		}
	}

	return true
}

// calculateContextualConfidence calculates confidence for contextual pattern matches
func (d *PrivilegedDataDetector) calculateContextualConfidence(columnName string, context map[string]string, pattern *ContextualPattern) float64 {
	baseConfidence := 0.6

	// Increase confidence for exact keyword matches
	columnLower := strings.ToLower(columnName)
	for _, keyword := range pattern.NameKeywords {
		if columnLower == keyword {
			baseConfidence += 0.2
			break
		}
	}

	// Increase confidence based on the number of required context columns present
	contextBonus := float64(len(pattern.RequiredContext)) * 0.05
	baseConfidence += contextBonus

	if baseConfidence > 1.0 {
		baseConfidence = 1.0
	}

	return baseConfidence
}

// calculateRiskScore calculates overall risk score for the schema
func (d *PrivilegedDataDetector) calculateRiskScore(findings []PrivilegedDataFinding) float64 {
	if len(findings) == 0 {
		return 0.0
	}

	totalRisk := 0.0
	highRiskCount := 0

	for _, finding := range findings {
		riskWeight := 0.0
		switch finding.RiskLevel {
		case "critical":
			riskWeight = 1.0
			highRiskCount++
		case "high":
			riskWeight = 0.8
			highRiskCount++
		case "medium":
			riskWeight = 0.5
		case "low":
			riskWeight = 0.2
		}
		totalRisk += riskWeight * finding.Confidence
	}

	// Calculate weighted average with penalty for high-risk findings
	avgRisk := totalRisk / float64(len(findings))
	highRiskPenalty := float64(highRiskCount) * 0.1

	finalScore := avgRisk + highRiskPenalty
	if finalScore > 1.0 {
		finalScore = 1.0
	}

	return finalScore
}

// buildComplianceSummary builds compliance impact summary
func (d *PrivilegedDataDetector) buildComplianceSummary(findings []PrivilegedDataFinding) ComplianceSummary {
	summary := ComplianceSummary{
		GDPRFindings:   make([]string, 0),
		HIPAAFindings:  make([]string, 0),
		PCIDSSFindings: make([]string, 0),
		SOXFindings:    make([]string, 0),
		CCPAFindings:   make([]string, 0),
	}

	for _, finding := range findings {
		findingDesc := fmt.Sprintf("%s.%s (%s)", finding.TableName, finding.ColumnName, finding.DataCategory)

		for _, compliance := range finding.ComplianceImpact {
			switch compliance {
			case "gdpr":
				summary.GDPRFindings = append(summary.GDPRFindings, findingDesc)
			case "hipaa":
				summary.HIPAAFindings = append(summary.HIPAAFindings, findingDesc)
			case "pcidss":
				summary.PCIDSSFindings = append(summary.PCIDSSFindings, findingDesc)
			case "sox":
				summary.SOXFindings = append(summary.SOXFindings, findingDesc)
			case "ccpa":
				summary.CCPAFindings = append(summary.CCPAFindings, findingDesc)
			}
		}
	}

	return summary
}

// generateRecommendations generates actionable recommendations based on findings
func (d *PrivilegedDataDetector) generateRecommendations(findings []PrivilegedDataFinding) []string {
	recommendations := make([]string, 0)
	categoryCount := make(map[string]int)
	highRiskCount := 0

	// Count categories and risk levels
	for _, finding := range findings {
		categoryCount[finding.DataCategory]++
		if finding.RiskLevel == "high" || finding.RiskLevel == "critical" {
			highRiskCount++
		}
	}

	// General recommendations based on findings
	if len(findings) > 0 {
		recommendations = append(recommendations, "Conduct a comprehensive data audit to verify the contents of flagged columns")
		recommendations = append(recommendations, "Implement data classification policies based on sensitivity levels")
	}

	if highRiskCount > 0 {
		recommendations = append(recommendations, "Prioritize encryption for high-risk data columns")
		recommendations = append(recommendations, "Implement strict access controls for sensitive data")
		recommendations = append(recommendations, "Consider data masking for non-production environments")
	}

	// Category-specific recommendations
	if categoryCount["personal_name"] > 0 || categoryCount["birth_info"] > 0 {
		recommendations = append(recommendations, "Implement GDPR/CCPA compliance measures for personal data")
	}

	if categoryCount["credit_card"] > 0 || categoryCount["bank_account"] > 0 {
		recommendations = append(recommendations, "Ensure PCI DSS compliance for payment card data")
		recommendations = append(recommendations, "Implement tokenization for financial data")
	}

	if categoryCount["medical_record"] > 0 || categoryCount["health_info"] > 0 {
		recommendations = append(recommendations, "Ensure HIPAA compliance for health information")
		recommendations = append(recommendations, "Implement audit logging for medical data access")
	}

	if categoryCount["biometric"] > 0 {
		recommendations = append(recommendations, "Implement biometric data protection measures")
		recommendations = append(recommendations, "Consider pseudonymization for biometric identifiers")
	}

	return recommendations
}

// Helper functions for generating metadata
func (d *PrivilegedDataDetector) getSubCategory(category, pattern string) string {
	subCategories := map[string]map[string]string{
		"personal_name":  {"first_name": "given_name", "last_name": "surname", "full_name": "complete_name"},
		"contact_info":   {"email": "electronic_mail", "phone": "telephone", "address": "postal_address"},
		"government_id":  {"ssn": "social_security", "passport": "travel_document", "drivers_license": "driving_permit"},
		"financial_info": {"credit_card": "payment_card", "bank_account": "banking_info", "routing": "bank_routing"},
		"medical_record": {"patient": "patient_info", "medical": "health_record", "insurance": "coverage_info"},
		"biometric":      {"fingerprint": "finger_scan", "facial": "face_recognition", "iris": "eye_scan"},
	}

	if subCats, exists := subCategories[category]; exists {
		if subCat, exists := subCats[pattern]; exists {
			return subCat
		}
	}

	return "general"
}

func (d *PrivilegedDataDetector) generateDescription(category, pattern, detectionMethod string) string {
	descriptions := map[string]string{
		"personal_name":  "Contains personal name information that may be subject to privacy regulations",
		"birth_info":     "Contains birth/age related information that is considered personal data",
		"email":          "Contains email addresses which are personally identifiable information",
		"phone":          "Contains phone numbers which are contact information subject to privacy laws",
		"address":        "Contains address information which is personally identifiable",
		"ssn":            "Contains Social Security Numbers which require strict protection",
		"tax_id":         "Contains tax identification numbers which are sensitive government identifiers",
		"credit_card":    "Contains credit card information subject to PCI DSS requirements",
		"bank_account":   "Contains banking information requiring financial data protection",
		"medical_record": "Contains medical record information subject to HIPAA regulations",
		"health_info":    "Contains health information requiring medical data protection",
		"biometric":      "Contains biometric data requiring special protection measures",
		"browsing":       "Contains browsing/behavioral data subject to privacy regulations",
		"location":       "Contains location data which may be sensitive personal information",
		"credentials":    "Contains authentication credentials requiring secure handling",
	}

	if desc, exists := descriptions[category]; exists {
		return fmt.Sprintf("%s (detected via %s pattern: %s)", desc, detectionMethod, pattern)
	}

	return fmt.Sprintf("Potentially contains %s data (detected via %s pattern: %s)", category, detectionMethod, pattern)
}

func (d *PrivilegedDataDetector) calculateRiskLevel(category string, confidence float64) string {
	// Define critical categories
	criticalCategories := []string{"ssn", "credit_card", "medical_record", "biometric", "credentials"}
	highCategories := []string{"personal_name", "birth_info", "bank_account", "health_info", "tax_id", "passport"}

	for _, critical := range criticalCategories {
		if category == critical && confidence >= 0.7 {
			return "critical"
		}
	}

	for _, high := range highCategories {
		if category == high && confidence >= 0.6 {
			return "high"
		}
	}

	if confidence >= 0.5 {
		return "medium"
	}

	return "low"
}

func (d *PrivilegedDataDetector) getComplianceImpact(category string) []string {
	impacts := make([]string, 0)

	// Check each compliance framework
	for framework, categories := range d.complianceRules {
		for _, complianceCategory := range categories {
			if category == complianceCategory {
				impacts = append(impacts, framework)
				break
			}
		}
	}

	return impacts
}

func (d *PrivilegedDataDetector) getRecommendedAction(category string) string {
	actions := map[string]string{
		"personal_name":  "Implement data minimization and pseudonymization techniques",
		"birth_info":     "Apply age-based access controls and data retention policies",
		"email":          "Implement email encryption and secure communication protocols",
		"phone":          "Apply communication data protection and opt-out mechanisms",
		"ssn":            "Implement strong encryption, access logging, and strict need-to-know access",
		"credit_card":    "Implement PCI DSS compliance, tokenization, and secure payment processing",
		"medical_record": "Ensure HIPAA compliance with audit trails and access controls",
		"biometric":      "Implement biometric template protection and consider privacy-preserving techniques",
		"location":       "Apply location data anonymization and user consent mechanisms",
		"credentials":    "Implement secure credential storage with salting and hashing",
		"browsing":       "Apply behavioral data anonymization and user privacy controls",
	}

	if action, exists := actions[category]; exists {
		return action
	}

	return "Review data handling practices and implement appropriate security measures"
}

// convertToUnifiedModel converts the input schema to a unified model
func (d *PrivilegedDataDetector) convertToUnifiedModel(schemaType string, schema json.RawMessage) (*models.UnifiedModel, []string, error) {
	// Get the appropriate adapter for the schema type
	adapter, err := getAdapter(schemaType)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get adapter: %w", err)
	}

	// Convert schema to unified model using the adapter
	model, warnings, err := adapter.IngestSchema(schema)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert schema: %w", err)
	}

	return model, warnings, nil
}

// getAdapter returns the appropriate adapter based on the schema type
func getAdapter(schemaType string) (adapters.SchemaIngester, error) {
	switch schemaType {
	case "postgres", "postgresql":
		return &adapters.PostgresIngester{}, nil
	case "mysql":
		return &adapters.MySQLIngester{}, nil
	case "mariadb":
		return &adapters.MariaDBIngester{}, nil
	case "mssql", "sqlserver":
		return &adapters.MSSQLIngester{}, nil
	case "oracle":
		return &adapters.OracleIngester{}, nil
	case "db2":
		return &adapters.Db2Ingester{}, nil
	case "cockroach", "cockroachdb":
		return &adapters.CockroachIngester{}, nil
	case "clickhouse":
		return &adapters.ClickhouseIngester{}, nil
	case "cassandra":
		return &adapters.CassandraIngester{}, nil
	case "mongodb":
		return &adapters.MongoDBIngester{}, nil
	case "redis":
		return &adapters.RedisIngester{}, nil
	case "neo4j":
		return &adapters.Neo4jIngester{}, nil
	case "elasticsearch":
		return &adapters.ElasticsearchIngester{}, nil
	case "snowflake":
		return &adapters.SnowflakeIngester{}, nil
	case "pinecone":
		return &adapters.PineconeIngester{}, nil
	case "edgedb":
		return &adapters.EdgeDBIngester{}, nil
	default:
		return nil, fmt.Errorf("unsupported schema type: %s", schemaType)
	}
}
