package unifiedmodel

import (
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// DatabaseFeatureSupport defines what UnifiedModel objects each database supports
type DatabaseFeatureSupport struct {
	DatabaseType           dbcapabilities.DatabaseType   `json:"database_type"`
	SupportedObjects       map[ObjectType]ObjectSupport  `json:"supported_objects"`
	Paradigms              []dbcapabilities.DataParadigm `json:"paradigms"`
	ConversionCapabilities ConversionCapabilities        `json:"conversion_capabilities"`
}

// ObjectSupport defines the level of support for a specific UnifiedModel object type
type ObjectSupport struct {
	Supported      bool         `json:"supported"`
	SupportLevel   SupportLevel `json:"support_level"`
	Limitations    []string     `json:"limitations,omitempty"`
	Alternatives   []ObjectType `json:"alternatives,omitempty"`
	RequiredFields []string     `json:"required_fields,omitempty"`
	OptionalFields []string     `json:"optional_fields,omitempty"`
	Notes          string       `json:"notes,omitempty"`
}

// SupportLevel indicates how well an object type is supported
type SupportLevel string

const (
	SupportLevelFull        SupportLevel = "full"        // Complete support with all features
	SupportLevelPartial     SupportLevel = "partial"     // Limited support, some features missing
	SupportLevelEmulated    SupportLevel = "emulated"    // Can be simulated using other objects
	SupportLevelUnsupported SupportLevel = "unsupported" // Not supported at all
)

// ConversionCapabilities defines database-specific conversion capabilities
type ConversionCapabilities struct {
	CanBeSource           bool                          `json:"can_be_source"`
	CanBeTarget           bool                          `json:"can_be_target"`
	PreferredSourceTypes  []dbcapabilities.DatabaseType `json:"preferred_source_types,omitempty"`
	PreferredTargetTypes  []dbcapabilities.DatabaseType `json:"preferred_target_types,omitempty"`
	SpecialRequirements   []string                      `json:"special_requirements,omitempty"`
	ConversionLimitations []string                      `json:"conversion_limitations,omitempty"`
}

// DatabaseFeatureRegistry holds feature support information for all databases
var DatabaseFeatureRegistry = map[dbcapabilities.DatabaseType]DatabaseFeatureSupport{
	// Will be populated with actual database definitions
}

// GetDatabaseFeatures returns feature support information for a database
func GetDatabaseFeatures(dbType dbcapabilities.DatabaseType) (DatabaseFeatureSupport, bool) {
	features, exists := DatabaseFeatureRegistry[dbType]
	return features, exists
}

// IsObjectSupported checks if a database supports a specific object type
func IsObjectSupported(dbType dbcapabilities.DatabaseType, objType ObjectType) bool {
	features, exists := GetDatabaseFeatures(dbType)
	if !exists {
		return false
	}

	support, exists := features.SupportedObjects[objType]
	return exists && support.Supported
}

// GetObjectSupport returns detailed support information for an object type
func GetObjectSupport(dbType dbcapabilities.DatabaseType, objType ObjectType) (ObjectSupport, bool) {
	features, exists := GetDatabaseFeatures(dbType)
	if !exists {
		return ObjectSupport{}, false
	}

	support, exists := features.SupportedObjects[objType]
	return support, exists
}

// GetSupportedObjects returns all object types supported by a database
func GetSupportedObjects(dbType dbcapabilities.DatabaseType) []ObjectType {
	features, exists := GetDatabaseFeatures(dbType)
	if !exists {
		return nil
	}

	var supported []ObjectType
	for objType, support := range features.SupportedObjects {
		if support.Supported {
			supported = append(supported, objType)
		}
	}

	return supported
}

// GetUnsupportedObjects returns object types not supported by a database
func GetUnsupportedObjects(dbType dbcapabilities.DatabaseType) []ObjectType {
	features, exists := GetDatabaseFeatures(dbType)
	if !exists {
		return nil
	}

	var unsupported []ObjectType
	for objType, support := range features.SupportedObjects {
		if !support.Supported {
			unsupported = append(unsupported, objType)
		}
	}

	return unsupported
}

// GetObjectAlternatives returns alternative object types for unsupported objects
func GetObjectAlternatives(dbType dbcapabilities.DatabaseType, objType ObjectType) []ObjectType {
	support, exists := GetObjectSupport(dbType, objType)
	if !exists || support.Supported {
		return nil
	}

	return support.Alternatives
}

// HasConversionCapability checks if a database can participate in conversions
func HasConversionCapability(dbType dbcapabilities.DatabaseType, asSource bool) bool {
	features, exists := GetDatabaseFeatures(dbType)
	if !exists {
		return false
	}

	if asSource {
		return features.ConversionCapabilities.CanBeSource
	}
	return features.ConversionCapabilities.CanBeTarget
}

// GetPreferredConversionPartners returns preferred databases for conversion
func GetPreferredConversionPartners(dbType dbcapabilities.DatabaseType, asSource bool) []dbcapabilities.DatabaseType {
	features, exists := GetDatabaseFeatures(dbType)
	if !exists {
		return nil
	}

	if asSource {
		return features.ConversionCapabilities.PreferredTargetTypes
	}
	return features.ConversionCapabilities.PreferredSourceTypes
}

// Helper functions for creating object support definitions

// FullSupport creates an ObjectSupport with full support
func FullSupport() ObjectSupport {
	return ObjectSupport{
		Supported:    true,
		SupportLevel: SupportLevelFull,
	}
}

// PartialSupport creates an ObjectSupport with partial support
func PartialSupport(limitations []string, notes string) ObjectSupport {
	return ObjectSupport{
		Supported:    true,
		SupportLevel: SupportLevelPartial,
		Limitations:  limitations,
		Notes:        notes,
	}
}

// EmulatedSupport creates an ObjectSupport that can be emulated
func EmulatedSupport(alternatives []ObjectType, notes string) ObjectSupport {
	return ObjectSupport{
		Supported:    false,
		SupportLevel: SupportLevelEmulated,
		Alternatives: alternatives,
		Notes:        notes,
	}
}

// UnsupportedObject creates an ObjectSupport for unsupported objects
func UnsupportedObject(alternatives []ObjectType, notes string) ObjectSupport {
	return ObjectSupport{
		Supported:    false,
		SupportLevel: SupportLevelUnsupported,
		Alternatives: alternatives,
		Notes:        notes,
	}
}

// WithRequiredFields adds required fields to ObjectSupport
func (os ObjectSupport) WithRequiredFields(fields []string) ObjectSupport {
	os.RequiredFields = fields
	return os
}

// WithOptionalFields adds optional fields to ObjectSupport
func (os ObjectSupport) WithOptionalFields(fields []string) ObjectSupport {
	os.OptionalFields = fields
	return os
}
