package engine

import (
	"encoding/json"
	"testing"
)

func TestTransformDataColumnMapping(t *testing.T) {
	server := &Server{}

	// Test data with source column names
	sourceData := []map[string]interface{}{
		{
			"id":                     "123",
			"email_address":          "test@example.com",
			"password_hash":          "hashed_password",
			"social_security_number": "123-45-6789",
			"medical_history":        "Some medical history",
		},
	}

	// Transformation rules that map source columns to target columns
	transformationRules := []interface{}{
		map[string]interface{}{
			"source_field":        "id",
			"target_field":        "id",
			"transformation_type": "direct",
		},
		map[string]interface{}{
			"source_field":        "email_address",
			"target_field":        "email",
			"transformation_type": "direct",
		},
		map[string]interface{}{
			"source_field":        "password_hash",
			"target_field":        "password",
			"transformation_type": "direct",
		},
		map[string]interface{}{
			"source_field":        "social_security_number",
			"target_field":        "ssn",
			"transformation_type": "direct",
		},
		map[string]interface{}{
			"source_field":        "medical_history",
			"target_field":        "history",
			"transformation_type": "direct",
		},
	}

	options := map[string]interface{}{
		"transformation_rules": transformationRules,
	}

	// Transform the data
	transformedData, err := server.transformData(sourceData, options)
	if err != nil {
		t.Fatalf("transformData failed: %v", err)
	}

	if len(transformedData) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(transformedData))
	}

	transformedRow := transformedData[0]

	// Check that target columns exist with correct values
	expectedTargetColumns := map[string]interface{}{
		"id":       "123",
		"email":    "test@example.com",
		"password": "hashed_password",
		"ssn":      "123-45-6789",
		"history":  "Some medical history",
	}

	for expectedCol, expectedValue := range expectedTargetColumns {
		if value, exists := transformedRow[expectedCol]; !exists {
			t.Errorf("Expected target column '%s' not found", expectedCol)
		} else if value != expectedValue {
			t.Errorf("Expected column '%s' to have value '%v', got '%v'", expectedCol, expectedValue, value)
		}
	}

	// Check that source columns are removed
	sourceColumns := []string{"email_address", "password_hash", "social_security_number", "medical_history"}
	for _, sourceCol := range sourceColumns {
		if _, exists := transformedRow[sourceCol]; exists {
			t.Errorf("Source column '%s' should have been removed but still exists", sourceCol)
		}
	}

	// Verify the total number of columns is correct
	expectedColumnCount := len(expectedTargetColumns)
	actualColumnCount := len(transformedRow)
	if actualColumnCount != expectedColumnCount {
		t.Errorf("Expected %d columns, got %d", expectedColumnCount, actualColumnCount)
	}

	// Print the transformed data for debugging
	jsonData, _ := json.MarshalIndent(transformedData, "", "  ")
	t.Logf("Transformed data: %s", string(jsonData))
}

func TestTransformDataRealWorldScenario(t *testing.T) {
	server := &Server{}

	// Simulate data from PostgreSQL demo_source table
	sourceData := []map[string]interface{}{
		{
			"id":                     "550e8400-e29b-41d4-a716-446655440000",
			"email_address":          "john.doe@example.com",
			"password_hash":          "$2a$10$hashedpassword123",
			"social_security_number": "123-45-6789",
			"medical_history":        "Patient has no known allergies",
		},
		{
			"id":                     "550e8400-e29b-41d4-a716-446655440001",
			"email_address":          "jane.smith@example.com",
			"password_hash":          "$2a$10$hashedpassword456",
			"social_security_number": "987-65-4321",
			"medical_history":        "Patient has seasonal allergies",
		},
	}

	// Transformation rules matching the user's mapping rules
	transformationRules := []interface{}{
		map[string]interface{}{
			"source_field":        "id",
			"target_field":        "id",
			"transformation_type": "direct",
		},
		map[string]interface{}{
			"source_field":        "email_address",
			"target_field":        "email",
			"transformation_type": "direct",
		},
		map[string]interface{}{
			"source_field":        "password_hash",
			"target_field":        "password",
			"transformation_type": "direct",
		},
		map[string]interface{}{
			"source_field":        "social_security_number",
			"target_field":        "ssn",
			"transformation_type": "direct",
		},
		map[string]interface{}{
			"source_field":        "medical_history",
			"target_field":        "history",
			"transformation_type": "direct",
		},
	}

	options := map[string]interface{}{
		"transformation_rules": transformationRules,
	}

	// Transform the data
	transformedData, err := server.transformData(sourceData, options)
	if err != nil {
		t.Fatalf("transformData failed: %v", err)
	}

	if len(transformedData) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(transformedData))
	}

	// Check first row
	row1 := transformedData[0]
	expectedRow1 := map[string]interface{}{
		"id":       "550e8400-e29b-41d4-a716-446655440000",
		"email":    "john.doe@example.com",
		"password": "$2a$10$hashedpassword123",
		"ssn":      "123-45-6789",
		"history":  "Patient has no known allergies",
	}

	for expectedCol, expectedValue := range expectedRow1 {
		if value, exists := row1[expectedCol]; !exists {
			t.Errorf("Row 1: Expected target column '%s' not found", expectedCol)
		} else if value != expectedValue {
			t.Errorf("Row 1: Expected column '%s' to have value '%v', got '%v'", expectedCol, expectedValue, value)
		}
	}

	// Check second row
	row2 := transformedData[1]
	expectedRow2 := map[string]interface{}{
		"id":       "550e8400-e29b-41d4-a716-446655440001",
		"email":    "jane.smith@example.com",
		"password": "$2a$10$hashedpassword456",
		"ssn":      "987-65-4321",
		"history":  "Patient has seasonal allergies",
	}

	for expectedCol, expectedValue := range expectedRow2 {
		if value, exists := row2[expectedCol]; !exists {
			t.Errorf("Row 2: Expected target column '%s' not found", expectedCol)
		} else if value != expectedValue {
			t.Errorf("Row 2: Expected column '%s' to have value '%v', got '%v'", expectedCol, expectedValue, value)
		}
	}

	// Verify that source columns are completely removed from both rows
	sourceColumns := []string{"email_address", "password_hash", "social_security_number", "medical_history"}
	for _, sourceCol := range sourceColumns {
		if _, exists := row1[sourceCol]; exists {
			t.Errorf("Row 1: Source column '%s' should have been removed but still exists", sourceCol)
		}
		if _, exists := row2[sourceCol]; exists {
			t.Errorf("Row 2: Source column '%s' should have been removed but still exists", sourceCol)
		}
	}

	// Print the transformed data for debugging
	jsonData, _ := json.MarshalIndent(transformedData, "", "  ")
	t.Logf("Transformed data: %s", string(jsonData))
}

func TestTransformDataNoRules(t *testing.T) {
	server := &Server{}

	// Test data
	sourceData := []map[string]interface{}{
		{
			"id":   "123",
			"name": "test",
		},
	}

	// No transformation rules
	options := map[string]interface{}{}

	// Transform the data
	transformedData, err := server.transformData(sourceData, options)
	if err != nil {
		t.Fatalf("transformData failed: %v", err)
	}

	// Should return data as-is
	if len(transformedData) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(transformedData))
	}

	transformedRow := transformedData[0]
	if len(transformedRow) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(transformedRow))
	}

	if value, exists := transformedRow["id"]; !exists || value != "123" {
		t.Errorf("Expected id=123, got %v", value)
	}

	if value, exists := transformedRow["name"]; !exists || value != "test" {
		t.Errorf("Expected name=test, got %v", value)
	}
}

func TestTransformDataEmptyData(t *testing.T) {
	server := &Server{}

	// Empty data
	sourceData := []map[string]interface{}{}

	options := map[string]interface{}{
		"transformation_rules": []interface{}{},
	}

	// Transform the data
	transformedData, err := server.transformData(sourceData, options)
	if err != nil {
		t.Fatalf("transformData failed: %v", err)
	}

	// Should return empty data
	if len(transformedData) != 0 {
		t.Errorf("Expected 0 rows, got %d", len(transformedData))
	}
}
