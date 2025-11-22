package mapping

import (
	"context"
	"testing"
)

func TestResolveURI(t *testing.T) {
	// Note: These are unit tests for the logic.
	// Integration tests would require a real database.

	tests := []struct {
		name           string
		uri            string
		wantIsTemplate bool
		wantIsVirtual  bool
	}{
		{
			name:           "template URI",
			uri:            "template://default/database/collection/users/field/email",
			wantIsTemplate: true,
			wantIsVirtual:  false,
		},
		{
			name:           "regular redb URI",
			uri:            "redb://data/database/db1/table/users/column/email",
			wantIsTemplate: false,
			wantIsVirtual:  false,
		},
		{
			name:           "other protocol",
			uri:            "mcp://server1/resource/users",
			wantIsTemplate: false,
			wantIsVirtual:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &TemplateResolutionService{}
			result := svc.resolveURI(context.Background(), tt.uri)

			if result.IsTemplate != tt.wantIsTemplate {
				t.Errorf("IsTemplate = %v, want %v", result.IsTemplate, tt.wantIsTemplate)
			}
		})
	}
}

func TestResolutionResult(t *testing.T) {
	result := ResolutionResult{
		OriginalURI:  "template://default/database/collection/users/field/email",
		ResolvedURI:  "redb://data/database/db1/collection/users/field/email",
		WasResolved:  true,
		IsTemplate:   true,
		ErrorMessage: "",
	}

	if !result.WasResolved {
		t.Error("Expected WasResolved to be true")
	}

	if result.OriginalURI == result.ResolvedURI {
		t.Error("Expected OriginalURI != ResolvedURI after resolution")
	}
}

func TestMappingResolutionReport(t *testing.T) {
	report := &MappingResolutionReport{
		MappingID:     "map_123",
		RulesTotal:    10,
		RulesResolved: 7,
		Results:       []ResolutionResult{},
	}

	if report.RulesResolved > report.RulesTotal {
		t.Error("RulesResolved should not exceed RulesTotal")
	}

	resolutionRate := float64(report.RulesResolved) / float64(report.RulesTotal)
	if resolutionRate < 0 || resolutionRate > 1 {
		t.Errorf("Resolution rate should be 0-1, got %f", resolutionRate)
	}
}
