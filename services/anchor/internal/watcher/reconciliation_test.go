package watcher

import (
	"testing"

	"github.com/redbco/redb-open/pkg/models"
)

func TestMatchContainers(t *testing.T) {
	tests := []struct {
		name                 string
		virtualContainers    []*models.ResourceContainer
		discoveredContainers []*models.ResourceContainer
		expectedMatches      int
	}{
		{
			name: "exact match",
			virtualContainers: []*models.ResourceContainer{
				{
					ContainerID: "vc1",
					ObjectName:  "users",
					ObjectType:  "collection",
				},
			},
			discoveredContainers: []*models.ResourceContainer{
				{
					ContainerID: "dc1",
					ObjectName:  "users",
					ObjectType:  "collection",
				},
			},
			expectedMatches: 1,
		},
		{
			name: "no match - different names",
			virtualContainers: []*models.ResourceContainer{
				{
					ContainerID: "vc1",
					ObjectName:  "users",
					ObjectType:  "collection",
				},
			},
			discoveredContainers: []*models.ResourceContainer{
				{
					ContainerID: "dc1",
					ObjectName:  "customers",
					ObjectType:  "collection",
				},
			},
			expectedMatches: 0,
		},
		{
			name: "no match - different types",
			virtualContainers: []*models.ResourceContainer{
				{
					ContainerID: "vc1",
					ObjectName:  "users",
					ObjectType:  "collection",
				},
			},
			discoveredContainers: []*models.ResourceContainer{
				{
					ContainerID: "dc1",
					ObjectName:  "users",
					ObjectType:  "table",
				},
			},
			expectedMatches: 0,
		},
		{
			name: "multiple containers, one match",
			virtualContainers: []*models.ResourceContainer{
				{
					ContainerID: "vc1",
					ObjectName:  "users",
					ObjectType:  "collection",
				},
				{
					ContainerID: "vc2",
					ObjectName:  "orders",
					ObjectType:  "collection",
				},
			},
			discoveredContainers: []*models.ResourceContainer{
				{
					ContainerID: "dc1",
					ObjectName:  "users",
					ObjectType:  "collection",
				},
			},
			expectedMatches: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matches := matchContainers(tt.virtualContainers, tt.discoveredContainers)
			if len(matches) != tt.expectedMatches {
				t.Errorf("expected %d matches, got %d", tt.expectedMatches, len(matches))
			}
		})
	}
}

func TestMatchItems(t *testing.T) {
	tests := []struct {
		name              string
		virtualItems      []*models.ResourceItem
		discoveredItems   []*models.ResourceItem
		expectedMatches   int
		expectedConflicts int
	}{
		{
			name: "exact type match",
			virtualItems: []*models.ResourceItem{
				{
					ItemID:   "vi1",
					ItemName: "email",
					DataType: "string",
				},
			},
			discoveredItems: []*models.ResourceItem{
				{
					ItemID:   "di1",
					ItemName: "email",
					DataType: "string",
				},
			},
			expectedMatches:   1,
			expectedConflicts: 0,
		},
		{
			name: "type widening - compatible",
			virtualItems: []*models.ResourceItem{
				{
					ItemID:   "vi1",
					ItemName: "age",
					DataType: "int16",
				},
			},
			discoveredItems: []*models.ResourceItem{
				{
					ItemID:   "di1",
					ItemName: "age",
					DataType: "int32",
				},
			},
			expectedMatches:   1,
			expectedConflicts: 0,
		},
		{
			name: "type conflict - incompatible",
			virtualItems: []*models.ResourceItem{
				{
					ItemID:   "vi1",
					ItemName: "price",
					DataType: "string",
				},
			},
			discoveredItems: []*models.ResourceItem{
				{
					ItemID:   "di1",
					ItemName: "price",
					DataType: "integer",
				},
			},
			expectedMatches:   0,
			expectedConflicts: 1,
		},
		{
			name: "no match - different names",
			virtualItems: []*models.ResourceItem{
				{
					ItemID:   "vi1",
					ItemName: "email",
					DataType: "string",
				},
			},
			discoveredItems: []*models.ResourceItem{
				{
					ItemID:   "di1",
					ItemName: "username",
					DataType: "string",
				},
			},
			expectedMatches:   0,
			expectedConflicts: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matches, conflicts := matchItems(tt.virtualItems, tt.discoveredItems)
			if len(matches) != tt.expectedMatches {
				t.Errorf("expected %d matches, got %d", tt.expectedMatches, len(matches))
			}
			if len(conflicts) != tt.expectedConflicts {
				t.Errorf("expected %d conflicts, got %d", tt.expectedConflicts, len(conflicts))
			}
		})
	}
}

func TestCheckTypeCompatibility(t *testing.T) {
	tests := []struct {
		name           string
		virtualType    string
		discoveredType string
		wantCompatible bool
		wantSuggestion string
	}{
		{
			name:           "exact match",
			virtualType:    "string",
			discoveredType: "string",
			wantCompatible: true,
			wantSuggestion: "exact_match",
		},
		{
			name:           "int16 to int32 widening",
			virtualType:    "int16",
			discoveredType: "int32",
			wantCompatible: true,
			wantSuggestion: "widen_type",
		},
		{
			name:           "int32 to int64 widening",
			virtualType:    "int32",
			discoveredType: "int64",
			wantCompatible: true,
			wantSuggestion: "widen_type",
		},
		{
			name:           "float32 to float64 widening",
			virtualType:    "float32",
			discoveredType: "float64",
			wantCompatible: true,
			wantSuggestion: "widen_type",
		},
		{
			name:           "varchar compatibility",
			virtualType:    "varchar(50)",
			discoveredType: "varchar(100)",
			wantCompatible: true,
			wantSuggestion: "widen_varchar",
		},
		{
			name:           "text types compatibility",
			virtualType:    "varchar",
			discoveredType: "text",
			wantCompatible: true,
			wantSuggestion: "text_compatible",
		},
		{
			name:           "incompatible - string to integer",
			virtualType:    "string",
			discoveredType: "integer",
			wantCompatible: false,
			wantSuggestion: "user_resolve",
		},
		{
			name:           "incompatible - integer to string",
			virtualType:    "integer",
			discoveredType: "string",
			wantCompatible: false,
			wantSuggestion: "user_resolve",
		},
		{
			name:           "int64 to int32 - not compatible (narrowing)",
			virtualType:    "int64",
			discoveredType: "int32",
			wantCompatible: false,
			wantSuggestion: "user_resolve",
		},
		{
			name:           "case insensitive matching",
			virtualType:    "STRING",
			discoveredType: "string",
			wantCompatible: true,
			wantSuggestion: "exact_match",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compatible, suggestion := checkTypeCompatibility(tt.virtualType, tt.discoveredType)
			if compatible != tt.wantCompatible {
				t.Errorf("checkTypeCompatibility() compatible = %v, want %v", compatible, tt.wantCompatible)
			}
			if suggestion != tt.wantSuggestion {
				t.Errorf("checkTypeCompatibility() suggestion = %v, want %v", suggestion, tt.wantSuggestion)
			}
		})
	}
}

func TestFilterItemsByContainer(t *testing.T) {
	items := []*models.ResourceItem{
		{ItemID: "i1", ContainerID: "c1", ItemName: "field1"},
		{ItemID: "i2", ContainerID: "c1", ItemName: "field2"},
		{ItemID: "i3", ContainerID: "c2", ItemName: "field3"},
		{ItemID: "i4", ContainerID: "c1", ItemName: "field4"},
	}

	filtered := filterItemsByContainer(items, "c1")

	if len(filtered) != 3 {
		t.Errorf("expected 3 items for container c1, got %d", len(filtered))
	}

	for _, item := range filtered {
		if item.ContainerID != "c1" {
			t.Errorf("expected all items to have container_id c1, got %s", item.ContainerID)
		}
	}
}
