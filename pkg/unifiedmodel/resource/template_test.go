package resource

import (
	"testing"
)

func TestParseTemplateURI(t *testing.T) {
	tests := []struct {
		name        string
		uri         string
		wantErr     bool
		wantNS      string
		wantObjType ObjectType
		wantObjName string
		wantType    string
	}{
		{
			name:        "simple collection field",
			uri:         "template://default/database/collection/users/field/email",
			wantNS:      "default",
			wantObjType: ObjectTypeCollection,
			wantObjName: "users",
			wantType:    "",
		},
		{
			name:        "collection field with type",
			uri:         "template://default/database/collection/orders/field/total?type=decimal",
			wantNS:      "default",
			wantObjType: ObjectTypeCollection,
			wantObjName: "orders",
			wantType:    "decimal",
		},
		{
			name:        "table column with type",
			uri:         "template://staging/database/table/customers/column/id?type=uuid",
			wantNS:      "staging",
			wantObjType: ObjectTypeTable,
			wantObjName: "customers",
			wantType:    "uuid",
		},
		{
			name:        "container only (no fields)",
			uri:         "template://default/database/collection/products",
			wantNS:      "default",
			wantObjType: ObjectTypeCollection,
			wantObjName: "products",
			wantType:    "",
		},
		{
			name:    "invalid protocol",
			uri:     "redb://default/database/collection/users",
			wantErr: true,
		},
		{
			name:    "missing object name",
			uri:     "template://default/database/collection",
			wantErr: true,
		},
		{
			name:    "odd number of path segments",
			uri:     "template://default/database/collection/users/field",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTemplateURI(tt.uri)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseTemplateURI() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			if got.Namespace != tt.wantNS {
				t.Errorf("Namespace = %v, want %v", got.Namespace, tt.wantNS)
			}
			if got.ObjectType != tt.wantObjType {
				t.Errorf("ObjectType = %v, want %v", got.ObjectType, tt.wantObjType)
			}
			if got.ObjectName != tt.wantObjName {
				t.Errorf("ObjectName = %v, want %v", got.ObjectName, tt.wantObjName)
			}
			if got.ExpectedType != tt.wantType {
				t.Errorf("ExpectedType = %v, want %v", got.ExpectedType, tt.wantType)
			}
		})
	}
}

func TestTemplateURIBuilder(t *testing.T) {
	tests := []struct {
		name     string
		build    func() string
		expected string
	}{
		{
			name: "collection with field and type",
			build: func() string {
				return NewTemplateURIBuilder("default", ObjectTypeCollection, "users").
					WithField("email").
					WithType("string").
					Build()
			},
			expected: "template://default/database/collection/users/field/email?type=string",
		},
		{
			name: "table with column",
			build: func() string {
				return NewTemplateURIBuilder("staging", ObjectTypeTable, "orders").
					WithColumn("total").
					WithType("decimal").
					Build()
			},
			expected: "template://staging/database/table/orders/column/total?type=decimal",
		},
		{
			name: "container only",
			build: func() string {
				return NewTemplateURIBuilder("default", ObjectTypeCollection, "products").
					Build()
			},
			expected: "template://default/database/collection/products",
		},
		{
			name: "default namespace when empty",
			build: func() string {
				return NewTemplateURIBuilder("", ObjectTypeCollection, "test").
					Build()
			},
			expected: "template://default/database/collection/test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.build()
			if got != tt.expected {
				t.Errorf("Build() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestTemplateAddress_Methods(t *testing.T) {
	ta := &TemplateAddress{
		Namespace:  "default",
		ObjectType: ObjectTypeCollection,
		ObjectName: "users",
		PathSegments: []PathSegment{
			{Type: SegmentTypeField, Name: "email"},
		},
		ExpectedType: "string",
	}

	t.Run("String", func(t *testing.T) {
		expected := "template://default/database/collection/users/field/email?type=string"
		if got := ta.String(); got != expected {
			t.Errorf("String() = %v, want %v", got, expected)
		}
	})

	t.Run("GetContainerURI", func(t *testing.T) {
		expected := "template://default/database/collection/users"
		if got := ta.GetContainerURI(); got != expected {
			t.Errorf("GetContainerURI() = %v, want %v", got, expected)
		}
	})

	t.Run("GetItemName", func(t *testing.T) {
		expected := "email"
		if got := ta.GetItemName(); got != expected {
			t.Errorf("GetItemName() = %v, want %v", got, expected)
		}
	})

	t.Run("GetItemType", func(t *testing.T) {
		expected := SegmentTypeField
		if got := ta.GetItemType(); got != expected {
			t.Errorf("GetItemType() = %v, want %v", got, expected)
		}
	})

	t.Run("IsItemAddress", func(t *testing.T) {
		if !ta.IsItemAddress() {
			t.Error("IsItemAddress() = false, want true")
		}
	})

	t.Run("IsContainerAddress", func(t *testing.T) {
		if ta.IsContainerAddress() {
			t.Error("IsContainerAddress() = true, want false")
		}
	})

	t.Run("ToResourceURI", func(t *testing.T) {
		expected := "redb://data/database/db_abc123/collection/users/field/email"
		if got := ta.ToResourceURI("db_abc123"); got != expected {
			t.Errorf("ToResourceURI() = %v, want %v", got, expected)
		}
	})
}

func TestValidateTemplateAddress(t *testing.T) {
	tests := []struct {
		name    string
		ta      *TemplateAddress
		wantErr bool
	}{
		{
			name: "valid template address",
			ta: &TemplateAddress{
				Namespace:  "default",
				ObjectType: ObjectTypeCollection,
				ObjectName: "users",
			},
			wantErr: false,
		},
		{
			name:    "nil template address",
			ta:      nil,
			wantErr: true,
		},
		{
			name: "missing namespace",
			ta: &TemplateAddress{
				ObjectType: ObjectTypeCollection,
				ObjectName: "users",
			},
			wantErr: true,
		},
		{
			name: "missing object type",
			ta: &TemplateAddress{
				Namespace:  "default",
				ObjectName: "users",
			},
			wantErr: true,
		},
		{
			name: "missing object name",
			ta: &TemplateAddress{
				Namespace:  "default",
				ObjectType: ObjectTypeCollection,
			},
			wantErr: true,
		},
		{
			name: "invalid object type",
			ta: &TemplateAddress{
				Namespace:  "default",
				ObjectType: ObjectType("invalid"),
				ObjectName: "users",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTemplateAddress(tt.ta)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateTemplateAddress() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
