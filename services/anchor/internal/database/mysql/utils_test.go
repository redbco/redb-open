package mysql

import (
	"testing"
)

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"simple_table", "`simple_table`"},
		{"table_with_backticks", "`table_with_backticks`"},
		{"table`with`backticks", "`table``with``backticks`"},
		{"table_with_spaces", "`table_with_spaces`"},
		{"table-with-dashes", "`table-with-dashes`"},
		{"table_with_underscores", "`table_with_underscores`"},
		{"123table", "`123table`"},
		{"table123", "`table123`"},
		{"", "``"},
	}

	for _, test := range tests {
		result := QuoteIdentifier(test.input)
		if result != test.expected {
			t.Errorf("QuoteIdentifier(%q) = %q, expected %q", test.input, result, test.expected)
		}
	}
}
