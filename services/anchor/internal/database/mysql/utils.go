package mysql

import (
	"fmt"
	"strings"
)

// QuoteIdentifier quotes a MySQL identifier using backticks
func QuoteIdentifier(name string) string {
	// Replace any existing backticks with double backticks to escape them
	name = strings.Replace(name, "`", "``", -1)
	// Wrap the entire name in backticks
	return fmt.Sprintf("`%s`", name)
}
