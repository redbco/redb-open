package common

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

type TableNode struct {
	Name    string
	Edges   []string
	Visited bool
	Temp    bool
}

func TopologicalSort(tables []TableInfo) ([]TableInfo, error) {
	nodes := make(map[string]*TableNode)

	// Create nodes and edges
	for _, table := range tables {
		nodes[table.Name] = &TableNode{Name: table.Name}
		for _, constraint := range table.Constraints {
			if constraint.Type == "FOREIGN KEY" && constraint.ForeignKey != nil {
				nodes[table.Name].Edges = append(nodes[table.Name].Edges, constraint.ForeignKey.Table)
			}
		}
	}

	// Perform topological sort
	var sorted []string
	var visit func(string) error

	visit = func(name string) error {
		node := nodes[name]
		if node.Temp {
			return fmt.Errorf("cyclic dependency detected")
		}
		if !node.Visited {
			node.Temp = true
			for _, edge := range node.Edges {
				if err := visit(edge); err != nil {
					return err
				}
			}
			node.Visited = true
			node.Temp = false
			sorted = append([]string{name}, sorted...)
		}
		return nil
	}

	for name := range nodes {
		if !nodes[name].Visited {
			if err := visit(name); err != nil {
				return nil, err
			}
		}
	}

	// Reorder tables based on sorted order
	sortedTables := make([]TableInfo, len(tables))
	for i, name := range sorted {
		for _, table := range tables {
			if table.Name == name {
				sortedTables[i] = table
				break
			}
		}
	}

	return sortedTables, nil
}

func QuoteIdentifier(name string) string {
	// Replace any existing quotes with double quotes to escape them
	name = strings.Replace(name, `"`, `""`, -1)
	// Wrap the entire name in quotes
	return fmt.Sprintf(`"%s"`, name)
}

func CleanSQLDefinition(sql string) string {
	// Remove leading and trailing whitespace
	sql = strings.TrimSpace(sql)

	// Replace newlines and multiple spaces with a single space
	sql = regexp.MustCompile(`\s+`).ReplaceAllString(sql, " ")

	// Remove spaces after opening parentheses and before closing parentheses
	sql = regexp.MustCompile(`\(\s+`).ReplaceAllString(sql, "(")
	sql = regexp.MustCompile(`\s+\)`).ReplaceAllString(sql, ")")

	// Remove spaces before and after commas
	sql = regexp.MustCompile(`\s*,\s*`).ReplaceAllString(sql, ",")

	return sql
}

func GenerateUniqueID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func QuoteStringSlice(slice []string) []string {
	quoted := make([]string, len(slice))
	for i, s := range slice {
		quoted[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(s, "'", "''"))
	}
	return quoted
}
