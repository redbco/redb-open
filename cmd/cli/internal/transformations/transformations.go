package transformations

import (
	"fmt"
	"strings"

	"github.com/redbco/redb-open/cmd/cli/internal/common"
)

// Transformation represents a transformation
type Transformation struct {
	TenantID                  string `json:"tenant_id"`
	TransformationID          string `json:"transformation_id"`
	TransformationName        string `json:"transformation_name"`
	TransformationDescription string `json:"transformation_description,omitempty"`
	TransformationType        string `json:"transformation_type"`
	TransformationVersion     string `json:"transformation_version,omitempty"`
	TransformationFunction    string `json:"transformation_function,omitempty"`
	OwnerID                   string `json:"owner_id,omitempty"`
	WorkspaceID               string `json:"workspace_id,omitempty"`
	IsBuiltin                 bool   `json:"is_builtin"`
}

// ListTransformations lists all available transformations
func ListTransformations(verbose bool) error {
	// Get profile info and client
	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	// Build URL - transformations don't need workspace path, just tenant
	url := fmt.Sprintf("%s/api/v1/transformations?builtin=true", profileInfo.TenantURL)

	// Make the request
	var response struct {
		Transformations []Transformation `json:"transformations"`
	}

	if err := client.Get(url, &response); err != nil {
		return fmt.Errorf("failed to list transformations: %w", err)
	}

	// Print transformations
	if len(response.Transformations) == 0 {
		fmt.Println("No transformations found.")
		return nil
	}

	if verbose {
		// Verbose mode: grouped by type with descriptions
		passthroughTxs := []Transformation{}
		generatorTxs := []Transformation{}
		nullReturningTxs := []Transformation{}

		for _, t := range response.Transformations {
			switch t.TransformationType {
			case "passthrough":
				passthroughTxs = append(passthroughTxs, t)
			case "generator":
				generatorTxs = append(generatorTxs, t)
			case "null_returning":
				nullReturningTxs = append(nullReturningTxs, t)
			default:
				passthroughTxs = append(passthroughTxs, t)
			}
		}

		// Print summary
		fmt.Printf("Available Transformations (%d total)\n", len(response.Transformations))
		fmt.Println(strings.Repeat("=", 80))
		fmt.Println()

		// Print passthrough transformations
		if len(passthroughTxs) > 0 {
			fmt.Printf("Passthrough Transformations (%d)\n", len(passthroughTxs))
			fmt.Println(strings.Repeat("-", 80))
			fmt.Println("Transform data from source column to target column")
			fmt.Println()

			for _, t := range passthroughTxs {
				fmt.Printf("  • %s\n", t.TransformationName)
				if t.TransformationDescription != "" {
					fmt.Printf("    %s\n", t.TransformationDescription)
				}
				fmt.Println()
			}
		}

		// Print generator transformations
		if len(generatorTxs) > 0 {
			fmt.Printf("Generator Transformations (%d)\n", len(generatorTxs))
			fmt.Println(strings.Repeat("-", 80))
			fmt.Println("Generate values without requiring source data")
			fmt.Println()

			for _, t := range generatorTxs {
				fmt.Printf("  • %s\n", t.TransformationName)
				if t.TransformationDescription != "" {
					fmt.Printf("    %s\n", t.TransformationDescription)
				}
				fmt.Println()
			}
		}

		// Print null-returning transformations
		if len(nullReturningTxs) > 0 {
			fmt.Printf("Null-Returning Transformations (%d)\n", len(nullReturningTxs))
			fmt.Println(strings.Repeat("-", 80))
			fmt.Println("Process data without mapping to target column (side effects)")
			fmt.Println()

			for _, t := range nullReturningTxs {
				fmt.Printf("  • %s\n", t.TransformationName)
				if t.TransformationDescription != "" {
					fmt.Printf("    %s\n", t.TransformationDescription)
				}
				fmt.Println()
			}
		}

		// Print usage hint
		fmt.Println(strings.Repeat("=", 80))
		fmt.Println()
		fmt.Println("Usage:")
		fmt.Println("  Use these transformations with 'mappings add-rule' or 'mappings modify-rule'")
		fmt.Println()
	} else {
		// Non-verbose mode: table format like mappings list
		fmt.Println()
		fmt.Printf("%-25s %-45s %-15s\n", "Name", "Description", "Type")
		fmt.Println(strings.Repeat("-", 90))

		for _, t := range response.Transformations {
			description := t.TransformationDescription
			if len(description) > 43 {
				description = description[:40] + "..."
			}

			// Format type nicely
			typeDisplay := t.TransformationType
			switch t.TransformationType {
			case "passthrough":
				typeDisplay = "Passthrough"
			case "generator":
				typeDisplay = "Generator"
			case "null_returning":
				typeDisplay = "Null-Returning"
			}

			fmt.Printf("%-25s %-45s %-15s\n",
				t.TransformationName,
				description,
				typeDisplay)
		}
		fmt.Println()
	}

	return nil
}
