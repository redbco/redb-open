package regions

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/redbco/redb-open/cmd/cli/internal/config"
	"github.com/redbco/redb-open/cmd/cli/internal/httpclient"
)

type Region struct {
	ID            string  `json:"region_id"`
	Name          string  `json:"region_name"`
	Type          string  `json:"region_type"`
	Description   string  `json:"region_description"`
	Location      string  `json:"region_location"`
	Latitude      float64 `json:"region_latitude"`
	Longitude     float64 `json:"region_longitude"`
	Status        string  `json:"status"`
	NodeCount     int     `json:"node_count"`
	InstanceCount int     `json:"instance_count"`
	DatabaseCount int     `json:"database_count"`
	GlobalRegion  bool    `json:"global_region"`
	Created       string  `json:"created"`
	Updated       string  `json:"updated"`
}

// Response wraps the API response for listing regions
type Response struct {
	Regions []Region `json:"regions"`
}

// RegionResponse wraps the API response for a single region
type RegionResponse struct {
	Region Region `json:"region"`
}

// CreateRegionResponse wraps the API response for creating a region
type CreateRegionResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Region  Region `json:"region"`
	Status  string `json:"status"`
}

// UpdateRegionResponse wraps the API response for updating a region
type UpdateRegionResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Region  Region `json:"region"`
	Status  string `json:"status"`
}

type CreateRegionRequest struct {
	Name        string  `json:"region_name"`
	Type        string  `json:"region_type"`
	Description string  `json:"region_description,omitempty"`
	Location    string  `json:"region_location,omitempty"`
	Latitude    float64 `json:"region_latitude,omitempty"`
	Longitude   float64 `json:"region_longitude,omitempty"`
}

type UpdateRegionRequest struct {
	Name        string  `json:"region_name,omitempty"`
	NameNew     string  `json:"region_name_new,omitempty"`
	Description string  `json:"region_description,omitempty"`
	Location    string  `json:"region_location,omitempty"`
	Latitude    float64 `json:"region_latitude,omitempty"`
	Longitude   float64 `json:"region_longitude,omitempty"`
}

// ShowRegions lists all regions
func ListRegions() error {
	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/regions", tenantURL)

	var response Response
	if err := client.Get(url, &response, true); err != nil {
		return fmt.Errorf("failed to get regions: %v", err)
	}

	regions := response.Regions

	if len(regions) == 0 {
		fmt.Println("No regions found")
		return nil
	}

	// Sort regions by name
	sort.Slice(regions, func(i, j int) bool {
		return regions[i].Name < regions[j].Name
	})

	// Create a tabwriter for formatted output
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)

	fmt.Println()

	// Print header
	fmt.Fprintln(w, "Name\tType\tLocation\tStatus\tNodes\tInstances\tDatabases\tDescription")
	fmt.Fprintln(w, "----\t----\t--------\t------\t-----\t--------\t--------\t-----------")

	// Print each region
	for _, region := range regions {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%d\t%d\t%s\n",
			region.Name,
			region.Type,
			region.Location,
			region.Status,
			region.NodeCount,
			region.InstanceCount,
			region.DatabaseCount,
			region.Description)
	}

	_ = w.Flush()
	fmt.Println()
	return nil
}

// ShowRegion displays details of a specific region
func ShowRegion(regionName string) error {
	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	client := httpclient.GetClient()

	// Now get detailed region info using the ID
	detailURL := fmt.Sprintf("%s/api/v1/regions/%s", tenantURL, regionName)

	var response RegionResponse
	if err := client.Get(detailURL, &response, true); err != nil {
		return fmt.Errorf("failed to get region details: %v", err)
	}

	detailedRegion := response.Region

	// Display region details
	fmt.Printf("\nRegion: %s\n", detailedRegion.Name)
	fmt.Println("----------------------------------------")

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintf(w, "ID:\t%s\n", detailedRegion.ID)
	fmt.Fprintf(w, "Type:\t%s\n", detailedRegion.Type)
	fmt.Fprintf(w, "Description:\t%s\n", detailedRegion.Description)
	fmt.Fprintf(w, "Location:\t%s\n", detailedRegion.Location)
	fmt.Fprintf(w, "Coordinates:\t%.6f, %.6f\n", detailedRegion.Latitude, detailedRegion.Longitude)
	fmt.Fprintf(w, "Status:\t%s\n", detailedRegion.Status)
	fmt.Fprintf(w, "Node Count:\t%d\n", detailedRegion.NodeCount)
	fmt.Fprintf(w, "Created:\t%s\n", detailedRegion.Created)
	fmt.Fprintf(w, "Updated:\t%s\n", detailedRegion.Updated)
	_ = w.Flush()
	fmt.Println()

	return nil
}

// AddRegion creates a new region
func AddRegion(args []string) error {
	reader := bufio.NewReader(os.Stdin)

	// Get region name
	var regionName string
	if len(args) > 0 && strings.HasPrefix(args[0], "--name=") {
		regionName = strings.TrimPrefix(args[0], "--name=")
	} else {
		fmt.Print("Region Name: ")
		regionName, _ = reader.ReadString('\n')
		regionName = strings.TrimSpace(regionName)
	}

	if regionName == "" {
		return fmt.Errorf("region name is required")
	}

	// Get region type
	var regionType string
	if len(args) > 1 && strings.HasPrefix(args[1], "--type=") {
		regionType = strings.TrimPrefix(args[1], "--type=")
	} else {
		fmt.Print("Region Type (aws/azure/gcp/on-premise): ")
		regionType, _ = reader.ReadString('\n')
		regionType = strings.TrimSpace(regionType)
	}

	if regionType == "" {
		return fmt.Errorf("region type is required")
	}

	// Validate region type
	validTypes := []string{"aws", "azure", "gcp", "on-premise"}
	isValidType := false
	for _, validType := range validTypes {
		if regionType == validType {
			isValidType = true
			break
		}
	}
	if !isValidType {
		return fmt.Errorf("invalid region type. Must be one of: %s", strings.Join(validTypes, ", "))
	}

	// Get optional fields
	var description, location string
	var latitude, longitude float64

	fmt.Print("Description (optional): ")
	description, _ = reader.ReadString('\n')
	description = strings.TrimSpace(description)

	fmt.Print("Location (optional): ")
	location, _ = reader.ReadString('\n')
	location = strings.TrimSpace(location)

	fmt.Print("Latitude (optional): ")
	latStr, _ := reader.ReadString('\n')
	latStr = strings.TrimSpace(latStr)
	if latStr != "" {
		if parsed, err := strconv.ParseFloat(latStr, 64); err == nil {
			latitude = parsed
		}
	}

	fmt.Print("Longitude (optional): ")
	lngStr, _ := reader.ReadString('\n')
	lngStr = strings.TrimSpace(lngStr)
	if lngStr != "" {
		if parsed, err := strconv.ParseFloat(lngStr, 64); err == nil {
			longitude = parsed
		}
	}

	// Create the region
	createReq := CreateRegionRequest{
		Name:        regionName,
		Type:        regionType,
		Description: description,
		Location:    location,
		Latitude:    latitude,
		Longitude:   longitude,
	}

	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/regions", tenantURL)

	var createResponse CreateRegionResponse
	if err := client.Post(url, createReq, &createResponse, true); err != nil {
		return fmt.Errorf("failed to create region: %v", err)
	}

	fmt.Printf("Successfully created region '%s' (ID: %s)\n", createResponse.Region.Name, createResponse.Region.ID)
	return nil
}

// ModifyRegion updates an existing region
func ModifyRegion(regionName string, args []string) error {
	// First find the region to get its ID
	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/regions/%s", tenantURL, regionName)

	fmt.Println()

	var response RegionResponse
	if err := client.Get(url, &response, true); err != nil {
		return fmt.Errorf("failed to get region: %v", err)
	}

	targetRegion := response.Region

	reader := bufio.NewReader(os.Stdin)
	updateReq := UpdateRegionRequest{}
	hasChanges := false

	// Parse command line arguments or prompt for input
	for _, arg := range args {
		switch {
		case strings.HasPrefix(arg, "--name="):
			updateReq.Name = strings.TrimPrefix(arg, "--name=")
			hasChanges = true
		case strings.HasPrefix(arg, "--description="):
			updateReq.Description = strings.TrimPrefix(arg, "--description=")
			hasChanges = true
		case strings.HasPrefix(arg, "--location="):
			updateReq.Location = strings.TrimPrefix(arg, "--location=")
			hasChanges = true
		}
	}

	// If no arguments provided, prompt for input
	if !hasChanges {
		fmt.Printf("Modifying region '%s' (press Enter to keep current value):\n", regionName)

		fmt.Printf("New Name [%s]: ", targetRegion.Name)
		newName, _ := reader.ReadString('\n')
		newName = strings.TrimSpace(newName)
		if newName != "" {
			updateReq.NameNew = newName
			hasChanges = true
		}

		fmt.Printf("Description [%s]: ", targetRegion.Description)
		newDescription, _ := reader.ReadString('\n')
		newDescription = strings.TrimSpace(newDescription)
		if newDescription != "" {
			updateReq.Description = newDescription
			hasChanges = true
		}

		fmt.Printf("Location [%s]: ", targetRegion.Location)
		newLocation, _ := reader.ReadString('\n')
		newLocation = strings.TrimSpace(newLocation)
		if newLocation != "" {
			updateReq.Location = newLocation
			hasChanges = true
		}

		fmt.Printf("Latitude [%.6f]: ", targetRegion.Latitude)
		latStr, _ := reader.ReadString('\n')
		latStr = strings.TrimSpace(latStr)
		if latStr != "" {
			if parsed, err := strconv.ParseFloat(latStr, 64); err == nil {
				updateReq.Latitude = parsed
				hasChanges = true
			}
		}

		fmt.Printf("Longitude [%.6f]: ", targetRegion.Longitude)
		lngStr, _ := reader.ReadString('\n')
		lngStr = strings.TrimSpace(lngStr)
		if lngStr != "" {
			if parsed, err := strconv.ParseFloat(lngStr, 64); err == nil {
				updateReq.Longitude = parsed
				hasChanges = true
			}
		}
	}

	if !hasChanges {
		fmt.Println("No changes made")
		return nil
	}

	// Update the region
	updateURL := fmt.Sprintf("%s/api/v1/regions/%s", tenantURL, regionName)

	var updateResponse UpdateRegionResponse
	if err := client.Put(updateURL, updateReq, &updateResponse, true); err != nil {
		return fmt.Errorf("failed to update region: %v", err)
	}

	fmt.Printf("Successfully updated region '%s'\n", updateResponse.Region.Name)
	fmt.Println()
	return nil
}

// DeleteRegion deletes an existing region
func DeleteRegion(regionName string, args []string) error {
	// Check for force flag
	force := false
	for _, arg := range args {
		if arg == "--force" || arg == "-f" {
			force = true
			break
		}
	}

	// First find the region to get its ID
	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	client := httpclient.GetClient()

	// Confirm deletion unless force flag is used
	if !force {
		reader := bufio.NewReader(os.Stdin)
		fmt.Println()
		fmt.Printf("Are you sure you want to delete region '%s'? This action cannot be undone. (y/N): ", regionName)
		confirmation, _ := reader.ReadString('\n')
		confirmation = strings.TrimSpace(strings.ToLower(confirmation))

		if confirmation != "y" && confirmation != "yes" {
			fmt.Println("Operation cancelled")
			fmt.Println()
			return nil
		}
	}

	// Delete the region
	deleteURL := fmt.Sprintf("%s/api/v1/regions/%s", tenantURL, regionName)

	if err := client.Delete(deleteURL, true); err != nil {
		return fmt.Errorf("failed to delete region: %v", err)
	}

	fmt.Printf("Successfully deleted region '%s'\n", regionName)
	fmt.Println()
	return nil
}
