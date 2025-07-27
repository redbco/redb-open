package engine

// Region represents a region
type Region struct {
	RegionID          string  `json:"region_id"`
	RegionName        string  `json:"region_name"`
	RegionDescription string  `json:"region_description,omitempty"`
	RegionLocation    string  `json:"region_location,omitempty"`
	RegionLatitude    float64 `json:"region_latitude,omitempty"`
	RegionLongitude   float64 `json:"region_longitude,omitempty"`
	RegionType        string  `json:"region_type"`
	NodeCount         int32   `json:"node_count"`
	InstanceCount     int32   `json:"instance_count"`
	DatabaseCount     int32   `json:"database_count"`
	Status            Status  `json:"status"`
	GlobalRegion      bool    `json:"global_region"`
	Created           string  `json:"created"`
	Updated           string  `json:"updated"`
}

// ListRegionsResponse represents the list regions response
type ListRegionsResponse struct {
	Regions []Region `json:"regions"`
}

// ShowRegionResponse represents the show region response
type ShowRegionResponse struct {
	Region Region `json:"region"`
}

// AddRegionRequest represents the add region request
// Note: owner_id is automatically set from the authenticated user's profile
type AddRegionRequest struct {
	RegionName        string   `json:"region_name" validate:"required"`
	RegionType        string   `json:"region_type" validate:"required"`
	RegionDescription string   `json:"region_description,omitempty"`
	RegionLocation    string   `json:"region_location,omitempty"`
	RegionLatitude    *float64 `json:"region_latitude,omitempty"`
	RegionLongitude   *float64 `json:"region_longitude,omitempty"`
}

// AddRegionResponse represents the add region response
type AddRegionResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Region  Region `json:"region"`
	Status  Status `json:"status"`
}

// ModifyRegionRequest represents the modify region request
type ModifyRegionRequest struct {
	RegionNameNew     string   `json:"region_name_new,omitempty"`
	RegionDescription string   `json:"region_description,omitempty"`
	RegionLocation    string   `json:"region_location,omitempty"`
	RegionLatitude    *float64 `json:"region_latitude,omitempty"`
	RegionLongitude   *float64 `json:"region_longitude,omitempty"`
}

// ModifyRegionResponse represents the modify region response
type ModifyRegionResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Region  Region `json:"region"`
	Status  Status `json:"status"`
}

// DeleteRegionResponse represents the delete region response
type DeleteRegionResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  Status `json:"status"`
}
