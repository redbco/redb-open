package engine

// Satellite represents a satellite
type Satellite struct {
	TenantID             string `json:"tenant_id"`
	SatelliteID          string `json:"satellite_id"`
	SatelliteName        string `json:"satellite_name"`
	SatelliteDescription string `json:"satellite_description,omitempty"`
	SatellitePlatform    string `json:"satellite_platform"`
	SatelliteVersion     string `json:"satellite_version"`
	IPAddress            string `json:"ip_address"`
	NodeID               string `json:"node_id"`
	Status               Status `json:"status"`
	OwnerID              string `json:"owner_id"`
}

type ListSatellitesResponse struct {
	Satellites []Satellite `json:"satellites"`
}

type ShowSatelliteResponse struct {
	Satellite Satellite `json:"satellite"`
}

type AddSatelliteRequest struct {
	SatelliteName        string `json:"satellite_name" validate:"required"`
	SatelliteDescription string `json:"satellite_description,omitempty"`
	SatellitePlatform    string `json:"satellite_platform" validate:"required"`
	SatelliteVersion     string `json:"satellite_version" validate:"required"`
	IPAddress            string `json:"ip_address" validate:"required"`
	NodeID               string `json:"node_id" validate:"required"`
	PublicKey            string `json:"public_key" validate:"required"`
	PrivateKey           string `json:"private_key" validate:"required"`
}

type AddSatelliteResponse struct {
	Message   string    `json:"message"`
	Success   bool      `json:"success"`
	Satellite Satellite `json:"satellite"`
	Status    Status    `json:"status"`
}

type ModifySatelliteRequest struct {
	SatelliteName        string `json:"satellite_name,omitempty"`
	SatelliteDescription string `json:"satellite_description,omitempty"`
	SatellitePlatform    string `json:"satellite_platform,omitempty"`
	SatelliteVersion     string `json:"satellite_version,omitempty"`
	IPAddress            string `json:"ip_address,omitempty"`
	NodeID               string `json:"node_id,omitempty"`
	PublicKey            string `json:"public_key,omitempty"`
	PrivateKey           string `json:"private_key,omitempty"`
}

type ModifySatelliteResponse struct {
	Message   string    `json:"message"`
	Success   bool      `json:"success"`
	Satellite Satellite `json:"satellite"`
	Status    Status    `json:"status"`
}

type DeleteSatelliteResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  Status `json:"status"`
}
