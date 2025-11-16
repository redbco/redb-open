package config

// Model defines the data structures for stream configurations
type Model struct {
	ID              string
	TenantID        string
	Name            string
	Description     string
	Platform        string
	ConnectedNodeID int64
	Status          string
}
