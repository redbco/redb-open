package engine

// Anchor represents an anchor
type Anchor struct {
	TenantID          string `json:"tenant_id"`
	AnchorID          string `json:"anchor_id"`
	AnchorName        string `json:"anchor_name"`
	AnchorDescription string `json:"anchor_description,omitempty"`
	AnchorPlatform    string `json:"anchor_platform"`
	AnchorVersion     string `json:"anchor_version"`
	IPAddress         string `json:"ip_address"`
	NodeID            string `json:"node_id"`
	Status            Status `json:"status"`
	OwnerID           string `json:"owner_id"`
}

type ListAnchorsResponse struct {
	Anchors []Anchor `json:"anchors"`
}

type ShowAnchorResponse struct {
	Anchor Anchor `json:"anchor"`
}

type AddAnchorRequest struct {
	AnchorName        string `json:"anchor_name" validate:"required"`
	AnchorDescription string `json:"anchor_description,omitempty"`
	AnchorPlatform    string `json:"anchor_platform" validate:"required"`
	AnchorVersion     string `json:"anchor_version" validate:"required"`
	IPAddress         string `json:"ip_address" validate:"required"`
	NodeID            string `json:"node_id" validate:"required"`
	PublicKey         string `json:"public_key" validate:"required"`
	PrivateKey        string `json:"private_key" validate:"required"`
}

type AddAnchorResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Anchor  Anchor `json:"anchor"`
	Status  Status `json:"status"`
}

type ModifyAnchorRequest struct {
	AnchorName        string `json:"anchor_name,omitempty"`
	AnchorDescription string `json:"anchor_description,omitempty"`
	AnchorPlatform    string `json:"anchor_platform,omitempty"`
	AnchorVersion     string `json:"anchor_version,omitempty"`
	IPAddress         string `json:"ip_address,omitempty"`
	NodeID            string `json:"node_id,omitempty"`
	PublicKey         string `json:"public_key,omitempty"`
	PrivateKey        string `json:"private_key,omitempty"`
}

type ModifyAnchorResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Anchor  Anchor `json:"anchor"`
	Status  Status `json:"status"`
}

type DeleteAnchorResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  Status `json:"status"`
}
