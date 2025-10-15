package engine

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

type ListTransformationsResponse struct {
	Transformations []Transformation `json:"transformations"`
}

type ShowTransformationResponse struct {
	Transformation Transformation `json:"transformation"`
}

type AddTransformationRequest struct {
	TransformationName        string `json:"transformation_name" validate:"required"`
	TransformationDescription string `json:"transformation_description" validate:"required"`
	TransformationType        string `json:"transformation_type" validate:"required"`
	TransformationVersion     string `json:"transformation_version" validate:"required"`
	TransformationFunction    string `json:"transformation_function" validate:"required"`
}

type AddTransformationResponse struct {
	Message        string         `json:"message"`
	Success        bool           `json:"success"`
	Transformation Transformation `json:"transformation"`
	Status         Status         `json:"status"`
}

type ModifyTransformationRequest struct {
	TransformationNameNew     string `json:"transformation_name_new,omitempty"`
	TransformationDescription string `json:"transformation_description,omitempty"`
	TransformationType        string `json:"transformation_type,omitempty"`
	TransformationVersion     string `json:"transformation_version,omitempty"`
	TransformationFunction    string `json:"transformation_function,omitempty"`
}

type ModifyTransformationResponse struct {
	Message        string         `json:"message"`
	Success        bool           `json:"success"`
	Transformation Transformation `json:"transformation"`
	Status         Status         `json:"status"`
}

type DeleteTransformationResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  Status `json:"status"`
}
