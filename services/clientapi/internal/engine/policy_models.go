package engine

// Policy represents a policy
type Policy struct {
	TenantID          string      `json:"tenant_id"`
	PolicyID          string      `json:"policy_id"`
	PolicyName        string      `json:"policy_name"`
	PolicyDescription string      `json:"policy_description,omitempty"`
	PolicyObject      interface{} `json:"policy_object"`
	OwnerID           string      `json:"owner_id"`
}

type ListPoliciesResponse struct {
	Policies []Policy `json:"policies"`
}

type ShowPolicyResponse struct {
	Policy Policy `json:"policy"`
}

type AddPolicyRequest struct {
	PolicyName        string      `json:"policy_name" validate:"required"`
	PolicyDescription string      `json:"policy_description" validate:"required"`
	PolicyObject      interface{} `json:"policy_object" validate:"required"`
}

type AddPolicyResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Policy  Policy `json:"policy"`
	Status  Status `json:"status"`
}

type ModifyPolicyRequest struct {
	PolicyNameNew     string      `json:"policy_name_new,omitempty"`
	PolicyDescription string      `json:"policy_description,omitempty"`
	PolicyObject      interface{} `json:"policy_object,omitempty"`
}

type ModifyPolicyResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Policy  Policy `json:"policy"`
	Status  Status `json:"status"`
}

type DeletePolicyResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  Status `json:"status"`
}
