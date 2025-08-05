package mesh

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/redbco/redb-open/cmd/cli/internal/config"
	"github.com/redbco/redb-open/cmd/cli/internal/httpclient"
)

type Mesh struct {
	ID          string `json:"mesh_id"`
	Name        string `json:"mesh_name"`
	Description string `json:"mesh_description"`
	AllowJoin   bool   `json:"allow_join"`
	NodeCount   int32  `json:"node_count"`
	Status      string `json:"status"`
}

type Node struct {
	ID          string `json:"node_id"`
	Name        string `json:"node_name"`
	Description string `json:"node_description"`
	Platform    string `json:"node_platform"`
	Version     string `json:"node_version"`
	RegionID    string `json:"region_id"`
	RegionName  string `json:"region_name"`
	PublicKey   string `json:"public_key"`
	PrivateKey  string `json:"private_key"`
	IPAddress   string `json:"ip_address"`
	Port        int32  `json:"port"`
	Status      string `json:"status"`
}

// SeedMeshResponse wraps the API response for seeding a mesh
type SeedMeshResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Status  string `json:"status"`
	Mesh    Mesh   `json:"mesh"`
}

// JoinMeshResponse wraps the API response for joining a mesh
type JoinMeshResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Status  string `json:"status"`
}

// ShowMeshResponse wraps the API response for showing mesh details
type ShowMeshResponse struct {
	Mesh Mesh `json:"mesh"`
}

// ListNodesResponse wraps the API response for listing nodes
type ListNodesResponse struct {
	Nodes []Node `json:"nodes"`
}

type SeedMeshRequest struct {
	MeshName        string `json:"mesh_name"`
	MeshDescription string `json:"mesh_description,omitempty"`
	AllowJoin       bool   `json:"allow_join"`
	JoinKey         string `json:"join_key,omitempty"`
}

type JoinMeshRequest struct {
	MeshID          string `json:"mesh_id"`
	NodeName        string `json:"node_name"`
	NodeDescription string `json:"node_description,omitempty"`
	JoinKey         string `json:"join_key,omitempty"`
}

// SeedMesh creates a new mesh network
func SeedMesh(args []string) error {
	serviceURL, err := config.GetServiceAPIURL()
	if err != nil {
		return err
	}

	reader := bufio.NewReader(os.Stdin)

	// Get mesh name
	fmt.Print("Mesh name: ")
	meshName, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read mesh name: %v", err)
	}
	meshName = strings.TrimSpace(meshName)

	if meshName == "" {
		return fmt.Errorf("mesh name cannot be empty")
	}

	// Get mesh description (optional)
	fmt.Print("Mesh description (optional): ")
	meshDescription, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read mesh description: %v", err)
	}
	meshDescription = strings.TrimSpace(meshDescription)

	// Note: Node details are retrieved from the existing local node in the database
	// No need to prompt for node information

	// Get allow join setting
	fmt.Print("Allow other nodes to join? (y/n) [y]: ")
	allowJoinStr, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read allow join setting: %v", err)
	}
	allowJoinStr = strings.TrimSpace(strings.ToLower(allowJoinStr))
	allowJoin := allowJoinStr == "" || allowJoinStr == "y" || allowJoinStr == "yes"

	// Create the request
	seedReq := SeedMeshRequest{
		MeshName:        meshName,
		MeshDescription: meshDescription,
		AllowJoin:       allowJoin,
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/mesh/seed", serviceURL)

	var response SeedMeshResponse
	if err := client.Post(url, seedReq, &response, false); err != nil {
		return fmt.Errorf("failed to seed mesh: %v", err)
	}

	if !response.Success {
		return fmt.Errorf("failed to seed mesh: %s", response.Message)
	}

	fmt.Printf("✅ Successfully seeded mesh network!\n\n")
	fmt.Printf("Mesh ID: %s\n", response.Mesh.ID)
	fmt.Printf("Mesh Name: %s\n", response.Mesh.Name)
	fmt.Printf("Description: %s\n", response.Mesh.Description)
	fmt.Printf("Allow Join: %t\n", response.Mesh.AllowJoin)
	fmt.Printf("Node Count: %d\n", response.Mesh.NodeCount)
	fmt.Printf("Status: %s\n", response.Mesh.Status)
	fmt.Printf("\nMessage: %s\n", response.Message)

	return nil
}

// JoinMesh joins an existing mesh network
func JoinMesh(meshID string) error {
	serviceURL, err := config.GetServiceAPIURL()
	if err != nil {
		return err
	}

	reader := bufio.NewReader(os.Stdin)

	// Get node name
	fmt.Print("Node name: ")
	nodeName, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read node name: %v", err)
	}
	nodeName = strings.TrimSpace(nodeName)

	if nodeName == "" {
		return fmt.Errorf("node name cannot be empty")
	}

	// Get node description (optional)
	fmt.Print("Node description (optional): ")
	nodeDescription, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read node description: %v", err)
	}
	nodeDescription = strings.TrimSpace(nodeDescription)

	// Get join key (optional for now)
	fmt.Print("Join key (optional): ")
	joinKey, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read join key: %v", err)
	}
	joinKey = strings.TrimSpace(joinKey)

	// Create the request
	joinReq := JoinMeshRequest{
		MeshID:          meshID,
		NodeName:        nodeName,
		NodeDescription: nodeDescription,
		JoinKey:         joinKey,
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/mesh/join", serviceURL)

	var response JoinMeshResponse
	if err := client.Post(url, joinReq, &response, false); err != nil {
		return fmt.Errorf("failed to join mesh: %v", err)
	}

	if !response.Success {
		return fmt.Errorf("failed to join mesh: %s", response.Message)
	}

	fmt.Printf("✅ Successfully joined mesh network!\n\n")
	fmt.Printf("Mesh ID: %s\n", meshID)
	fmt.Printf("Status: %s\n", response.Status)
	fmt.Printf("Message: %s\n", response.Message)

	return nil
}

// ShowMesh displays detailed information about a mesh
func ShowMesh(meshID string) error {
	serviceURL, err := config.GetServiceAPIURL()
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/mesh/%s", serviceURL, meshID)

	var response ShowMeshResponse
	if err := client.Get(url, &response, false); err != nil {
		return fmt.Errorf("failed to get mesh details: %v", err)
	}

	mesh := response.Mesh

	fmt.Printf("Mesh Details:\n\n")
	fmt.Printf("ID: %s\n", mesh.ID)
	fmt.Printf("Name: %s\n", mesh.Name)
	fmt.Printf("Description: %s\n", mesh.Description)
	fmt.Printf("Allow Join: %t\n", mesh.AllowJoin)
	fmt.Printf("Node Count: %d\n", mesh.NodeCount)
	fmt.Printf("Status: %s\n", mesh.Status)

	return nil
}

// ListNodes displays all nodes in a mesh
func ListNodes(meshID string) error {
	serviceURL, err := config.GetServiceAPIURL()
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/mesh/%s/nodes", serviceURL, meshID)

	var response ListNodesResponse
	if err := client.Get(url, &response, false); err != nil {
		return fmt.Errorf("failed to get nodes: %v", err)
	}

	nodes := response.Nodes

	if len(nodes) == 0 {
		fmt.Printf("No nodes found in mesh %s\n", meshID)
		return nil
	}

	fmt.Printf("Nodes in mesh %s:\n\n", meshID)

	// Create a table writer
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "ID\tNAME\tDESCRIPTION\tPLATFORM\tVERSION\tIP\tPORT\tSTATUS")
	fmt.Fprintln(w, "---\t----\t-----------\t--------\t-------\t--\t----\t------")

	for _, node := range nodes {
		description := node.Description
		if description == "" {
			description = "-"
		}

		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\n",
			node.ID,
			node.Name,
			description,
			node.Platform,
			node.Version,
			node.IPAddress,
			node.Port,
			node.Status,
		)
	}

	_ = w.Flush()

	return nil
}
