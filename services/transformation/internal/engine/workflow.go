package engine

import (
	"context"
	"fmt"
	"reflect"

	pb "github.com/redbco/redb-open/api/proto/transformation/v1"
	"github.com/redbco/redb-open/pkg/logger"
	"google.golang.org/protobuf/types/known/structpb"
)

// WorkflowEngine executes DAG-based transformation workflows
type WorkflowEngine struct {
	registry *TransformationRegistry
	logger   *logger.Logger
}

// NewWorkflowEngine creates a new workflow engine
func NewWorkflowEngine(registry *TransformationRegistry, logger *logger.Logger) *WorkflowEngine {
	return &WorkflowEngine{
		registry: registry,
		logger:   logger,
	}
}

// WorkflowDAG represents a directed acyclic graph of transformations
type WorkflowDAG struct {
	Nodes map[string]*WorkflowNodeData
	Edges []*WorkflowEdgeData
}

// WorkflowNodeData represents a node in the workflow
type WorkflowNodeData struct {
	Node           *pb.WorkflowNode
	Transformation *TransformationRecord
	Inputs         map[string]interface{}
	Outputs        map[string]interface{}
	Executed       bool
}

// WorkflowEdgeData represents an edge in the workflow
type WorkflowEdgeData struct {
	Edge *pb.WorkflowEdge
}

// BuildDAG constructs a workflow DAG from nodes and edges
func (we *WorkflowEngine) BuildDAG(nodes []*pb.WorkflowNode, edges []*pb.WorkflowEdge) (*WorkflowDAG, error) {
	dag := &WorkflowDAG{
		Nodes: make(map[string]*WorkflowNodeData),
		Edges: make([]*WorkflowEdgeData, 0),
	}

	// Build nodes
	for _, node := range nodes {
		nodeData := &WorkflowNodeData{
			Node:    node,
			Inputs:  make(map[string]interface{}),
			Outputs: make(map[string]interface{}),
		}

		// For transformation nodes, load the transformation record
		if node.NodeType == pb.NodeType_NODE_TYPE_TRANSFORMATION && node.TransformationId != nil {
			transformation, err := we.registry.GetTransformation(*node.TransformationId)
			if err != nil {
				return nil, fmt.Errorf("failed to load transformation for node %s: %w", node.NodeId, err)
			}
			nodeData.Transformation = transformation
		}

		dag.Nodes[node.NodeId] = nodeData
	}

	// Build edges
	for _, edge := range edges {
		// Validate that source and target nodes exist
		if _, exists := dag.Nodes[edge.SourceNodeId]; !exists {
			return nil, fmt.Errorf("source node not found: %s", edge.SourceNodeId)
		}
		if _, exists := dag.Nodes[edge.TargetNodeId]; !exists {
			return nil, fmt.Errorf("target node not found: %s", edge.TargetNodeId)
		}

		dag.Edges = append(dag.Edges, &WorkflowEdgeData{Edge: edge})
	}

	return dag, nil
}

// ValidateDAG validates the workflow DAG
func (we *WorkflowEngine) ValidateDAG(dag *WorkflowDAG) ([]string, []string, error) {
	var errors []string
	var warnings []string

	// Check for cycles using DFS
	if hasCycle(dag) {
		errors = append(errors, "workflow contains cycles")
	}

	// Validate each transformation node
	for nodeID, nodeData := range dag.Nodes {
		if nodeData.Node.NodeType == pb.NodeType_NODE_TYPE_TRANSFORMATION {
			if nodeData.Transformation == nil {
				errors = append(errors, fmt.Sprintf("node %s: transformation not loaded", nodeID))
				continue
			}

			// Check that all mandatory inputs have incoming edges
			for _, ioDef := range nodeData.Transformation.IODefinitions {
				if ioDef.IOType == "input" && ioDef.IsMandatory {
					hasInput := false
					for _, edge := range dag.Edges {
						if edge.Edge.TargetNodeId == nodeID && edge.Edge.TargetInputName == ioDef.Name {
							hasInput = true
							break
						}
					}
					if !hasInput && ioDef.DefaultValue == nil {
						errors = append(errors, fmt.Sprintf("node %s: mandatory input '%s' not connected", nodeID, ioDef.Name))
					}
				}
			}
		}

		// Validate that target nodes have inputs
		if nodeData.Node.NodeType == pb.NodeType_NODE_TYPE_TARGET {
			hasInput := false
			for _, edge := range dag.Edges {
				if edge.Edge.TargetNodeId == nodeID {
					hasInput = true
					break
				}
			}
			if !hasInput {
				errors = append(errors, fmt.Sprintf("target node %s has no inputs", nodeID))
			}
		}

		// Validate that source nodes have outputs
		if nodeData.Node.NodeType == pb.NodeType_NODE_TYPE_SOURCE {
			hasOutput := false
			for _, edge := range dag.Edges {
				if edge.Edge.SourceNodeId == nodeID {
					hasOutput = true
					break
				}
			}
			if !hasOutput {
				warnings = append(warnings, fmt.Sprintf("source node %s has no outputs", nodeID))
			}
		}
	}

	if len(errors) > 0 {
		return errors, warnings, fmt.Errorf("workflow validation failed with %d errors", len(errors))
	}

	return errors, warnings, nil
}

// ExecuteDAG executes the workflow DAG
func (we *WorkflowEngine) ExecuteDAG(ctx context.Context, dag *WorkflowDAG, sourceData map[string]*structpb.Value) (map[string]*structpb.Value, []string, error) {
	executionLog := []string{}
	targetData := make(map[string]*structpb.Value)

	// Initialize source nodes with input data
	for nodeID, nodeData := range dag.Nodes {
		if nodeData.Node.NodeType == pb.NodeType_NODE_TYPE_SOURCE {
			if data, exists := sourceData[nodeID]; exists {
				// Convert structpb.Value to interface{}
				value := convertStructpbValueToInterface(data)
				nodeData.Outputs["value"] = value
				nodeData.Executed = true
				executionLog = append(executionLog, fmt.Sprintf("Initialized source node %s", nodeID))
			}
		}
	}

	// Execute in topological order
	executionOrder, err := topologicalSort(dag)
	if err != nil {
		return nil, executionLog, fmt.Errorf("failed to determine execution order: %w", err)
	}

	for _, nodeID := range executionOrder {
		nodeData := dag.Nodes[nodeID]

		// Skip if already executed (source nodes)
		if nodeData.Executed {
			continue
		}

		switch nodeData.Node.NodeType {
		case pb.NodeType_NODE_TYPE_TRANSFORMATION:
			err := we.executeTransformationNode(nodeData, dag)
			if err != nil {
				return nil, executionLog, fmt.Errorf("failed to execute node %s: %w", nodeID, err)
			}
			executionLog = append(executionLog, fmt.Sprintf("Executed transformation node %s (%s)", nodeID, nodeData.Transformation.Name))

		case pb.NodeType_NODE_TYPE_TARGET:
			// Collect inputs for target node
			we.resolveNodeInputs(nodeData, dag)

			// Convert outputs to structpb.Value and add to targetData
			for key, value := range nodeData.Inputs {
				pbValue, err := convertInterfaceToStructpbValue(value)
				if err != nil {
					return nil, executionLog, fmt.Errorf("failed to convert target value: %w", err)
				}
				targetData[nodeID] = pbValue
				executionLog = append(executionLog, fmt.Sprintf("Collected target node %s with key %s", nodeID, key))
				break // Only use the first input for target
			}
		}

		nodeData.Executed = true
	}

	return targetData, executionLog, nil
}

// executeTransformationNode executes a single transformation node
func (we *WorkflowEngine) executeTransformationNode(nodeData *WorkflowNodeData, dag *WorkflowDAG) error {
	// Resolve inputs from incoming edges
	we.resolveNodeInputs(nodeData, dag)

	// Get the transformation function
	fn, err := we.registry.GetFunction(nodeData.Transformation.Implementation)
	if err != nil {
		return fmt.Errorf("function not found: %w", err)
	}

	// Execute the transformation based on its type
	var outputs map[string]interface{}

	switch nodeData.Transformation.Cardinality {
	case "one-to-one":
		outputs, err = we.executeOneToOne(fn, nodeData.Inputs)
	case "one-to-many":
		outputs, err = we.executeOneToMany(fn, nodeData.Inputs)
	case "many-to-one":
		outputs, err = we.executeManyToOne(fn, nodeData.Inputs)
	case "many-to-many":
		outputs, err = we.executeManyToMany(fn, nodeData.Inputs)
	case "generator":
		outputs, err = we.executeGenerator(fn)
	case "sink":
		err = we.executeSink(fn, nodeData.Inputs)
		outputs = make(map[string]interface{}) // No outputs for sink
	default:
		return fmt.Errorf("unsupported cardinality: %s", nodeData.Transformation.Cardinality)
	}

	if err != nil {
		return err
	}

	nodeData.Outputs = outputs
	return nil
}

// resolveNodeInputs resolves inputs for a node from incoming edges
func (we *WorkflowEngine) resolveNodeInputs(nodeData *WorkflowNodeData, dag *WorkflowDAG) {
	for _, edgeData := range dag.Edges {
		if edgeData.Edge.TargetNodeId == nodeData.Node.NodeId {
			sourceNode := dag.Nodes[edgeData.Edge.SourceNodeId]
			if sourceNode.Executed {
				if value, exists := sourceNode.Outputs[edgeData.Edge.SourceOutputName]; exists {
					nodeData.Inputs[edgeData.Edge.TargetInputName] = value
				}
			}
		}
	}
}

// ResolveDataFlow maps outputs to inputs across edges
func (we *WorkflowEngine) ResolveDataFlow(dag *WorkflowDAG) error {
	for _, edgeData := range dag.Edges {
		sourceNode := dag.Nodes[edgeData.Edge.SourceNodeId]
		targetNode := dag.Nodes[edgeData.Edge.TargetNodeId]

		// Check if source has the output
		if _, exists := sourceNode.Outputs[edgeData.Edge.SourceOutputName]; !exists {
			return fmt.Errorf("source node %s does not have output %s",
				edgeData.Edge.SourceNodeId, edgeData.Edge.SourceOutputName)
		}

		// Map to target input
		targetNode.Inputs[edgeData.Edge.TargetInputName] = sourceNode.Outputs[edgeData.Edge.SourceOutputName]
	}

	return nil
}

// Execute transformation functions based on cardinality

func (we *WorkflowEngine) executeOneToOne(fn interface{}, inputs map[string]interface{}) (map[string]interface{}, error) {
	input, exists := inputs["value"]
	if !exists {
		return nil, fmt.Errorf("input 'value' not found")
	}

	// Call function using reflection
	fnValue := reflect.ValueOf(fn)
	inputStr := fmt.Sprintf("%v", input)

	results := fnValue.Call([]reflect.Value{reflect.ValueOf(inputStr)})

	// Handle error return
	if len(results) == 2 {
		if !results[1].IsNil() {
			return nil, results[1].Interface().(error)
		}
		return map[string]interface{}{"result": results[0].Interface()}, nil
	}

	return map[string]interface{}{"result": results[0].Interface()}, nil
}

func (we *WorkflowEngine) executeOneToMany(fn interface{}, inputs map[string]interface{}) (map[string]interface{}, error) {
	// For split operations
	input, exists := inputs["value"]
	if !exists {
		return nil, fmt.Errorf("input 'value' not found")
	}

	fnValue := reflect.ValueOf(fn)
	inputStr := fmt.Sprintf("%v", input)

	results := fnValue.Call([]reflect.Value{reflect.ValueOf(inputStr)})

	if len(results) == 2 && !results[1].IsNil() {
		return nil, results[1].Interface().(error)
	}

	return map[string]interface{}{"outputs": results[0].Interface()}, nil
}

func (we *WorkflowEngine) executeManyToOne(fn interface{}, inputs map[string]interface{}) (map[string]interface{}, error) {
	// For combine operations
	fnValue := reflect.ValueOf(fn)
	results := fnValue.Call([]reflect.Value{reflect.ValueOf(inputs)})

	if len(results) == 2 && !results[1].IsNil() {
		return nil, results[1].Interface().(error)
	}

	return map[string]interface{}{"result": results[0].Interface()}, nil
}

func (we *WorkflowEngine) executeManyToMany(fn interface{}, inputs map[string]interface{}) (map[string]interface{}, error) {
	// Execute as many-to-one then one-to-many
	fnValue := reflect.ValueOf(fn)
	results := fnValue.Call([]reflect.Value{reflect.ValueOf(inputs)})

	if len(results) == 2 && !results[1].IsNil() {
		return nil, results[1].Interface().(error)
	}

	return results[0].Interface().(map[string]interface{}), nil
}

func (we *WorkflowEngine) executeGenerator(fn interface{}) (map[string]interface{}, error) {
	fnValue := reflect.ValueOf(fn)
	results := fnValue.Call([]reflect.Value{})

	return map[string]interface{}{"result": results[0].Interface()}, nil
}

func (we *WorkflowEngine) executeSink(fn interface{}, inputs map[string]interface{}) error {
	input, exists := inputs["value"]
	if !exists {
		return fmt.Errorf("input 'value' not found")
	}

	fnValue := reflect.ValueOf(fn)
	inputStr := fmt.Sprintf("%v", input)
	fnValue.Call([]reflect.Value{reflect.ValueOf(inputStr)})

	return nil
}

// Helper functions

func hasCycle(dag *WorkflowDAG) bool {
	visited := make(map[string]bool)
	recStack := make(map[string]bool)

	for nodeID := range dag.Nodes {
		if hasCycleUtil(nodeID, dag, visited, recStack) {
			return true
		}
	}

	return false
}

func hasCycleUtil(nodeID string, dag *WorkflowDAG, visited, recStack map[string]bool) bool {
	visited[nodeID] = true
	recStack[nodeID] = true

	// Check all adjacent nodes
	for _, edge := range dag.Edges {
		if edge.Edge.SourceNodeId == nodeID {
			targetID := edge.Edge.TargetNodeId
			if !visited[targetID] {
				if hasCycleUtil(targetID, dag, visited, recStack) {
					return true
				}
			} else if recStack[targetID] {
				return true
			}
		}
	}

	recStack[nodeID] = false
	return false
}

func topologicalSort(dag *WorkflowDAG) ([]string, error) {
	// Calculate in-degree for each node
	inDegree := make(map[string]int)
	for nodeID := range dag.Nodes {
		inDegree[nodeID] = 0
	}

	for _, edge := range dag.Edges {
		inDegree[edge.Edge.TargetNodeId]++
	}

	// Queue nodes with no incoming edges
	queue := []string{}
	for nodeID, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, nodeID)
		}
	}

	// Process nodes
	result := []string{}
	for len(queue) > 0 {
		// Dequeue
		nodeID := queue[0]
		queue = queue[1:]
		result = append(result, nodeID)

		// Reduce in-degree for adjacent nodes
		for _, edge := range dag.Edges {
			if edge.Edge.SourceNodeId == nodeID {
				inDegree[edge.Edge.TargetNodeId]--
				if inDegree[edge.Edge.TargetNodeId] == 0 {
					queue = append(queue, edge.Edge.TargetNodeId)
				}
			}
		}
	}

	if len(result) != len(dag.Nodes) {
		return nil, fmt.Errorf("cycle detected in workflow")
	}

	return result, nil
}

func convertStructpbValueToInterface(v *structpb.Value) interface{} {
	switch v.GetKind().(type) {
	case *structpb.Value_NullValue:
		return nil
	case *structpb.Value_NumberValue:
		return v.GetNumberValue()
	case *structpb.Value_StringValue:
		return v.GetStringValue()
	case *structpb.Value_BoolValue:
		return v.GetBoolValue()
	case *structpb.Value_StructValue:
		return v.GetStructValue().AsMap()
	case *structpb.Value_ListValue:
		list := v.GetListValue().GetValues()
		result := make([]interface{}, len(list))
		for i, item := range list {
			result[i] = convertStructpbValueToInterface(item)
		}
		return result
	default:
		return nil
	}
}

func convertInterfaceToStructpbValue(v interface{}) (*structpb.Value, error) {
	return structpb.NewValue(v)
}
