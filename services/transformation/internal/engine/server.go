package engine

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	pb "github.com/redbco/redb-open/api/proto/transformation/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

type TransformationServer struct {
	pb.UnimplementedTransformationServiceServer
	engine *Engine
}

func NewTransformationServer(engine *Engine) *TransformationServer {
	return &TransformationServer{
		engine: engine,
	}
}

func (s *TransformationServer) Transform(ctx context.Context, req *pb.TransformRequest) (*pb.TransformResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()

	// Increment requests processed metric
	atomic.AddInt64(&s.engine.metrics.requestsProcessed, 1)

	// Create context with timeout
	timeoutStr := s.engine.config.Get("services.transformation.timeout")
	timeout := 30 * time.Second // default timeout
	if timeoutStr != "" {
		if parsedTimeout, err := time.ParseDuration(timeoutStr + "s"); err == nil {
			timeout = parsedTimeout
		}
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Validate request
	if req.FunctionName == "" {
		atomic.AddInt64(&s.engine.metrics.errors, 1)
		return &pb.TransformResponse{
			Output:        "",
			StatusMessage: "function_name is required",
			Status:        commonv1.Status_STATUS_FAILURE,
		}, nil
	}

	// Execute transformation function
	output, err := s.executeTransformation(req)
	if err != nil {
		atomic.AddInt64(&s.engine.metrics.errors, 1)
		return &pb.TransformResponse{
			Output:        "",
			StatusMessage: err.Error(),
			Status:        commonv1.Status_STATUS_ERROR,
		}, nil
	}

	return &pb.TransformResponse{
		Output:        output,
		StatusMessage: "transformation completed successfully",
		Status:        commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *TransformationServer) executeTransformation(req *pb.TransformRequest) (string, error) {
	// Route to specific transformation function based on function_name
	switch req.FunctionName {
	case "direct_mapping":
		return transformDirectMapping(req.Input), nil
	case "uppercase":
		return transformUppercase(req.Input), nil
	case "lowercase":
		return transformLowercase(req.Input), nil
	case "reverse":
		return transformReverse(req.Input), nil
	case "base64_encode":
		return transformBase64Encode(req.Input), nil
	case "base64_decode":
		return transformBase64Decode(req.Input)
	case "json_format":
		return transformJSONFormat(req.Input)
	case "xml_format":
		return transformXMLFormat(req.Input)
	case "csv_to_json":
		return transformCSVToJSON(req.Input)
	case "json_to_csv":
		return transformJSONToCSV(req.Input)
	case "hash_sha256":
		return transformHashSHA256(req.Input), nil
	case "hash_md5":
		return transformHashMD5(req.Input), nil
	case "url_encode":
		return transformURLEncode(req.Input), nil
	case "url_decode":
		return transformURLDecode(req.Input)
	case "timestamp_to_iso":
		return transformTimestampToISO(req.Input)
	case "iso_to_timestamp":
		return transformISOToTimestamp(req.Input)
	case "uuid_generator":
		return transformUUIDGenerator(), nil
	case "null_export":
		return transformNullExport(req.Input), nil
	default:
		return "", fmt.Errorf("unknown transformation function: %s", req.FunctionName)
	}
}

// GetTransformationMetadata returns metadata about a specific transformation
func (s *TransformationServer) GetTransformationMetadata(ctx context.Context, req *pb.GetTransformationMetadataRequest) (*pb.GetTransformationMetadataResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()

	atomic.AddInt64(&s.engine.metrics.requestsProcessed, 1)

	// Validate request
	if req.TransformationName == "" {
		atomic.AddInt64(&s.engine.metrics.errors, 1)
		return &pb.GetTransformationMetadataResponse{
			Metadata:      nil,
			StatusMessage: "transformation_name is required",
			Status:        commonv1.Status_STATUS_FAILURE,
		}, nil
	}

	// Get metadata for the requested transformation
	metadata, exists := getTransformationMetadata(req.TransformationName)
	if !exists {
		atomic.AddInt64(&s.engine.metrics.errors, 1)
		return &pb.GetTransformationMetadataResponse{
			Metadata:      nil,
			StatusMessage: fmt.Sprintf("transformation '%s' not found", req.TransformationName),
			Status:        commonv1.Status_STATUS_FAILURE,
		}, nil
	}

	return &pb.GetTransformationMetadataResponse{
		Metadata:      metadata,
		StatusMessage: "metadata retrieved successfully",
		Status:        commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// ListTransformations returns a list of all available transformations
func (s *TransformationServer) ListTransformations(ctx context.Context, req *pb.ListTransformationsRequest) (*pb.ListTransformationsResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()

	atomic.AddInt64(&s.engine.metrics.requestsProcessed, 1)

	// Get all transformation metadata
	transformations := getAllTransformationMetadata()

	return &pb.ListTransformationsResponse{
		Transformations: transformations,
	}, nil
}

// getTransformationMetadata returns metadata for a specific transformation
func getTransformationMetadata(name string) (*pb.TransformationMetadata, bool) {
	metadataMap := map[string]*pb.TransformationMetadata{
		"direct_mapping": {
			Name:                  "direct_mapping",
			Description:           "Direct mapping with no transformation (passthrough)",
			Type:                  "passthrough",
			RequiresSource:        true,
			RequiresTarget:        true,
			AllowsMultipleTargets: true,
		},
		"uppercase": {
			Name:                  "uppercase",
			Description:           "Convert text to uppercase",
			Type:                  "passthrough",
			RequiresSource:        true,
			RequiresTarget:        true,
			AllowsMultipleTargets: true,
		},
		"lowercase": {
			Name:                  "lowercase",
			Description:           "Convert text to lowercase",
			Type:                  "passthrough",
			RequiresSource:        true,
			RequiresTarget:        true,
			AllowsMultipleTargets: true,
		},
		"reverse": {
			Name:                  "reverse",
			Description:           "Reverse the input string",
			Type:                  "passthrough",
			RequiresSource:        true,
			RequiresTarget:        true,
			AllowsMultipleTargets: true,
		},
		"base64_encode": {
			Name:                  "base64_encode",
			Description:           "Encode input to base64",
			Type:                  "passthrough",
			RequiresSource:        true,
			RequiresTarget:        true,
			AllowsMultipleTargets: true,
		},
		"base64_decode": {
			Name:                  "base64_decode",
			Description:           "Decode base64 input",
			Type:                  "passthrough",
			RequiresSource:        true,
			RequiresTarget:        true,
			AllowsMultipleTargets: true,
		},
		"json_format": {
			Name:                  "json_format",
			Description:           "Format and validate JSON",
			Type:                  "passthrough",
			RequiresSource:        true,
			RequiresTarget:        true,
			AllowsMultipleTargets: true,
		},
		"xml_format": {
			Name:                  "xml_format",
			Description:           "Format and validate XML",
			Type:                  "passthrough",
			RequiresSource:        true,
			RequiresTarget:        true,
			AllowsMultipleTargets: true,
		},
		"csv_to_json": {
			Name:                  "csv_to_json",
			Description:           "Convert CSV to JSON",
			Type:                  "passthrough",
			RequiresSource:        true,
			RequiresTarget:        true,
			AllowsMultipleTargets: true,
		},
		"json_to_csv": {
			Name:                  "json_to_csv",
			Description:           "Convert JSON to CSV",
			Type:                  "passthrough",
			RequiresSource:        true,
			RequiresTarget:        true,
			AllowsMultipleTargets: true,
		},
		"hash_sha256": {
			Name:                  "hash_sha256",
			Description:           "Generate SHA256 hash",
			Type:                  "passthrough",
			RequiresSource:        true,
			RequiresTarget:        true,
			AllowsMultipleTargets: true,
		},
		"hash_md5": {
			Name:                  "hash_md5",
			Description:           "Generate MD5 hash",
			Type:                  "passthrough",
			RequiresSource:        true,
			RequiresTarget:        true,
			AllowsMultipleTargets: true,
		},
		"url_encode": {
			Name:                  "url_encode",
			Description:           "URL encode the input",
			Type:                  "passthrough",
			RequiresSource:        true,
			RequiresTarget:        true,
			AllowsMultipleTargets: true,
		},
		"url_decode": {
			Name:                  "url_decode",
			Description:           "URL decode the input",
			Type:                  "passthrough",
			RequiresSource:        true,
			RequiresTarget:        true,
			AllowsMultipleTargets: true,
		},
		"timestamp_to_iso": {
			Name:                  "timestamp_to_iso",
			Description:           "Convert Unix timestamp to ISO 8601",
			Type:                  "passthrough",
			RequiresSource:        true,
			RequiresTarget:        true,
			AllowsMultipleTargets: true,
		},
		"iso_to_timestamp": {
			Name:                  "iso_to_timestamp",
			Description:           "Convert ISO 8601 to Unix timestamp",
			Type:                  "passthrough",
			RequiresSource:        true,
			RequiresTarget:        true,
			AllowsMultipleTargets: true,
		},
		"uuid_generator": {
			Name:                  "uuid_generator",
			Description:           "Generate a random UUID (no source required)",
			Type:                  "generator",
			RequiresSource:        false,
			RequiresTarget:        true,
			AllowsMultipleTargets: true,
		},
		"null_export": {
			Name:                  "null_export",
			Description:           "Export data to external interface without mapping to target column",
			Type:                  "null_returning",
			RequiresSource:        true,
			RequiresTarget:        false,
			AllowsMultipleTargets: false,
		},
	}

	metadata, exists := metadataMap[name]
	return metadata, exists
}

// getAllTransformationMetadata returns all transformation metadata
func getAllTransformationMetadata() []*pb.TransformationMetadata {
	transformations := []string{
		"direct_mapping", "uppercase", "lowercase", "reverse",
		"base64_encode", "base64_decode", "json_format", "xml_format",
		"csv_to_json", "json_to_csv", "hash_sha256", "hash_md5",
		"url_encode", "url_decode", "timestamp_to_iso", "iso_to_timestamp",
		"uuid_generator", "null_export",
	}

	result := make([]*pb.TransformationMetadata, 0, len(transformations))
	for _, name := range transformations {
		if metadata, exists := getTransformationMetadata(name); exists {
			result = append(result, metadata)
		}
	}

	return result
}

// TransformWorkflow executes a workflow-based transformation
func (s *TransformationServer) TransformWorkflow(ctx context.Context, req *pb.TransformWorkflowRequest) (*pb.TransformWorkflowResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()

	atomic.AddInt64(&s.engine.metrics.requestsProcessed, 1)

	// Validate request
	if len(req.Nodes) == 0 {
		atomic.AddInt64(&s.engine.metrics.errors, 1)
		return &pb.TransformWorkflowResponse{
			TargetData:    nil,
			StatusMessage: "no workflow nodes provided",
			Status:        commonv1.Status_STATUS_FAILURE,
		}, nil
	}

	// Build DAG
	dag, err := s.engine.workflowEngine.BuildDAG(req.Nodes, req.Edges)
	if err != nil {
		atomic.AddInt64(&s.engine.metrics.errors, 1)
		return &pb.TransformWorkflowResponse{
			TargetData:    nil,
			StatusMessage: fmt.Sprintf("failed to build workflow DAG: %v", err),
			Status:        commonv1.Status_STATUS_ERROR,
		}, nil
	}

	// Validate DAG
	errors, warnings, err := s.engine.workflowEngine.ValidateDAG(dag)
	if err != nil {
		atomic.AddInt64(&s.engine.metrics.errors, 1)
		return &pb.TransformWorkflowResponse{
			TargetData:    nil,
			StatusMessage: fmt.Sprintf("workflow validation failed: %v", err),
			Status:        commonv1.Status_STATUS_FAILURE,
			ExecutionLog:  append(errors, warnings...),
		}, nil
	}

	// Execute DAG
	targetData, executionLog, err := s.engine.workflowEngine.ExecuteDAG(ctx, dag, req.SourceData)
	if err != nil {
		atomic.AddInt64(&s.engine.metrics.errors, 1)
		return &pb.TransformWorkflowResponse{
			TargetData:    nil,
			StatusMessage: fmt.Sprintf("workflow execution failed: %v", err),
			Status:        commonv1.Status_STATUS_ERROR,
			ExecutionLog:  executionLog,
		}, nil
	}

	return &pb.TransformWorkflowResponse{
		TargetData:    targetData,
		StatusMessage: "workflow executed successfully",
		Status:        commonv1.Status_STATUS_SUCCESS,
		ExecutionLog:  executionLog,
	}, nil
}

// ValidateWorkflow validates a workflow without executing it
func (s *TransformationServer) ValidateWorkflow(ctx context.Context, req *pb.ValidateWorkflowRequest) (*pb.ValidateWorkflowResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()

	atomic.AddInt64(&s.engine.metrics.requestsProcessed, 1)

	// Validate request
	if len(req.Nodes) == 0 {
		atomic.AddInt64(&s.engine.metrics.errors, 1)
		return &pb.ValidateWorkflowResponse{
			IsValid: false,
			Errors:  []string{"no workflow nodes provided"},
			Status:  commonv1.Status_STATUS_FAILURE,
		}, nil
	}

	// Build DAG
	dag, err := s.engine.workflowEngine.BuildDAG(req.Nodes, req.Edges)
	if err != nil {
		atomic.AddInt64(&s.engine.metrics.errors, 1)
		return &pb.ValidateWorkflowResponse{
			IsValid: false,
			Errors:  []string{fmt.Sprintf("failed to build workflow DAG: %v", err)},
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	// Validate DAG
	errors, warnings, err := s.engine.workflowEngine.ValidateDAG(dag)
	if err != nil {
		atomic.AddInt64(&s.engine.metrics.errors, 1)
		return &pb.ValidateWorkflowResponse{
			IsValid:  false,
			Errors:   errors,
			Warnings: warnings,
			Status:   commonv1.Status_STATUS_FAILURE,
		}, nil
	}

	return &pb.ValidateWorkflowResponse{
		IsValid:  true,
		Errors:   []string{},
		Warnings: warnings,
		Status:   commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// CreateTransformation creates a new transformation in the database
func (s *TransformationServer) CreateTransformation(ctx context.Context, req *pb.CreateTransformationRequest) (*pb.CreateTransformationResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()

	atomic.AddInt64(&s.engine.metrics.requestsProcessed, 1)

	// Validate request
	if req.TenantId == "" || req.TransformationName == "" {
		atomic.AddInt64(&s.engine.metrics.errors, 1)
		return &pb.CreateTransformationResponse{
			TransformationId: "",
			StatusMessage:    "tenant_id and transformation_name are required",
			Status:           commonv1.Status_STATUS_FAILURE,
		}, nil
	}

	// Convert protobuf request to database record
	metadata := make(map[string]interface{})
	if req.TransformationMetadata != nil {
		metadata = req.TransformationMetadata.AsMap()
	}

	record := &TransformationRecord{
		TenantID:       req.TenantId,
		Name:           req.TransformationName,
		Description:    req.TransformationDescription,
		Type:           req.TransformationType,
		Version:        "1.0.0",
		Function:       "",
		Cardinality:    req.TransformationCardinality,
		RequiresInput:  req.RequiresInput,
		ProducesOutput: req.ProducesOutput,
		Implementation: req.TransformationImplementation,
		Metadata:       metadata,
		Enabled:        true,
		OwnerID:        req.OwnerId,
	}

	// Convert I/O definitions
	for _, ioDef := range req.IoDefinitions {
		ioType := "input"
		if ioDef.IoType == pb.IOType_IO_TYPE_OUTPUT {
			ioType = "output"
		}

		var defaultValue interface{}
		if ioDef.DefaultValue != nil {
			defaultValue = ioDef.DefaultValue.AsInterface()
		}

		validationRules := make(map[string]interface{})
		if ioDef.ValidationRules != nil {
			validationRules = ioDef.ValidationRules.AsMap()
		}

		record.IODefinitions = append(record.IODefinitions, IODefinitionRecord{
			IOType:          ioType,
			Name:            ioDef.IoName,
			DataType:        ioDef.DataType,
			IsMandatory:     ioDef.IsMandatory,
			IsArray:         ioDef.IsArray,
			DefaultValue:    defaultValue,
			Description:     ioDef.Description,
			ValidationRules: validationRules,
		})
	}

	// Create transformation in database
	dbOps := NewDatabaseOps(s.engine.db, s.engine.logger)
	transformationID, err := dbOps.CreateTransformation(ctx, record)
	if err != nil {
		atomic.AddInt64(&s.engine.metrics.errors, 1)
		return &pb.CreateTransformationResponse{
			TransformationId: "",
			StatusMessage:    fmt.Sprintf("failed to create transformation: %v", err),
			Status:           commonv1.Status_STATUS_ERROR,
		}, nil
	}

	// Register in registry
	record.ID = transformationID
	s.engine.registry.RegisterTransformation(record)

	return &pb.CreateTransformationResponse{
		TransformationId: transformationID,
		StatusMessage:    "transformation created successfully",
		Status:           commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// GetTransformationIO retrieves I/O definitions for a transformation
func (s *TransformationServer) GetTransformationIO(ctx context.Context, req *pb.GetTransformationIORequest) (*pb.GetTransformationIOResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()

	atomic.AddInt64(&s.engine.metrics.requestsProcessed, 1)

	// Validate request
	if req.TransformationId == "" {
		atomic.AddInt64(&s.engine.metrics.errors, 1)
		return &pb.GetTransformationIOResponse{
			IoDefinitions: nil,
			StatusMessage: "transformation_id is required",
			Status:        commonv1.Status_STATUS_FAILURE,
		}, nil
	}

	// Get transformation from registry
	transformation, err := s.engine.registry.GetTransformation(req.TransformationId)
	if err != nil {
		atomic.AddInt64(&s.engine.metrics.errors, 1)
		return &pb.GetTransformationIOResponse{
			IoDefinitions: nil,
			StatusMessage: fmt.Sprintf("transformation not found: %v", err),
			Status:        commonv1.Status_STATUS_FAILURE,
		}, nil
	}

	// Convert I/O definitions to protobuf
	ioDefinitions := make([]*pb.TransformationIODefinition, 0, len(transformation.IODefinitions))
	for _, ioDef := range transformation.IODefinitions {
		var ioType pb.IOType
		if ioDef.IOType == "input" {
			ioType = pb.IOType_IO_TYPE_INPUT
		} else {
			ioType = pb.IOType_IO_TYPE_OUTPUT
		}

		var defaultValue *structpb.Value
		if ioDef.DefaultValue != nil {
			var err error
			defaultValue, err = structpb.NewValue(ioDef.DefaultValue)
			if err != nil {
				s.engine.logger.Warnf("Failed to convert default value: %v", err)
			}
		}

		validationRules, err := structpb.NewStruct(ioDef.ValidationRules)
		if err != nil {
			s.engine.logger.Warnf("Failed to convert validation rules: %v", err)
			validationRules = &structpb.Struct{}
		}

		ioDefinitions = append(ioDefinitions, &pb.TransformationIODefinition{
			IoId:            ioDef.ID,
			IoName:          ioDef.Name,
			IoType:          ioType,
			DataType:        ioDef.DataType,
			IsMandatory:     ioDef.IsMandatory,
			IsArray:         ioDef.IsArray,
			DefaultValue:    defaultValue,
			Description:     ioDef.Description,
			ValidationRules: validationRules,
		})
	}

	return &pb.GetTransformationIOResponse{
		IoDefinitions: ioDefinitions,
		StatusMessage: "I/O definitions retrieved successfully",
		Status:        commonv1.Status_STATUS_SUCCESS,
	}, nil
}
