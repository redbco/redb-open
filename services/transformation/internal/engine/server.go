package engine

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	pb "github.com/redbco/redb-open/api/proto/transformation/v1"
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
	default:
		return "", fmt.Errorf("unknown transformation function: %s", req.FunctionName)
	}
}
