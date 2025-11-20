package engine

import (
	"google.golang.org/protobuf/types/known/structpb"
)

// convertMapToStruct converts a map[string]interface{} to a protobuf Struct
func convertMapToStruct(m map[string]interface{}) *structpb.Struct {
	if m == nil {
		return nil
	}

	s, err := structpb.NewStruct(m)
	if err != nil {
		return nil
	}

	return s
}
