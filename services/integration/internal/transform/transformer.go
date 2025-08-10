package transform

import (
	"time"

	integrationv1 "github.com/redbco/redb-open/api/proto/integration/v1"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type DataTransformer struct{}

func NewDataTransformer() *DataTransformer { return &DataTransformer{} }

func (t *DataTransformer) ToDocument(raw map[string]any, metadata map[string]any) (*integrationv1.Document, error) {
	meta, _ := structpb.NewStruct(metadata)
	content := ""
	if v, ok := raw["content"].(string); ok {
		content = v
	}
	id := ""
	if v, ok := raw["id"].(string); ok {
		id = v
	}
	return &integrationv1.Document{
		Id:        id,
		Content:   content,
		Metadata:  meta,
		CreatedAt: timestamppb.New(time.Now()),
	}, nil
}
