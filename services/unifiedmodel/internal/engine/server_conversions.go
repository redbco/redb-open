package engine

import (
	"fmt"

	pb "github.com/redbco/redb-open/api/proto/unifiedmodel/v1"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/translator/core"
)

// Helper conversion functions for enhanced translation

// convertProtoToPreferences converts protobuf preferences to internal preferences
func (s *Server) convertProtoToPreferences(proto *pb.TranslationPreferences) core.TranslationPreferences {
	if proto == nil {
		return core.TranslationPreferences{
			AcceptDataLoss:         false,
			OptimizeForPerformance: true,
			PreserveRelationships:  true,
			IncludeMetadata:        true,
			GenerateComments:       true,
		}
	}

	return core.TranslationPreferences{
		AcceptDataLoss:         proto.AcceptDataLoss,
		OptimizeForPerformance: proto.OptimizeForPerformance,
		OptimizeForStorage:     proto.OptimizeForStorage,
		PreserveRelationships:  proto.PreserveRelationships,
		IncludeMetadata:        proto.IncludeMetadata,
		GenerateComments:       proto.GenerateComments,
		IncludeOriginalNames:   proto.IncludeOriginalNames,
		UseQualifiedNames:      proto.UseQualifiedNames,
		PreserveCaseStyle:      proto.PreserveCaseStyle,
		InteractiveMode:        proto.InteractiveMode,
		AutoApproveSimple:      proto.AutoApproveSimple,
		CustomMappings:         proto.CustomMappings,
		ExcludeObjects:         proto.ExcludeObjects,
	}
}

// convertProtoToSampleData converts protobuf sample data to internal sample data
func (s *Server) convertProtoToSampleData(proto *pb.UnifiedModelSampleData) *unifiedmodel.UnifiedModelSampleData {
	if proto == nil {
		return nil
	}

	sampleData := &unifiedmodel.UnifiedModelSampleData{
		TableSamples:      make(map[string]unifiedmodel.TableSampleData),
		CollectionSamples: make(map[string]unifiedmodel.CollectionSampleData),
		GraphSamples:      make(map[string]unifiedmodel.GraphSampleData),
		VectorSamples:     make(map[string]unifiedmodel.VectorSampleData),
	}

	// Convert table samples
	for name, tableSample := range proto.TableSamples {
		columns := make(map[string]unifiedmodel.ColumnSampleValues)
		for colName, colSample := range tableSample.Columns {
			// Convert string values to interface{}
			values := make([]interface{}, len(colSample.Values))
			for i, v := range colSample.Values {
				values[i] = v
			}

			col := unifiedmodel.ColumnSampleValues{
				FieldName:     colSample.ColumnName,
				DataType:      colSample.DataType,
				Values:        values,
				NullCount:     int(colSample.NullCount),
				DistinctCount: int(colSample.DistinctCount),
			}

			// Assign optional fields
			if colSample.MinValue != "" {
				col.MinValue = colSample.MinValue
			}
			if colSample.MaxValue != "" {
				col.MaxValue = colSample.MaxValue
			}

			columns[colName] = col
		}

		sampleData.TableSamples[name] = unifiedmodel.TableSampleData{
			TableName:   tableSample.TableName,
			RowCount:    tableSample.TotalRows,
			SampleCount: int(tableSample.SampleCount),
			Columns:     columns,
		}
	}

	// Convert collection samples
	for name, collSample := range proto.CollectionSamples {
		// Parse JSON strings to map[string]interface{}
		documents := make([]map[string]interface{}, len(collSample.Documents))
		// TODO: Implement JSON parsing for documents

		sampleData.CollectionSamples[name] = unifiedmodel.CollectionSampleData{
			CollectionName: collSample.CollectionName,
			DocumentCount:  collSample.DocumentCount,
			SampleCount:    int(collSample.SampleCount),
			Documents:      documents,
		}
	}

	// Convert graph samples
	for name, graphSample := range proto.GraphSamples {
		nodeSamples := make(map[string]unifiedmodel.NodeSampleData)
		for nodeLabel, nodeSample := range graphSample.NodeSamples {
			samples := make([]map[string]interface{}, len(nodeSample.Samples))
			// TODO: Implement JSON parsing for samples

			nodeSamples[nodeLabel] = unifiedmodel.NodeSampleData{
				NodeLabel: nodeSample.NodeLabel,
				Count:     nodeSample.Count,
				Samples:   samples,
			}
		}

		edgeSamples := make(map[string]unifiedmodel.EdgeSampleData)
		for edgeType, edgeSample := range graphSample.EdgeSamples {
			// Parse JSON strings to GraphEdgeSample structs
			samples := make([]unifiedmodel.GraphEdgeSample, len(edgeSample.Samples))
			// TODO: Implement JSON parsing for edge samples

			edgeSamples[edgeType] = unifiedmodel.EdgeSampleData{
				EdgeType: edgeSample.EdgeType,
				Count:    edgeSample.Count,
				Samples:  samples,
			}
		}

		sampleData.GraphSamples[name] = unifiedmodel.GraphSampleData{
			GraphName:   graphSample.GraphName,
			NodeSamples: nodeSamples,
			EdgeSamples: edgeSamples,
		}
	}

	return sampleData
}

// convertGeneratedMappingsToProto converts internal mappings to protobuf format
func (s *Server) convertGeneratedMappingsToProto(mappings []core.GeneratedMappingInfo) []*pb.GeneratedMapping {
	protoMappings := make([]*pb.GeneratedMapping, len(mappings))

	for i, mapping := range mappings {
		rules := make([]*pb.MappingRule, len(mapping.MappingRules))
		for j, rule := range mapping.MappingRules {
			// Convert transformation options
			transformOptions := make(map[string]string)
			for k, v := range rule.TransformationOptions {
				if str, ok := v.(string); ok {
					transformOptions[k] = str
				}
			}

			// Convert metadata
			metadata := make(map[string]string)
			for k, v := range rule.Metadata {
				if str, ok := v.(string); ok {
					metadata[k] = str
				}
			}

			// Convert default value to string
			defaultValue := ""
			if rule.DefaultValue != nil {
				defaultValue = fmt.Sprintf("%v", rule.DefaultValue)
			}

			rules[j] = &pb.MappingRule{
				RuleId:                rule.RuleID,
				SourceField:           rule.SourceField,
				TargetField:           rule.TargetField,
				SourceType:            rule.SourceType,
				TargetType:            rule.TargetType,
				Cardinality:           rule.Cardinality,
				TransformationId:      rule.TransformationID,
				TransformationName:    rule.TransformationName,
				TransformationOptions: transformOptions,
				IsRequired:            rule.IsRequired,
				DefaultValue:          defaultValue,
				Metadata:              metadata,
			}
		}

		// Convert mapping metadata
		metadata := make(map[string]string)
		for k, v := range mapping.Metadata {
			if str, ok := v.(string); ok {
				metadata[k] = str
			}
		}

		protoMappings[i] = &pb.GeneratedMapping{
			SourceIdentifier: mapping.SourceIdentifier,
			TargetIdentifier: mapping.TargetIdentifier,
			MappingType:      mapping.MappingType,
			MappingRules:     rules,
			Metadata:         metadata,
		}
	}

	return protoMappings
}

// convertUserDecisionsToProto converts internal user decisions to protobuf format
func (s *Server) convertUserDecisionsToProto(decisions []core.PendingUserDecision) []*pb.PendingUserDecision {
	protoDecisions := make([]*pb.PendingUserDecision, len(decisions))

	for i, decision := range decisions {
		protoDecisions[i] = &pb.PendingUserDecision{
			DecisionId:   decision.DecisionID,
			ObjectType:   decision.ObjectType,
			ObjectName:   decision.ObjectName,
			DecisionType: string(decision.DecisionType),
			Context:      decision.Context,
			Options:      decision.Options,
			Recommended:  decision.Recommended,
		}
	}

	return protoDecisions
}
