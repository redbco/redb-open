package engine

import (
	pb "github.com/redbco/redb-open/api/proto/transformation/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

// BuiltInTransformation represents a built-in transformation with its metadata and I/O definitions
type BuiltInTransformation struct {
	Name           string
	Description    string
	Type           string
	Cardinality    string
	RequiresInput  bool
	ProducesOutput bool
	Implementation string
	IODefinitions  []IODefinition
	ExecuteFunc    interface{} // Function pointer to execute the transformation
}

// IODefinition defines an input or output for a transformation
type IODefinition struct {
	Name            string
	IOType          string // "input" or "output"
	DataType        string
	IsMandatory     bool
	IsArray         bool
	DefaultValue    interface{}
	Description     string
	ValidationRules map[string]interface{}
}

// GetBuiltInTransformations returns all built-in transformation definitions
func GetBuiltInTransformations() []BuiltInTransformation {
	return []BuiltInTransformation{
		{
			Name:           "direct_mapping",
			Description:    "Direct mapping with no transformation (passthrough)",
			Type:           "passthrough",
			Cardinality:    "one-to-one",
			RequiresInput:  true,
			ProducesOutput: true,
			Implementation: "transformDirectMapping",
			IODefinitions: []IODefinition{
				{
					Name:        "value",
					IOType:      "input",
					DataType:    "any",
					IsMandatory: true,
					Description: "The value to pass through",
				},
				{
					Name:        "result",
					IOType:      "output",
					DataType:    "any",
					Description: "The passthrough value",
				},
			},
			ExecuteFunc: transformDirectMapping,
		},
		{
			Name:           "uppercase",
			Description:    "Convert text to uppercase",
			Type:           "passthrough",
			Cardinality:    "one-to-one",
			RequiresInput:  true,
			ProducesOutput: true,
			Implementation: "transformUppercase",
			IODefinitions: []IODefinition{
				{
					Name:        "value",
					IOType:      "input",
					DataType:    "string",
					IsMandatory: true,
					Description: "The string to convert to uppercase",
				},
				{
					Name:        "result",
					IOType:      "output",
					DataType:    "string",
					Description: "The uppercase string",
				},
			},
			ExecuteFunc: transformUppercase,
		},
		{
			Name:           "lowercase",
			Description:    "Convert text to lowercase",
			Type:           "passthrough",
			Cardinality:    "one-to-one",
			RequiresInput:  true,
			ProducesOutput: true,
			Implementation: "transformLowercase",
			IODefinitions: []IODefinition{
				{
					Name:        "value",
					IOType:      "input",
					DataType:    "string",
					IsMandatory: true,
					Description: "The string to convert to lowercase",
				},
				{
					Name:        "result",
					IOType:      "output",
					DataType:    "string",
					Description: "The lowercase string",
				},
			},
			ExecuteFunc: transformLowercase,
		},
		{
			Name:           "reverse",
			Description:    "Reverse the input string",
			Type:           "passthrough",
			Cardinality:    "one-to-one",
			RequiresInput:  true,
			ProducesOutput: true,
			Implementation: "transformReverse",
			IODefinitions: []IODefinition{
				{
					Name:        "value",
					IOType:      "input",
					DataType:    "string",
					IsMandatory: true,
					Description: "The string to reverse",
				},
				{
					Name:        "result",
					IOType:      "output",
					DataType:    "string",
					Description: "The reversed string",
				},
			},
			ExecuteFunc: transformReverse,
		},
		{
			Name:           "base64_encode",
			Description:    "Encode input to base64",
			Type:           "passthrough",
			Cardinality:    "one-to-one",
			RequiresInput:  true,
			ProducesOutput: true,
			Implementation: "transformBase64Encode",
			IODefinitions: []IODefinition{
				{
					Name:        "value",
					IOType:      "input",
					DataType:    "string",
					IsMandatory: true,
					Description: "The string to encode",
				},
				{
					Name:        "result",
					IOType:      "output",
					DataType:    "string",
					Description: "The base64 encoded string",
				},
			},
			ExecuteFunc: transformBase64Encode,
		},
		{
			Name:           "base64_decode",
			Description:    "Decode base64 input",
			Type:           "passthrough",
			Cardinality:    "one-to-one",
			RequiresInput:  true,
			ProducesOutput: true,
			Implementation: "transformBase64Decode",
			IODefinitions: []IODefinition{
				{
					Name:        "value",
					IOType:      "input",
					DataType:    "string",
					IsMandatory: true,
					Description: "The base64 string to decode",
				},
				{
					Name:        "result",
					IOType:      "output",
					DataType:    "string",
					Description: "The decoded string",
				},
			},
			ExecuteFunc: transformBase64Decode,
		},
		{
			Name:           "json_format",
			Description:    "Format and validate JSON",
			Type:           "passthrough",
			Cardinality:    "one-to-one",
			RequiresInput:  true,
			ProducesOutput: true,
			Implementation: "transformJSONFormat",
			IODefinitions: []IODefinition{
				{
					Name:        "value",
					IOType:      "input",
					DataType:    "string",
					IsMandatory: true,
					Description: "The JSON string to format",
				},
				{
					Name:        "result",
					IOType:      "output",
					DataType:    "string",
					Description: "The formatted JSON string",
				},
			},
			ExecuteFunc: transformJSONFormat,
		},
		{
			Name:           "xml_format",
			Description:    "Format and validate XML",
			Type:           "passthrough",
			Cardinality:    "one-to-one",
			RequiresInput:  true,
			ProducesOutput: true,
			Implementation: "transformXMLFormat",
			IODefinitions: []IODefinition{
				{
					Name:        "value",
					IOType:      "input",
					DataType:    "string",
					IsMandatory: true,
					Description: "The XML string to format",
				},
				{
					Name:        "result",
					IOType:      "output",
					DataType:    "string",
					Description: "The formatted XML string",
				},
			},
			ExecuteFunc: transformXMLFormat,
		},
		{
			Name:           "csv_to_json",
			Description:    "Convert CSV to JSON",
			Type:           "passthrough",
			Cardinality:    "one-to-one",
			RequiresInput:  true,
			ProducesOutput: true,
			Implementation: "transformCSVToJSON",
			IODefinitions: []IODefinition{
				{
					Name:        "value",
					IOType:      "input",
					DataType:    "string",
					IsMandatory: true,
					Description: "The CSV string to convert",
				},
				{
					Name:        "result",
					IOType:      "output",
					DataType:    "json",
					Description: "The JSON representation of the CSV",
				},
			},
			ExecuteFunc: transformCSVToJSON,
		},
		{
			Name:           "json_to_csv",
			Description:    "Convert JSON to CSV",
			Type:           "passthrough",
			Cardinality:    "one-to-one",
			RequiresInput:  true,
			ProducesOutput: true,
			Implementation: "transformJSONToCSV",
			IODefinitions: []IODefinition{
				{
					Name:        "value",
					IOType:      "input",
					DataType:    "json",
					IsMandatory: true,
					Description: "The JSON array to convert",
				},
				{
					Name:        "result",
					IOType:      "output",
					DataType:    "string",
					Description: "The CSV representation",
				},
			},
			ExecuteFunc: transformJSONToCSV,
		},
		{
			Name:           "hash_sha256",
			Description:    "Generate SHA256 hash",
			Type:           "passthrough",
			Cardinality:    "one-to-one",
			RequiresInput:  true,
			ProducesOutput: true,
			Implementation: "transformHashSHA256",
			IODefinitions: []IODefinition{
				{
					Name:        "value",
					IOType:      "input",
					DataType:    "string",
					IsMandatory: true,
					Description: "The string to hash",
				},
				{
					Name:        "result",
					IOType:      "output",
					DataType:    "string",
					Description: "The SHA256 hash",
				},
			},
			ExecuteFunc: transformHashSHA256,
		},
		{
			Name:           "hash_md5",
			Description:    "Generate MD5 hash",
			Type:           "passthrough",
			Cardinality:    "one-to-one",
			RequiresInput:  true,
			ProducesOutput: true,
			Implementation: "transformHashMD5",
			IODefinitions: []IODefinition{
				{
					Name:        "value",
					IOType:      "input",
					DataType:    "string",
					IsMandatory: true,
					Description: "The string to hash",
				},
				{
					Name:        "result",
					IOType:      "output",
					DataType:    "string",
					Description: "The MD5 hash",
				},
			},
			ExecuteFunc: transformHashMD5,
		},
		{
			Name:           "url_encode",
			Description:    "URL encode the input",
			Type:           "passthrough",
			Cardinality:    "one-to-one",
			RequiresInput:  true,
			ProducesOutput: true,
			Implementation: "transformURLEncode",
			IODefinitions: []IODefinition{
				{
					Name:        "value",
					IOType:      "input",
					DataType:    "string",
					IsMandatory: true,
					Description: "The string to URL encode",
				},
				{
					Name:        "result",
					IOType:      "output",
					DataType:    "string",
					Description: "The URL encoded string",
				},
			},
			ExecuteFunc: transformURLEncode,
		},
		{
			Name:           "url_decode",
			Description:    "URL decode the input",
			Type:           "passthrough",
			Cardinality:    "one-to-one",
			RequiresInput:  true,
			ProducesOutput: true,
			Implementation: "transformURLDecode",
			IODefinitions: []IODefinition{
				{
					Name:        "value",
					IOType:      "input",
					DataType:    "string",
					IsMandatory: true,
					Description: "The URL encoded string to decode",
				},
				{
					Name:        "result",
					IOType:      "output",
					DataType:    "string",
					Description: "The decoded string",
				},
			},
			ExecuteFunc: transformURLDecode,
		},
		{
			Name:           "timestamp_to_iso",
			Description:    "Convert Unix timestamp to ISO 8601",
			Type:           "passthrough",
			Cardinality:    "one-to-one",
			RequiresInput:  true,
			ProducesOutput: true,
			Implementation: "transformTimestampToISO",
			IODefinitions: []IODefinition{
				{
					Name:        "value",
					IOType:      "input",
					DataType:    "string",
					IsMandatory: true,
					Description: "The Unix timestamp",
				},
				{
					Name:        "result",
					IOType:      "output",
					DataType:    "string",
					Description: "The ISO 8601 formatted timestamp",
				},
			},
			ExecuteFunc: transformTimestampToISO,
		},
		{
			Name:           "iso_to_timestamp",
			Description:    "Convert ISO 8601 to Unix timestamp",
			Type:           "passthrough",
			Cardinality:    "one-to-one",
			RequiresInput:  true,
			ProducesOutput: true,
			Implementation: "transformISOToTimestamp",
			IODefinitions: []IODefinition{
				{
					Name:        "value",
					IOType:      "input",
					DataType:    "string",
					IsMandatory: true,
					Description: "The ISO 8601 timestamp",
				},
				{
					Name:        "result",
					IOType:      "output",
					DataType:    "string",
					Description: "The Unix timestamp",
				},
			},
			ExecuteFunc: transformISOToTimestamp,
		},
		{
			Name:           "uuid_generator",
			Description:    "Generate a random UUID (no source required)",
			Type:           "generator",
			Cardinality:    "generator",
			RequiresInput:  false,
			ProducesOutput: true,
			Implementation: "transformUUIDGenerator",
			IODefinitions: []IODefinition{
				{
					Name:        "result",
					IOType:      "output",
					DataType:    "string",
					Description: "The generated UUID",
				},
			},
			ExecuteFunc: transformUUIDGenerator,
		},
		{
			Name:           "null_export",
			Description:    "Export data to external interface without mapping to target column",
			Type:           "sink",
			Cardinality:    "sink",
			RequiresInput:  true,
			ProducesOutput: false,
			Implementation: "transformNullExport",
			IODefinitions: []IODefinition{
				{
					Name:        "value",
					IOType:      "input",
					DataType:    "any",
					IsMandatory: true,
					Description: "The value to export",
				},
			},
			ExecuteFunc: transformNullExport,
		},
		{
			Name:           "combine_to_json",
			Description:    "Combine multiple inputs into a JSON object",
			Type:           "passthrough",
			Cardinality:    "many-to-one",
			RequiresInput:  true,
			ProducesOutput: true,
			Implementation: "transformCombineToJSON",
			IODefinitions: []IODefinition{
				{
					Name:        "inputs",
					IOType:      "input",
					DataType:    "any",
					IsMandatory: false,
					IsArray:     true,
					Description: "Variable number of inputs to combine",
				},
				{
					Name:        "result",
					IOType:      "output",
					DataType:    "json",
					Description: "The combined JSON object",
				},
			},
			ExecuteFunc: transformCombineToJSON,
		},
		{
			Name:           "split_json",
			Description:    "Split a JSON object into multiple outputs",
			Type:           "passthrough",
			Cardinality:    "one-to-many",
			RequiresInput:  true,
			ProducesOutput: true,
			Implementation: "transformSplitJSON",
			IODefinitions: []IODefinition{
				{
					Name:        "value",
					IOType:      "input",
					DataType:    "json",
					IsMandatory: true,
					Description: "The JSON object to split",
				},
				{
					Name:        "outputs",
					IOType:      "output",
					DataType:    "any",
					IsArray:     true,
					Description: "Variable number of outputs from the JSON",
				},
			},
			ExecuteFunc: transformSplitJSON,
		},
	}
}

// ConvertIODefinitionToProto converts an IODefinition to protobuf format
func ConvertIODefinitionToProto(ioDef IODefinition) (*pb.TransformationIODefinition, error) {
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
			return nil, err
		}
	}

	validationRules, err := structpb.NewStruct(ioDef.ValidationRules)
	if err != nil {
		return nil, err
	}

	return &pb.TransformationIODefinition{
		IoId:            "", // Will be set when stored in database
		IoName:          ioDef.Name,
		IoType:          ioType,
		DataType:        ioDef.DataType,
		IsMandatory:     ioDef.IsMandatory,
		IsArray:         ioDef.IsArray,
		DefaultValue:    defaultValue,
		Description:     ioDef.Description,
		ValidationRules: validationRules,
	}, nil
}
