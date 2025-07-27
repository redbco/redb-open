package models

// ElasticsearchSchema represents the schema of an Elasticsearch cluster
type ElasticsearchSchema struct {
	SchemaType string               `json:"schemaType"`
	Indices    []ElasticsearchIndex `json:"indices"`
	Mappings   []Mapping            `json:"mappings"`
	Settings   []Setting            `json:"settings"`
	Aliases    []Alias              `json:"aliases"`
}

// ElasticsearchIndex represents an Elasticsearch index
type ElasticsearchIndex struct {
	Name             string                 `json:"name"`
	Settings         map[string]interface{} `json:"settings"`
	Mappings         map[string]interface{} `json:"mappings"`
	Aliases          []string               `json:"aliases"`
	NumberOfShards   int                    `json:"numberOfShards"`
	NumberOfReplicas int                    `json:"numberOfReplicas"`
	Comment          string                 `json:"comment"`
}

// Mapping represents an Elasticsearch mapping
type Mapping struct {
	Name             string                 `json:"name"`
	Properties       map[string]interface{} `json:"properties"`
	Dynamic          bool                   `json:"dynamic"`
	DynamicTemplates []DynamicTemplate      `json:"dynamicTemplates"`
	Comment          string                 `json:"comment"`
}

// DynamicTemplate represents an Elasticsearch dynamic template
type DynamicTemplate struct {
	Name             string                 `json:"name"`
	Match            string                 `json:"match"`
	Unmatch          string                 `json:"unmatch"`
	MatchMappingType string                 `json:"matchMappingType"`
	PathMatch        string                 `json:"pathMatch"`
	PathUnmatch      string                 `json:"pathUnmatch"`
	Mapping          map[string]interface{} `json:"mapping"`
}

// Alias represents an Elasticsearch alias
type Alias struct {
	Name         string                 `json:"name"`
	Indices      []string               `json:"indices"`
	Filter       map[string]interface{} `json:"filter"`
	Routing      string                 `json:"routing"`
	IsWriteIndex bool                   `json:"isWriteIndex"`
	Comment      string                 `json:"comment"`
}

// ElasticsearchField represents a field in an Elasticsearch mapping
type ElasticsearchField struct {
	Name            string                 `json:"name"`
	Type            string                 `json:"type"`
	Format          string                 `json:"format"`
	Analyzer        string                 `json:"analyzer"`
	SearchAnalyzer  string                 `json:"searchAnalyzer"`
	Normalizer      string                 `json:"normalizer"`
	Fields          map[string]interface{} `json:"fields"`
	Properties      map[string]interface{} `json:"properties"`
	Enabled         bool                   `json:"enabled"`
	Index           bool                   `json:"index"`
	DocValues       bool                   `json:"docValues"`
	Store           bool                   `json:"store"`
	Boost           float64                `json:"boost"`
	NullValue       interface{}            `json:"nullValue"`
	CopyTo          []string               `json:"copyTo"`
	IgnoreAbove     int                    `json:"ignoreAbove"`
	IgnoreMalformed bool                   `json:"ignoreMalformed"`
	Coerce          bool                   `json:"coerce"`
	Comment         string                 `json:"comment"`
}
