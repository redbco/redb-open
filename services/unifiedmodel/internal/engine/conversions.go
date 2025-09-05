package engine

import (
	"time"

	pb "github.com/redbco/redb-open/api/proto/unifiedmodel/v1"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// convertEnrichmentToProto converts Go UnifiedModelEnrichment to protobuf
func (s *Server) convertEnrichmentToProto(enrichment *unifiedmodel.UnifiedModelEnrichment) *pb.UnifiedModelEnrichment {
	if enrichment == nil {
		return nil
	}

	protoEnrichment := &pb.UnifiedModelEnrichment{
		SchemaId:          enrichment.SchemaID,
		EnrichmentVersion: enrichment.EnrichmentVersion,
		GeneratedAt:       enrichment.GeneratedAt.Unix(),
		GeneratedBy:       enrichment.GeneratedBy,
		TableEnrichments:  make(map[string]*pb.TableEnrichment),
		ColumnEnrichments: make(map[string]*pb.ColumnEnrichment),
	}

	// Convert table enrichments
	for name, tableEnrich := range enrichment.TableEnrichments {
		protoEnrichment.TableEnrichments[name] = s.convertTableEnrichmentToProto(tableEnrich)
	}

	// Convert column enrichments
	for name, columnEnrich := range enrichment.ColumnEnrichments {
		protoEnrichment.ColumnEnrichments[name] = s.convertColumnEnrichmentToProto(columnEnrich)
	}

	return protoEnrichment
}

// convertProtoToEnrichment converts protobuf UnifiedModelEnrichment to Go
func (s *Server) convertProtoToEnrichment(protoEnrichment *pb.UnifiedModelEnrichment) *unifiedmodel.UnifiedModelEnrichment {
	if protoEnrichment == nil {
		return nil
	}

	enrichment := &unifiedmodel.UnifiedModelEnrichment{
		SchemaID:          protoEnrichment.SchemaId,
		EnrichmentVersion: protoEnrichment.EnrichmentVersion,
		GeneratedAt:       time.Unix(protoEnrichment.GeneratedAt, 0),
		GeneratedBy:       protoEnrichment.GeneratedBy,
		TableEnrichments:  make(map[string]unifiedmodel.TableEnrichment),
		ColumnEnrichments: make(map[string]unifiedmodel.ColumnEnrichment),
	}

	// Convert table enrichments
	for name, tableEnrich := range protoEnrichment.TableEnrichments {
		enrichment.TableEnrichments[name] = s.convertProtoToTableEnrichment(tableEnrich)
	}

	// Convert column enrichments
	for name, columnEnrich := range protoEnrichment.ColumnEnrichments {
		enrichment.ColumnEnrichments[name] = s.convertProtoToColumnEnrichment(columnEnrich)
	}

	return enrichment
}

func (s *Server) convertTableEnrichmentToProto(enrichment unifiedmodel.TableEnrichment) *pb.TableEnrichment {
	protoEnrichment := &pb.TableEnrichment{
		PrimaryCategory:          string(enrichment.PrimaryCategory),
		ClassificationConfidence: enrichment.ClassificationConfidence,
		AccessPattern:            string(enrichment.AccessPattern),
		HasPrivilegedData:        enrichment.HasPrivilegedData,
		PrivilegedColumns:        enrichment.PrivilegedColumns,
		DataSensitivity:          enrichment.DataSensitivity,
		RelatedTables:            enrichment.RelatedTables,
		DependentTables:          enrichment.DependentTables,
		BusinessPurpose:          enrichment.BusinessPurpose,
		Tags:                     enrichment.Tags,
		Context:                  enrichment.Context,
	}

	if enrichment.EstimatedRows != nil {
		protoEnrichment.EstimatedRows = *enrichment.EstimatedRows
	}

	if enrichment.GrowthRate != nil {
		protoEnrichment.GrowthRate = *enrichment.GrowthRate
	}

	if enrichment.QueryComplexity != nil {
		protoEnrichment.QueryComplexity = *enrichment.QueryComplexity
	}

	if enrichment.DataRetention != nil {
		protoEnrichment.DataRetentionDays = int64(enrichment.DataRetention.Hours() / 24)
	}

	// Convert classification scores
	for _, score := range enrichment.ClassificationScores {
		protoEnrichment.ClassificationScores = append(protoEnrichment.ClassificationScores, &pb.CategoryScore{
			Category: score.Category,
			Score:    score.Score,
			Reason:   score.Reason,
		})
	}

	return protoEnrichment
}

func (s *Server) convertProtoToTableEnrichment(protoEnrichment *pb.TableEnrichment) unifiedmodel.TableEnrichment {
	enrichment := unifiedmodel.TableEnrichment{
		PrimaryCategory:          unifiedmodel.TableCategory(protoEnrichment.PrimaryCategory),
		ClassificationConfidence: protoEnrichment.ClassificationConfidence,
		AccessPattern:            unifiedmodel.AccessPattern(protoEnrichment.AccessPattern),
		HasPrivilegedData:        protoEnrichment.HasPrivilegedData,
		PrivilegedColumns:        protoEnrichment.PrivilegedColumns,
		DataSensitivity:          protoEnrichment.DataSensitivity,
		RelatedTables:            protoEnrichment.RelatedTables,
		DependentTables:          protoEnrichment.DependentTables,
		BusinessPurpose:          protoEnrichment.BusinessPurpose,
		Tags:                     protoEnrichment.Tags,
		Context:                  protoEnrichment.Context,
	}

	if protoEnrichment.EstimatedRows > 0 {
		enrichment.EstimatedRows = &protoEnrichment.EstimatedRows
	}

	if protoEnrichment.GrowthRate > 0 {
		enrichment.GrowthRate = &protoEnrichment.GrowthRate
	}

	if protoEnrichment.QueryComplexity > 0 {
		enrichment.QueryComplexity = &protoEnrichment.QueryComplexity
	}

	if protoEnrichment.DataRetentionDays > 0 {
		retention := time.Duration(protoEnrichment.DataRetentionDays) * 24 * time.Hour
		enrichment.DataRetention = &retention
	}

	// Convert classification scores
	for _, score := range protoEnrichment.ClassificationScores {
		enrichment.ClassificationScores = append(enrichment.ClassificationScores, unifiedmodel.CategoryScore{
			Category: score.Category,
			Score:    score.Score,
			Reason:   score.Reason,
		})
	}

	return enrichment
}

func (s *Server) convertColumnEnrichmentToProto(enrichment unifiedmodel.ColumnEnrichment) *pb.ColumnEnrichment {
	protoEnrichment := &pb.ColumnEnrichment{
		IsPrivilegedData:      enrichment.IsPrivilegedData,
		DataCategory:          string(enrichment.DataCategory),
		SubCategory:           enrichment.SubCategory,
		PrivilegedConfidence:  enrichment.PrivilegedConfidence,
		PrivilegedDescription: enrichment.PrivilegedDescription,
		RiskLevel:             string(enrichment.RiskLevel),
		ValuePatterns:         enrichment.ValuePatterns,
		SampleValues:          enrichment.SampleValues,
		IsSearchable:          enrichment.IsSearchable,
		IsFilterable:          enrichment.IsFilterable,
		IsSortable:            enrichment.IsSortable,
		RecommendedIndexType:  string(enrichment.RecommendedIndexType),
		ShouldEncrypt:         enrichment.ShouldEncrypt,
		ShouldMask:            enrichment.ShouldMask,
		IsForeignKey:          enrichment.IsForeignKey,
		ReferencedTable:       enrichment.ReferencedTable,
		ReferencedColumn:      enrichment.ReferencedColumn,
		ReferencingTables:     enrichment.ReferencingTables,
		BusinessMeaning:       enrichment.BusinessMeaning,
		Tags:                  enrichment.Tags,
		Context:               enrichment.Context,
	}

	// Convert compliance impact
	for _, framework := range enrichment.ComplianceImpact {
		protoEnrichment.ComplianceImpact = append(protoEnrichment.ComplianceImpact, string(framework))
	}

	// Handle optional fields
	if enrichment.Cardinality != nil {
		protoEnrichment.Cardinality = *enrichment.Cardinality
	}

	if enrichment.NullPercentage != nil {
		protoEnrichment.NullPercentage = *enrichment.NullPercentage
	}

	if enrichment.DataDistribution != nil {
		protoEnrichment.DataDistribution = *enrichment.DataDistribution
	}

	if enrichment.QueryFrequency != nil {
		protoEnrichment.QueryFrequency = *enrichment.QueryFrequency
	}

	if enrichment.DataQualityScore != nil {
		protoEnrichment.DataQualityScore = *enrichment.DataQualityScore
	}

	if enrichment.CompletenessScore != nil {
		protoEnrichment.CompletenessScore = *enrichment.CompletenessScore
	}

	if enrichment.ConsistencyScore != nil {
		protoEnrichment.ConsistencyScore = *enrichment.ConsistencyScore
	}

	return protoEnrichment
}

func (s *Server) convertProtoToColumnEnrichment(protoEnrichment *pb.ColumnEnrichment) unifiedmodel.ColumnEnrichment {
	enrichment := unifiedmodel.ColumnEnrichment{
		IsPrivilegedData:      protoEnrichment.IsPrivilegedData,
		DataCategory:          unifiedmodel.DataCategory(protoEnrichment.DataCategory),
		SubCategory:           protoEnrichment.SubCategory,
		PrivilegedConfidence:  protoEnrichment.PrivilegedConfidence,
		PrivilegedDescription: protoEnrichment.PrivilegedDescription,
		RiskLevel:             unifiedmodel.RiskLevel(protoEnrichment.RiskLevel),
		ValuePatterns:         protoEnrichment.ValuePatterns,
		SampleValues:          protoEnrichment.SampleValues,
		IsSearchable:          protoEnrichment.IsSearchable,
		IsFilterable:          protoEnrichment.IsFilterable,
		IsSortable:            protoEnrichment.IsSortable,
		RecommendedIndexType:  unifiedmodel.IndexType(protoEnrichment.RecommendedIndexType),
		ShouldEncrypt:         protoEnrichment.ShouldEncrypt,
		ShouldMask:            protoEnrichment.ShouldMask,
		IsForeignKey:          protoEnrichment.IsForeignKey,
		ReferencedTable:       protoEnrichment.ReferencedTable,
		ReferencedColumn:      protoEnrichment.ReferencedColumn,
		ReferencingTables:     protoEnrichment.ReferencingTables,
		BusinessMeaning:       protoEnrichment.BusinessMeaning,
		Tags:                  protoEnrichment.Tags,
		Context:               protoEnrichment.Context,
	}

	// Convert compliance impact
	for _, framework := range protoEnrichment.ComplianceImpact {
		enrichment.ComplianceImpact = append(enrichment.ComplianceImpact, unifiedmodel.ComplianceFramework(framework))
	}

	// Handle optional fields
	if protoEnrichment.Cardinality > 0 {
		enrichment.Cardinality = &protoEnrichment.Cardinality
	}

	if protoEnrichment.NullPercentage > 0 {
		enrichment.NullPercentage = &protoEnrichment.NullPercentage
	}

	if protoEnrichment.DataDistribution != "" {
		enrichment.DataDistribution = &protoEnrichment.DataDistribution
	}

	if protoEnrichment.QueryFrequency > 0 {
		enrichment.QueryFrequency = &protoEnrichment.QueryFrequency
	}

	if protoEnrichment.DataQualityScore > 0 {
		enrichment.DataQualityScore = &protoEnrichment.DataQualityScore
	}

	if protoEnrichment.CompletenessScore > 0 {
		enrichment.CompletenessScore = &protoEnrichment.CompletenessScore
	}

	if protoEnrichment.ConsistencyScore > 0 {
		enrichment.ConsistencyScore = &protoEnrichment.ConsistencyScore
	}

	return enrichment
}

// convertUnifiedModelToProto converts a Go UnifiedModel to protobuf UnifiedModel
func (s *Server) convertUnifiedModelToProto(model *unifiedmodel.UnifiedModel) *pb.UnifiedModel {
	if model == nil {
		return nil
	}

	protoModel := &pb.UnifiedModel{
		DatabaseType:   string(model.DatabaseType),
		CacheMechanism: model.CacheMechanism,

		// Initialize all maps
		Catalogs:              make(map[string]*pb.Catalog),
		Databases:             make(map[string]*pb.Database),
		Schemas:               make(map[string]*pb.Schema),
		Tables:                make(map[string]*pb.Table),
		Collections:           make(map[string]*pb.Collection),
		Nodes:                 make(map[string]*pb.Node),
		MemoryTables:          make(map[string]*pb.MemoryTable),
		TemporaryTables:       make(map[string]*pb.TemporaryTable),
		TransientTables:       make(map[string]*pb.TransientTable),
		Caches:                make(map[string]*pb.Cache),
		Views:                 make(map[string]*pb.View),
		LiveViews:             make(map[string]*pb.LiveView),
		WindowViews:           make(map[string]*pb.WindowView),
		MaterializedViews:     make(map[string]*pb.MaterializedView),
		ExternalTables:        make(map[string]*pb.ExternalTable),
		ForeignTables:         make(map[string]*pb.ForeignTable),
		Graphs:                make(map[string]*pb.Graph),
		VectorIndexes:         make(map[string]*pb.VectorIndex),
		SearchIndexes:         make(map[string]*pb.SearchIndex),
		Vectors:               make(map[string]*pb.Vector),
		Embeddings:            make(map[string]*pb.Embedding),
		Documents:             make(map[string]*pb.Document),
		EmbeddedDocuments:     make(map[string]*pb.EmbeddedDocument),
		Relationships:         make(map[string]*pb.Relationship),
		Paths:                 make(map[string]*pb.Path),
		Partitions:            make(map[string]*pb.Partition),
		SubPartitions:         make(map[string]*pb.SubPartition),
		Shards:                make(map[string]*pb.Shard),
		Keyspaces:             make(map[string]*pb.Keyspace),
		Namespaces:            make(map[string]*pb.Namespace),
		Columns:               make(map[string]*pb.Column),
		Types:                 make(map[string]*pb.Type),
		PropertyKeys:          make(map[string]*pb.PropertyKey),
		Indexes:               make(map[string]*pb.Index),
		Constraints:           make(map[string]*pb.Constraint),
		Sequences:             make(map[string]*pb.Sequence),
		Identities:            make(map[string]*pb.Identity),
		UuidGenerators:        make(map[string]*pb.UUIDGenerator),
		Functions:             make(map[string]*pb.Function),
		Procedures:            make(map[string]*pb.Procedure),
		Methods:               make(map[string]*pb.Method),
		Triggers:              make(map[string]*pb.Trigger),
		EventTriggers:         make(map[string]*pb.EventTrigger),
		Aggregates:            make(map[string]*pb.Aggregate),
		Operators:             make(map[string]*pb.Operator),
		Modules:               make(map[string]*pb.Module),
		Packages:              make(map[string]*pb.Package),
		PackageBodies:         make(map[string]*pb.PackageBody),
		Macros:                make(map[string]*pb.Macro),
		Rules:                 make(map[string]*pb.Rule),
		WindowFunctions:       make(map[string]*pb.WindowFunc),
		Users:                 make(map[string]*pb.DBUser),
		Roles:                 make(map[string]*pb.DBRole),
		Grants:                make(map[string]*pb.Grant),
		Policies:              make(map[string]*pb.Policy),
		Tablespaces:           make(map[string]*pb.Tablespace),
		Segments:              make(map[string]*pb.Segment),
		Extents:               make(map[string]*pb.Extent),
		Pages:                 make(map[string]*pb.Page),
		Filegroups:            make(map[string]*pb.Filegroup),
		Datafiles:             make(map[string]*pb.Datafile),
		Extensions:            make(map[string]*pb.Extension),
		Plugins:               make(map[string]*pb.Plugin),
		ModuleExtensions:      make(map[string]*pb.ModuleExtension),
		TtlSettings:           make(map[string]*pb.TTLSetting),
		Dimensions:            make(map[string]*pb.DimensionSpec),
		DistanceMetrics:       make(map[string]*pb.DistanceMetricSpec),
		Projections:           make(map[string]*pb.Projection),
		AnalyticsAggregations: make(map[string]*pb.AggregationOp),
		Transformations:       make(map[string]*pb.TransformationStep),
		Enrichments:           make(map[string]*pb.Enrichment),
		BufferPools:           make(map[string]*pb.BufferPool),
		Publications:          make(map[string]*pb.Publication),
		Subscriptions:         make(map[string]*pb.Subscription),
		ReplicationSlots:      make(map[string]*pb.ReplicationSlot),
		FailoverGroups:        make(map[string]*pb.FailoverGroup),
	}

	// Convert core objects (most commonly used)
	for name, table := range model.Tables {
		protoModel.Tables[name] = s.convertTableToProto(table)
	}

	for name, collection := range model.Collections {
		protoModel.Collections[name] = s.convertCollectionToProto(collection)
	}

	for name, view := range model.Views {
		protoModel.Views[name] = s.convertViewToProto(view)
	}

	for name, matView := range model.MaterializedViews {
		protoModel.MaterializedViews[name] = s.convertMaterializedViewToProto(matView)
	}

	for name, index := range model.Indexes {
		protoModel.Indexes[name] = s.convertIndexToProto(index)
	}

	for name, constraint := range model.Constraints {
		protoModel.Constraints[name] = s.convertConstraintToProto(constraint)
	}

	for name, sequence := range model.Sequences {
		protoModel.Sequences[name] = s.convertSequenceToProto(sequence)
	}

	for name, function := range model.Functions {
		protoModel.Functions[name] = s.convertFunctionToProto(function)
	}

	for name, procedure := range model.Procedures {
		protoModel.Procedures[name] = s.convertProcedureToProto(procedure)
	}

	for name, trigger := range model.Triggers {
		protoModel.Triggers[name] = s.convertTriggerToProto(trigger)
	}

	for name, typ := range model.Types {
		protoModel.Types[name] = s.convertTypeToProto(typ)
	}

	// TODO: Add conversions for remaining types as needed
	// The framework is in place for all types, but conversions can be added incrementally
	// based on usage requirements

	return protoModel
}

// convertProtoToUnifiedModel converts a protobuf UnifiedModel to Go UnifiedModel
func (s *Server) convertProtoToUnifiedModel(protoModel *pb.UnifiedModel) *unifiedmodel.UnifiedModel {
	if protoModel == nil {
		return nil
	}

	model := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.DatabaseType(protoModel.DatabaseType),

		// Structural organization
		Catalogs:  make(map[string]unifiedmodel.Catalog),
		Databases: make(map[string]unifiedmodel.Database),
		Schemas:   make(map[string]unifiedmodel.Schema),

		// Primary Data Containers
		Tables:       make(map[string]unifiedmodel.Table),
		Collections:  make(map[string]unifiedmodel.Collection),
		Nodes:        make(map[string]unifiedmodel.Node),
		MemoryTables: make(map[string]unifiedmodel.MemoryTable),

		// Temporary Data Containers
		TemporaryTables: make(map[string]unifiedmodel.TemporaryTable),
		TransientTables: make(map[string]unifiedmodel.TransientTable),
		CacheMechanism:  protoModel.CacheMechanism,
		Caches:          make(map[string]unifiedmodel.Cache),

		// Virtual Data Containers
		Views:             make(map[string]unifiedmodel.View),
		LiveViews:         make(map[string]unifiedmodel.LiveView),
		WindowViews:       make(map[string]unifiedmodel.WindowView),
		MaterializedViews: make(map[string]unifiedmodel.MaterializedView),
		ExternalTables:    make(map[string]unifiedmodel.ExternalTable),
		ForeignTables:     make(map[string]unifiedmodel.ForeignTable),

		// Graph / Vector / Search abstractions
		Graphs:        make(map[string]unifiedmodel.Graph),
		VectorIndexes: make(map[string]unifiedmodel.VectorIndex),
		SearchIndexes: make(map[string]unifiedmodel.SearchIndex),

		// Specialized Data Containers
		Vectors:           make(map[string]unifiedmodel.Vector),
		Embeddings:        make(map[string]unifiedmodel.Embedding),
		Documents:         make(map[string]unifiedmodel.Document),
		EmbeddedDocuments: make(map[string]unifiedmodel.EmbeddedDocument),
		Relationships:     make(map[string]unifiedmodel.Relationship),
		Paths:             make(map[string]unifiedmodel.Path),

		// Data Organization Containers
		Partitions:    make(map[string]unifiedmodel.Partition),
		SubPartitions: make(map[string]unifiedmodel.SubPartition),
		Shards:        make(map[string]unifiedmodel.Shard),
		Keyspaces:     make(map[string]unifiedmodel.Keyspace),
		Namespaces:    make(map[string]unifiedmodel.Namespace),

		// Structural definition objects
		Columns:      make(map[string]unifiedmodel.Column),
		Types:        make(map[string]unifiedmodel.Type),
		PropertyKeys: make(map[string]unifiedmodel.PropertyKey),

		// Integrity, performance and identity objects
		Indexes:        make(map[string]unifiedmodel.Index),
		Constraints:    make(map[string]unifiedmodel.Constraint),
		Sequences:      make(map[string]unifiedmodel.Sequence),
		Identities:     make(map[string]unifiedmodel.Identity),
		UUIDGenerators: make(map[string]unifiedmodel.UUIDGenerator),

		// Executable code objects
		Functions:     make(map[string]unifiedmodel.Function),
		Procedures:    make(map[string]unifiedmodel.Procedure),
		Methods:       make(map[string]unifiedmodel.Method),
		Triggers:      make(map[string]unifiedmodel.Trigger),
		EventTriggers: make(map[string]unifiedmodel.EventTrigger),
		Aggregates:    make(map[string]unifiedmodel.Aggregate),
		Operators:     make(map[string]unifiedmodel.Operator),
		Modules:       make(map[string]unifiedmodel.Module),
		Packages:      make(map[string]unifiedmodel.Package),
		PackageBodies: make(map[string]unifiedmodel.PackageBody),
		Macros:        make(map[string]unifiedmodel.Macro),
		Rules:         make(map[string]unifiedmodel.Rule),
		WindowFuncs:   make(map[string]unifiedmodel.WindowFunc),

		// Security and access control
		Users:    make(map[string]unifiedmodel.DBUser),
		Roles:    make(map[string]unifiedmodel.DBRole),
		Grants:   make(map[string]unifiedmodel.Grant),
		Policies: make(map[string]unifiedmodel.Policy),

		// Physical storage and placement
		Tablespaces: make(map[string]unifiedmodel.Tablespace),
		Segments:    make(map[string]unifiedmodel.Segment),
		Extents:     make(map[string]unifiedmodel.Extent),
		Pages:       make(map[string]unifiedmodel.Page),
		Filegroups:  make(map[string]unifiedmodel.Filegroup),
		Datafiles:   make(map[string]unifiedmodel.Datafile),

		// Extensions and customization
		Extensions:       make(map[string]unifiedmodel.Extension),
		Plugins:          make(map[string]unifiedmodel.Plugin),
		ModuleExtensions: make(map[string]unifiedmodel.ModuleExtension),
		TTLSettings:      make(map[string]unifiedmodel.TTLSetting),
		Dimensions:       make(map[string]unifiedmodel.DimensionSpec),
		DistanceMetrics:  make(map[string]unifiedmodel.DistanceMetricSpec),

		// Advanced analytics
		Projections:     make(map[string]unifiedmodel.Projection),
		AnalyticsAggs:   make(map[string]unifiedmodel.AggregationOp),
		Transformations: make(map[string]unifiedmodel.TransformationStep),
		Enrichments:     make(map[string]unifiedmodel.Enrichment),
		BufferPools:     make(map[string]unifiedmodel.BufferPool),

		// Replication & distribution
		Publications:     make(map[string]unifiedmodel.Publication),
		Subscriptions:    make(map[string]unifiedmodel.Subscription),
		ReplicationSlots: make(map[string]unifiedmodel.ReplicationSlot),
		FailoverGroups:   make(map[string]unifiedmodel.FailoverGroup),
	}

	// Convert core objects (most commonly used)
	for name, table := range protoModel.Tables {
		model.Tables[name] = s.convertProtoToTable(table)
	}

	for name, collection := range protoModel.Collections {
		model.Collections[name] = s.convertProtoToCollection(collection)
	}

	for name, view := range protoModel.Views {
		model.Views[name] = s.convertProtoToView(view)
	}

	for name, matView := range protoModel.MaterializedViews {
		model.MaterializedViews[name] = s.convertProtoToMaterializedView(matView)
	}

	for name, index := range protoModel.Indexes {
		model.Indexes[name] = s.convertProtoToIndex(index)
	}

	for name, constraint := range protoModel.Constraints {
		model.Constraints[name] = s.convertProtoToConstraint(constraint)
	}

	for name, sequence := range protoModel.Sequences {
		model.Sequences[name] = s.convertProtoToSequence(sequence)
	}

	for name, function := range protoModel.Functions {
		model.Functions[name] = s.convertProtoToFunction(function)
	}

	for name, procedure := range protoModel.Procedures {
		model.Procedures[name] = s.convertProtoToProcedure(procedure)
	}

	for name, trigger := range protoModel.Triggers {
		model.Triggers[name] = s.convertProtoToTrigger(trigger)
	}

	for name, typ := range protoModel.Types {
		model.Types[name] = s.convertProtoToType(typ)
	}

	// TODO: Add conversions for remaining types as needed
	// The framework is in place for all types, but conversions can be added incrementally
	// based on usage requirements

	return model
}

func (s *Server) convertTableToProto(table unifiedmodel.Table) *pb.Table {
	protoTable := &pb.Table{
		Name:    table.Name,
		Owner:   table.Owner,
		Comment: table.Comment,
		Labels:  table.Labels,
		Columns: make(map[string]*pb.Column),
	}

	for name, column := range table.Columns {
		protoTable.Columns[name] = s.convertColumnToProto(column)
	}

	return protoTable
}

func (s *Server) convertProtoToTable(protoTable *pb.Table) unifiedmodel.Table {
	table := unifiedmodel.Table{
		Name:    protoTable.Name,
		Owner:   protoTable.Owner,
		Comment: protoTable.Comment,
		Labels:  protoTable.Labels,
		Columns: make(map[string]unifiedmodel.Column),
	}

	for name, column := range protoTable.Columns {
		table.Columns[name] = s.convertProtoToColumn(column)
	}

	return table
}

func (s *Server) convertColumnToProto(column unifiedmodel.Column) *pb.Column {
	return &pb.Column{
		Name:                column.Name,
		DataType:            column.DataType,
		Nullable:            column.Nullable,
		DefaultValue:        column.Default,
		GeneratedExpression: column.GeneratedExpression,
		IsPrimaryKey:        column.IsPrimaryKey,
		IsPartitionKey:      column.IsPartitionKey,
		IsClusteringKey:     column.IsClusteringKey,
		AutoIncrement:       column.AutoIncrement,
		Collation:           column.Collation,
	}
}

func (s *Server) convertProtoToColumn(protoColumn *pb.Column) unifiedmodel.Column {
	return unifiedmodel.Column{
		Name:                protoColumn.Name,
		DataType:            protoColumn.DataType,
		Nullable:            protoColumn.Nullable,
		Default:             protoColumn.DefaultValue,
		GeneratedExpression: protoColumn.GeneratedExpression,
		IsPrimaryKey:        protoColumn.IsPrimaryKey,
		IsPartitionKey:      protoColumn.IsPartitionKey,
		IsClusteringKey:     protoColumn.IsClusteringKey,
		AutoIncrement:       protoColumn.AutoIncrement,
		Collation:           protoColumn.Collation,
	}
}

// Additional conversion functions for new types

func (s *Server) convertCollectionToProto(collection unifiedmodel.Collection) *pb.Collection {
	protoCollection := &pb.Collection{
		Name:     collection.Name,
		Owner:    collection.Owner,
		Comment:  collection.Comment,
		Labels:   collection.Labels,
		Fields:   make(map[string]*pb.Field),
		Indexes:  make(map[string]*pb.Index),
		ShardKey: collection.ShardKey,
	}

	for name, field := range collection.Fields {
		protoCollection.Fields[name] = s.convertFieldToProto(field)
	}

	for name, index := range collection.Indexes {
		protoCollection.Indexes[name] = s.convertIndexToProto(index)
	}

	return protoCollection
}

func (s *Server) convertProtoToCollection(protoCollection *pb.Collection) unifiedmodel.Collection {
	collection := unifiedmodel.Collection{
		Name:     protoCollection.Name,
		Owner:    protoCollection.Owner,
		Comment:  protoCollection.Comment,
		Labels:   protoCollection.Labels,
		Fields:   make(map[string]unifiedmodel.Field),
		Indexes:  make(map[string]unifiedmodel.Index),
		ShardKey: protoCollection.ShardKey,
	}

	for name, field := range protoCollection.Fields {
		collection.Fields[name] = s.convertProtoToField(field)
	}

	for name, index := range protoCollection.Indexes {
		collection.Indexes[name] = s.convertProtoToIndex(index)
	}

	return collection
}

func (s *Server) convertFieldToProto(field unifiedmodel.Field) *pb.Field {
	return &pb.Field{
		Name:     field.Name,
		Type:     field.Type,
		Required: field.Required,
	}
}

func (s *Server) convertProtoToField(protoField *pb.Field) unifiedmodel.Field {
	return unifiedmodel.Field{
		Name:     protoField.Name,
		Type:     protoField.Type,
		Required: protoField.Required,
	}
}

func (s *Server) convertViewToProto(view unifiedmodel.View) *pb.View {
	protoView := &pb.View{
		Name:       view.Name,
		Definition: view.Definition,
		Comment:    view.Comment,
		Columns:    make(map[string]*pb.Column),
	}

	for name, column := range view.Columns {
		protoView.Columns[name] = s.convertColumnToProto(column)
	}

	return protoView
}

func (s *Server) convertProtoToView(protoView *pb.View) unifiedmodel.View {
	view := unifiedmodel.View{
		Name:       protoView.Name,
		Definition: protoView.Definition,
		Comment:    protoView.Comment,
		Columns:    make(map[string]unifiedmodel.Column),
	}

	for name, column := range protoView.Columns {
		view.Columns[name] = s.convertProtoToColumn(column)
	}

	return view
}

func (s *Server) convertMaterializedViewToProto(matView unifiedmodel.MaterializedView) *pb.MaterializedView {
	protoMatView := &pb.MaterializedView{
		Name:        matView.Name,
		Definition:  matView.Definition,
		RefreshMode: matView.RefreshMode,
		RefreshCron: matView.RefreshCron,
		Columns:     make(map[string]*pb.Column),
	}

	for name, column := range matView.Columns {
		protoMatView.Columns[name] = s.convertColumnToProto(column)
	}

	return protoMatView
}

func (s *Server) convertProtoToMaterializedView(protoMatView *pb.MaterializedView) unifiedmodel.MaterializedView {
	matView := unifiedmodel.MaterializedView{
		Name:        protoMatView.Name,
		Definition:  protoMatView.Definition,
		RefreshMode: protoMatView.RefreshMode,
		RefreshCron: protoMatView.RefreshCron,
		Columns:     make(map[string]unifiedmodel.Column),
	}

	for name, column := range protoMatView.Columns {
		matView.Columns[name] = s.convertProtoToColumn(column)
	}

	return matView
}

func (s *Server) convertIndexToProto(index unifiedmodel.Index) *pb.Index {
	return &pb.Index{
		Name:       index.Name,
		Type:       string(index.Type),
		Columns:    index.Columns,
		Fields:     index.Fields,
		Expression: index.Expression,
		Predicate:  index.Predicate,
		Unique:     index.Unique,
	}
}

func (s *Server) convertProtoToIndex(protoIndex *pb.Index) unifiedmodel.Index {
	return unifiedmodel.Index{
		Name:       protoIndex.Name,
		Type:       unifiedmodel.IndexType(protoIndex.Type),
		Columns:    protoIndex.Columns,
		Fields:     protoIndex.Fields,
		Expression: protoIndex.Expression,
		Predicate:  protoIndex.Predicate,
		Unique:     protoIndex.Unique,
	}
}

func (s *Server) convertConstraintToProto(constraint unifiedmodel.Constraint) *pb.Constraint {
	return &pb.Constraint{
		Name:       constraint.Name,
		Type:       string(constraint.Type),
		Columns:    constraint.Columns,
		Expression: constraint.Expression,
		Reference:  s.convertReferenceToProto(constraint.Reference),
	}
}

func (s *Server) convertProtoToConstraint(protoConstraint *pb.Constraint) unifiedmodel.Constraint {
	return unifiedmodel.Constraint{
		Name:       protoConstraint.Name,
		Type:       unifiedmodel.ConstraintType(protoConstraint.Type),
		Columns:    protoConstraint.Columns,
		Expression: protoConstraint.Expression,
		Reference:  s.convertProtoToReference(protoConstraint.Reference),
	}
}

func (s *Server) convertReferenceToProto(reference unifiedmodel.Reference) *pb.Reference {
	return &pb.Reference{
		Table:    reference.Table,
		Columns:  reference.Columns,
		OnUpdate: reference.OnUpdate,
		OnDelete: reference.OnDelete,
	}
}

func (s *Server) convertProtoToReference(protoReference *pb.Reference) unifiedmodel.Reference {
	if protoReference == nil {
		return unifiedmodel.Reference{}
	}
	return unifiedmodel.Reference{
		Table:    protoReference.Table,
		Columns:  protoReference.Columns,
		OnUpdate: protoReference.OnUpdate,
		OnDelete: protoReference.OnDelete,
	}
}

func (s *Server) convertSequenceToProto(sequence unifiedmodel.Sequence) *pb.Sequence {
	protoSeq := &pb.Sequence{
		Name:      sequence.Name,
		Start:     sequence.Start,
		Increment: sequence.Increment,
		Cycle:     sequence.Cycle,
	}

	if sequence.Min != nil {
		protoSeq.MinValue = *sequence.Min
	}
	if sequence.Max != nil {
		protoSeq.MaxValue = *sequence.Max
	}
	if sequence.Cache != nil {
		protoSeq.Cache = *sequence.Cache
	}

	return protoSeq
}

func (s *Server) convertProtoToSequence(protoSequence *pb.Sequence) unifiedmodel.Sequence {
	seq := unifiedmodel.Sequence{
		Name:      protoSequence.Name,
		Start:     protoSequence.Start,
		Increment: protoSequence.Increment,
		Cycle:     protoSequence.Cycle,
	}

	if protoSequence.MinValue != 0 {
		seq.Min = &protoSequence.MinValue
	}
	if protoSequence.MaxValue != 0 {
		seq.Max = &protoSequence.MaxValue
	}
	if protoSequence.Cache != 0 {
		seq.Cache = &protoSequence.Cache
	}

	return seq
}

func (s *Server) convertFunctionToProto(function unifiedmodel.Function) *pb.Function {
	protoFunction := &pb.Function{
		Name:       function.Name,
		Language:   function.Language,
		Returns:    function.Returns,
		Definition: function.Definition,
		Arguments:  make([]*pb.Argument, len(function.Arguments)),
	}

	for i, arg := range function.Arguments {
		protoFunction.Arguments[i] = &pb.Argument{
			Name: arg.Name,
			Type: arg.Type,
		}
	}

	return protoFunction
}

func (s *Server) convertProtoToFunction(protoFunction *pb.Function) unifiedmodel.Function {
	function := unifiedmodel.Function{
		Name:       protoFunction.Name,
		Language:   protoFunction.Language,
		Returns:    protoFunction.Returns,
		Definition: protoFunction.Definition,
		Arguments:  make([]unifiedmodel.Argument, len(protoFunction.Arguments)),
	}

	for i, arg := range protoFunction.Arguments {
		function.Arguments[i] = unifiedmodel.Argument{
			Name: arg.Name,
			Type: arg.Type,
		}
	}

	return function
}

func (s *Server) convertProcedureToProto(procedure unifiedmodel.Procedure) *pb.Procedure {
	protoProcedure := &pb.Procedure{
		Name:       procedure.Name,
		Language:   procedure.Language,
		Definition: procedure.Definition,
		Arguments:  make([]*pb.Argument, len(procedure.Arguments)),
	}

	for i, arg := range procedure.Arguments {
		protoProcedure.Arguments[i] = &pb.Argument{
			Name: arg.Name,
			Type: arg.Type,
		}
	}

	return protoProcedure
}

func (s *Server) convertProtoToProcedure(protoProcedure *pb.Procedure) unifiedmodel.Procedure {
	procedure := unifiedmodel.Procedure{
		Name:       protoProcedure.Name,
		Language:   protoProcedure.Language,
		Definition: protoProcedure.Definition,
		Arguments:  make([]unifiedmodel.Argument, len(protoProcedure.Arguments)),
	}

	for i, arg := range protoProcedure.Arguments {
		procedure.Arguments[i] = unifiedmodel.Argument{
			Name: arg.Name,
			Type: arg.Type,
		}
	}

	return procedure
}

func (s *Server) convertTriggerToProto(trigger unifiedmodel.Trigger) *pb.Trigger {
	return &pb.Trigger{
		Name:      trigger.Name,
		Table:     trigger.Table,
		Timing:    trigger.Timing,
		Events:    trigger.Events,
		Procedure: trigger.Procedure,
	}
}

func (s *Server) convertProtoToTrigger(protoTrigger *pb.Trigger) unifiedmodel.Trigger {
	return unifiedmodel.Trigger{
		Name:      protoTrigger.Name,
		Table:     protoTrigger.Table,
		Timing:    protoTrigger.Timing,
		Events:    protoTrigger.Events,
		Procedure: protoTrigger.Procedure,
	}
}

func (s *Server) convertTypeToProto(typ unifiedmodel.Type) *pb.Type {
	return &pb.Type{
		Name:     typ.Name,
		Category: typ.Category,
	}
}

func (s *Server) convertProtoToType(protoType *pb.Type) unifiedmodel.Type {
	return unifiedmodel.Type{
		Name:     protoType.Name,
		Category: protoType.Category,
	}
}
