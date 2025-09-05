// internal/generators/interface.go
package generators

import (
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

type StatementGenerator interface {
	// Structural organization
	GenerateCreateCatalogSQL(catalog unifiedmodel.Catalog) (string, error)
	GenerateCreateDatabaseSQL(database unifiedmodel.Database) (string, error)
	GenerateCreateSchemaSQL(schema unifiedmodel.Schema) (string, error)

	// Primary Data Containers
	GenerateCreateTableSQL(table unifiedmodel.Table) (string, error)
	GenerateCreateCollectionSQL(collection unifiedmodel.Collection) (string, error)
	GenerateCreateNodeSQL(node unifiedmodel.Node) (string, error)
	GenerateCreateMemoryTableSQL(memTable unifiedmodel.MemoryTable) (string, error)

	// Temporary Data Containers
	GenerateCreateTemporaryTableSQL(tempTable unifiedmodel.TemporaryTable) (string, error)
	GenerateCreateTransientTableSQL(transTable unifiedmodel.TransientTable) (string, error)
	GenerateCreateCacheSQL(cache unifiedmodel.Cache) (string, error)

	// Virtual Data Containers
	GenerateCreateViewSQL(view unifiedmodel.View) (string, error)
	GenerateCreateLiveViewSQL(liveView unifiedmodel.LiveView) (string, error)
	GenerateCreateWindowViewSQL(windowView unifiedmodel.WindowView) (string, error)
	GenerateCreateMaterializedViewSQL(matView unifiedmodel.MaterializedView) (string, error)
	GenerateCreateExternalTableSQL(extTable unifiedmodel.ExternalTable) (string, error)
	GenerateCreateForeignTableSQL(foreignTable unifiedmodel.ForeignTable) (string, error)

	// Graph / Vector / Search abstractions
	GenerateCreateGraphSQL(graph unifiedmodel.Graph) (string, error)
	GenerateCreateVectorIndexSQL(vectorIndex unifiedmodel.VectorIndex) (string, error)
	GenerateCreateSearchIndexSQL(searchIndex unifiedmodel.SearchIndex) (string, error)

	// Specialized Data Containers
	GenerateCreateVectorSQL(vector unifiedmodel.Vector) (string, error)
	GenerateCreateEmbeddingSQL(embedding unifiedmodel.Embedding) (string, error)
	GenerateCreateDocumentSQL(document unifiedmodel.Document) (string, error)
	GenerateCreateEmbeddedDocumentSQL(embeddedDoc unifiedmodel.EmbeddedDocument) (string, error)
	GenerateCreateRelationshipSQL(relationship unifiedmodel.Relationship) (string, error)
	GenerateCreatePathSQL(path unifiedmodel.Path) (string, error)

	// Data Organization Containers
	GenerateCreatePartitionSQL(partition unifiedmodel.Partition) (string, error)
	GenerateCreateSubPartitionSQL(subPartition unifiedmodel.SubPartition) (string, error)
	GenerateCreateShardSQL(shard unifiedmodel.Shard) (string, error)
	GenerateCreateKeyspaceSQL(keyspace unifiedmodel.Keyspace) (string, error)
	GenerateCreateNamespaceSQL(namespace unifiedmodel.Namespace) (string, error)

	// Structural definition objects
	GenerateCreateTypeSQL(dataType unifiedmodel.Type) (string, error)
	GenerateCreatePropertyKeySQL(propertyKey unifiedmodel.PropertyKey) (string, error)

	// Integrity, performance and identity objects
	GenerateCreateIndexSQL(index unifiedmodel.Index) (string, error)
	GenerateCreateConstraintSQL(constraint unifiedmodel.Constraint) (string, error)
	GenerateCreateSequenceSQL(seq unifiedmodel.Sequence) (string, error)
	GenerateCreateIdentitySQL(identity unifiedmodel.Identity) (string, error)
	GenerateCreateUUIDGeneratorSQL(uuidGen unifiedmodel.UUIDGenerator) (string, error)

	// Executable code objects
	GenerateCreateFunctionSQL(fn unifiedmodel.Function) (string, error)
	GenerateCreateProcedureSQL(procedure unifiedmodel.Procedure) (string, error)
	GenerateCreateMethodSQL(method unifiedmodel.Method) (string, error)
	GenerateCreateTriggerSQL(trigger unifiedmodel.Trigger) (string, error)
	GenerateCreateEventTriggerSQL(eventTrigger unifiedmodel.EventTrigger) (string, error)
	GenerateCreateAggregateSQL(aggregate unifiedmodel.Aggregate) (string, error)
	GenerateCreateOperatorSQL(operator unifiedmodel.Operator) (string, error)
	GenerateCreateModuleSQL(module unifiedmodel.Module) (string, error)
	GenerateCreatePackageSQL(pkg unifiedmodel.Package) (string, error)
	GenerateCreatePackageBodySQL(pkgBody unifiedmodel.PackageBody) (string, error)
	GenerateCreateMacroSQL(macro unifiedmodel.Macro) (string, error)
	GenerateCreateRuleSQL(rule unifiedmodel.Rule) (string, error)
	GenerateCreateWindowFuncSQL(windowFunc unifiedmodel.WindowFunc) (string, error)

	// Security and access control
	GenerateCreateUserSQL(user unifiedmodel.DBUser) (string, error)
	GenerateCreateRoleSQL(role unifiedmodel.DBRole) (string, error)
	GenerateCreateGrantSQL(grant unifiedmodel.Grant) (string, error)
	GenerateCreatePolicySQL(policy unifiedmodel.Policy) (string, error)

	// Physical storage and placement
	GenerateCreateTablespaceSQL(tablespace unifiedmodel.Tablespace) (string, error)
	GenerateCreateSegmentSQL(segment unifiedmodel.Segment) (string, error)
	GenerateCreateExtentSQL(extent unifiedmodel.Extent) (string, error)
	GenerateCreatePageSQL(page unifiedmodel.Page) (string, error)
	GenerateCreateFilegroupSQL(filegroup unifiedmodel.Filegroup) (string, error)
	GenerateCreateDatafileSQL(datafile unifiedmodel.Datafile) (string, error)

	// Connectivity and integration
	GenerateCreateServerSQL(server unifiedmodel.Server) (string, error)
	GenerateCreateConnectionSQL(connection unifiedmodel.Connection) (string, error)
	GenerateCreateEndpointSQL(endpoint unifiedmodel.Endpoint) (string, error)
	GenerateCreateForeignDataWrapperSQL(fdw unifiedmodel.ForeignDataWrapper) (string, error)
	GenerateCreateUserMappingSQL(userMapping unifiedmodel.UserMapping) (string, error)
	GenerateCreateFederationSQL(federation unifiedmodel.Federation) (string, error)
	GenerateCreateReplicaSQL(replica unifiedmodel.Replica) (string, error)
	GenerateCreateClusterSQL(cluster unifiedmodel.Cluster) (string, error)

	// Operational, pipelines and streaming
	GenerateCreateTaskSQL(task unifiedmodel.Task) (string, error)
	GenerateCreateJobSQL(job unifiedmodel.Job) (string, error)
	GenerateCreateScheduleSQL(schedule unifiedmodel.Schedule) (string, error)
	GenerateCreatePipelineSQL(pipeline unifiedmodel.Pipeline) (string, error)
	GenerateCreateStreamSQL(stream unifiedmodel.Stream) (string, error)

	// Monitoring and alerting
	GenerateCreateEventSQL(event unifiedmodel.Event) (string, error)
	GenerateCreateNotificationSQL(notification unifiedmodel.Notification) (string, error)
	GenerateCreateAlertSQL(alert unifiedmodel.Alert) (string, error)
	GenerateCreateStatisticSQL(statistic unifiedmodel.Statistic) (string, error)
	GenerateCreateHistogramSQL(histogram unifiedmodel.Histogram) (string, error)
	GenerateCreateMonitorSQL(monitor unifiedmodel.Monitor) (string, error)
	GenerateCreateMonitorMetricSQL(monitorMetric unifiedmodel.MonitorMetric) (string, error)
	GenerateCreateThresholdSQL(threshold unifiedmodel.Threshold) (string, error)

	// Text processing / search configuration
	GenerateCreateTextSearchComponentSQL(textSearchComponent unifiedmodel.TextSearchComponent) (string, error)

	// Metadata and documentation
	GenerateCreateCommentSQL(comment unifiedmodel.Comment) (string, error)
	GenerateCreateAnnotationSQL(annotation unifiedmodel.Annotation) (string, error)
	GenerateCreateTagSQL(tag unifiedmodel.Tag) (string, error)
	GenerateCreateAliasSQL(alias unifiedmodel.Alias) (string, error)
	GenerateCreateSynonymSQL(synonym unifiedmodel.Synonym) (string, error)
	GenerateCreateLabelSQL(label unifiedmodel.Label) (string, error)

	// Backup and recovery, versioning
	GenerateCreateSnapshotSQL(snapshot unifiedmodel.Snapshot) (string, error)
	GenerateCreateBackupSQL(backup unifiedmodel.Backup) (string, error)
	GenerateCreateArchiveSQL(archive unifiedmodel.Archive) (string, error)
	GenerateCreateRecoveryPointSQL(recoveryPoint unifiedmodel.RecoveryPoint) (string, error)
	GenerateCreateVersionSQL(version unifiedmodel.VersionNode) (string, error)
	GenerateCreateMigrationSQL(migration unifiedmodel.Migration) (string, error)
	GenerateCreateBranchSQL(branch unifiedmodel.Branch) (string, error)
	GenerateCreateTimeTravelSQL(timeTravel unifiedmodel.TimeTravel) (string, error)

	// Extensions and customization
	GenerateCreateExtensionSQL(extension unifiedmodel.Extension) (string, error)
	GenerateCreatePluginSQL(plugin unifiedmodel.Plugin) (string, error)
	GenerateCreateModuleExtensionSQL(moduleExt unifiedmodel.ModuleExtension) (string, error)
	GenerateCreateTTLSettingSQL(ttlSetting unifiedmodel.TTLSetting) (string, error)
	GenerateCreateDimensionSQL(dimension unifiedmodel.DimensionSpec) (string, error)
	GenerateCreateDistanceMetricSQL(distanceMetric unifiedmodel.DistanceMetricSpec) (string, error)

	// Advanced analytics
	GenerateCreateProjectionSQL(projection unifiedmodel.Projection) (string, error)
	GenerateCreateAnalyticsAggSQL(analyticsAgg unifiedmodel.AggregationOp) (string, error)
	GenerateCreateTransformationSQL(transformation unifiedmodel.TransformationStep) (string, error)
	GenerateCreateEnrichmentSQL(enrichment unifiedmodel.Enrichment) (string, error)
	GenerateCreateBufferPoolSQL(bufferPool unifiedmodel.BufferPool) (string, error)

	// Replication & distribution
	GenerateCreatePublicationSQL(publication unifiedmodel.Publication) (string, error)
	GenerateCreateSubscriptionSQL(subscription unifiedmodel.Subscription) (string, error)
	GenerateCreateReplicationSlotSQL(replicationSlot unifiedmodel.ReplicationSlot) (string, error)
	GenerateCreateFailoverGroupSQL(failoverGroup unifiedmodel.FailoverGroup) (string, error)

	// High-level generation methods
	GenerateSchema(model *unifiedmodel.UnifiedModel) (string, []string, error)
	GenerateCreateStatements(model *unifiedmodel.UnifiedModel) ([]string, error)
}
