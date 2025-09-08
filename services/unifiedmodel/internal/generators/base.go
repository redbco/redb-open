// internal/generators/base.go
package generators

import (
	"fmt"

	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// BaseGenerator provides a default implementation for the StatementGenerator interface
// All methods return empty strings or nil by default, allowing specific generators to override only what they support
type BaseGenerator struct{}

// Structural organization
func (b *BaseGenerator) GenerateCreateCatalogSQL(catalog unifiedmodel.Catalog) (string, error) {
	return "", fmt.Errorf("catalog creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateDatabaseSQL(database unifiedmodel.Database) (string, error) {
	return "", fmt.Errorf("database creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateSchemaSQL(schema unifiedmodel.Schema) (string, error) {
	return "", fmt.Errorf("schema creation not supported by this generator")
}

// Primary Data Containers
func (b *BaseGenerator) GenerateCreateTableSQL(table unifiedmodel.Table) (string, error) {
	return "", fmt.Errorf("table creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateCollectionSQL(collection unifiedmodel.Collection) (string, error) {
	return "", fmt.Errorf("collection creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateNodeSQL(node unifiedmodel.Node) (string, error) {
	return "", fmt.Errorf("node creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateMemoryTableSQL(memTable unifiedmodel.MemoryTable) (string, error) {
	return "", fmt.Errorf("memory table creation not supported by this generator")
}

// Temporary Data Containers
func (b *BaseGenerator) GenerateCreateTemporaryTableSQL(tempTable unifiedmodel.TemporaryTable) (string, error) {
	return "", fmt.Errorf("temporary table creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateTransientTableSQL(transTable unifiedmodel.TransientTable) (string, error) {
	return "", fmt.Errorf("transient table creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateCacheSQL(cache unifiedmodel.Cache) (string, error) {
	return "", fmt.Errorf("cache creation not supported by this generator")
}

// Virtual Data Containers
func (b *BaseGenerator) GenerateCreateViewSQL(view unifiedmodel.View) (string, error) {
	return "", fmt.Errorf("view creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateLiveViewSQL(liveView unifiedmodel.LiveView) (string, error) {
	return "", fmt.Errorf("live view creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateWindowViewSQL(windowView unifiedmodel.WindowView) (string, error) {
	return "", fmt.Errorf("window view creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateMaterializedViewSQL(matView unifiedmodel.MaterializedView) (string, error) {
	return "", fmt.Errorf("materialized view creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateExternalTableSQL(extTable unifiedmodel.ExternalTable) (string, error) {
	return "", fmt.Errorf("external table creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateForeignTableSQL(foreignTable unifiedmodel.ForeignTable) (string, error) {
	return "", fmt.Errorf("foreign table creation not supported by this generator")
}

// Graph / Vector / Search abstractions
func (b *BaseGenerator) GenerateCreateGraphSQL(graph unifiedmodel.Graph) (string, error) {
	return "", fmt.Errorf("graph creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateVectorIndexSQL(vectorIndex unifiedmodel.VectorIndex) (string, error) {
	return "", fmt.Errorf("vector index creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateSearchIndexSQL(searchIndex unifiedmodel.SearchIndex) (string, error) {
	return "", fmt.Errorf("search index creation not supported by this generator")
}

// Specialized Data Containers
func (b *BaseGenerator) GenerateCreateVectorSQL(vector unifiedmodel.Vector) (string, error) {
	return "", fmt.Errorf("vector creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateEmbeddingSQL(embedding unifiedmodel.Embedding) (string, error) {
	return "", fmt.Errorf("embedding creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateDocumentSQL(document unifiedmodel.Document) (string, error) {
	return "", fmt.Errorf("document creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateEmbeddedDocumentSQL(embeddedDoc unifiedmodel.EmbeddedDocument) (string, error) {
	return "", fmt.Errorf("embedded document creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateRelationshipSQL(relationship unifiedmodel.Relationship) (string, error) {
	return "", fmt.Errorf("relationship creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreatePathSQL(path unifiedmodel.Path) (string, error) {
	return "", fmt.Errorf("path creation not supported by this generator")
}

// Data Organization Containers
func (b *BaseGenerator) GenerateCreatePartitionSQL(partition unifiedmodel.Partition) (string, error) {
	return "", fmt.Errorf("partition creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateSubPartitionSQL(subPartition unifiedmodel.SubPartition) (string, error) {
	return "", fmt.Errorf("sub-partition creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateShardSQL(shard unifiedmodel.Shard) (string, error) {
	return "", fmt.Errorf("shard creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateKeyspaceSQL(keyspace unifiedmodel.Keyspace) (string, error) {
	return "", fmt.Errorf("keyspace creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateNamespaceSQL(namespace unifiedmodel.Namespace) (string, error) {
	return "", fmt.Errorf("namespace creation not supported by this generator")
}

// Structural definition objects
func (b *BaseGenerator) GenerateCreateTypeSQL(dataType unifiedmodel.Type) (string, error) {
	return "", fmt.Errorf("type creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreatePropertyKeySQL(propertyKey unifiedmodel.PropertyKey) (string, error) {
	return "", fmt.Errorf("property key creation not supported by this generator")
}

// Integrity, performance and identity objects
func (b *BaseGenerator) GenerateCreateIndexSQL(index unifiedmodel.Index) (string, error) {
	return "", fmt.Errorf("index creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateConstraintSQL(constraint unifiedmodel.Constraint) (string, error) {
	return "", fmt.Errorf("constraint creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateSequenceSQL(seq unifiedmodel.Sequence) (string, error) {
	return "", fmt.Errorf("sequence creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateIdentitySQL(identity unifiedmodel.Identity) (string, error) {
	return "", fmt.Errorf("identity creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateUUIDGeneratorSQL(uuidGen unifiedmodel.UUIDGenerator) (string, error) {
	return "", fmt.Errorf("UUID generator creation not supported by this generator")
}

// Executable code objects
func (b *BaseGenerator) GenerateCreateFunctionSQL(fn unifiedmodel.Function) (string, error) {
	return "", fmt.Errorf("function creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateProcedureSQL(procedure unifiedmodel.Procedure) (string, error) {
	return "", fmt.Errorf("procedure creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateMethodSQL(method unifiedmodel.Method) (string, error) {
	return "", fmt.Errorf("method creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateTriggerSQL(trigger unifiedmodel.Trigger) (string, error) {
	return "", fmt.Errorf("trigger creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateEventTriggerSQL(eventTrigger unifiedmodel.EventTrigger) (string, error) {
	return "", fmt.Errorf("event trigger creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateAggregateSQL(aggregate unifiedmodel.Aggregate) (string, error) {
	return "", fmt.Errorf("aggregate creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateOperatorSQL(operator unifiedmodel.Operator) (string, error) {
	return "", fmt.Errorf("operator creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateModuleSQL(module unifiedmodel.Module) (string, error) {
	return "", fmt.Errorf("module creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreatePackageSQL(pkg unifiedmodel.Package) (string, error) {
	return "", fmt.Errorf("package creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreatePackageBodySQL(pkgBody unifiedmodel.PackageBody) (string, error) {
	return "", fmt.Errorf("package body creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateMacroSQL(macro unifiedmodel.Macro) (string, error) {
	return "", fmt.Errorf("macro creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateRuleSQL(rule unifiedmodel.Rule) (string, error) {
	return "", fmt.Errorf("rule creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateWindowFuncSQL(windowFunc unifiedmodel.WindowFunc) (string, error) {
	return "", fmt.Errorf("window function creation not supported by this generator")
}

// Security and access control
func (b *BaseGenerator) GenerateCreateUserSQL(user unifiedmodel.DBUser) (string, error) {
	return "", fmt.Errorf("user creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateRoleSQL(role unifiedmodel.DBRole) (string, error) {
	return "", fmt.Errorf("role creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateGrantSQL(grant unifiedmodel.Grant) (string, error) {
	return "", fmt.Errorf("grant creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreatePolicySQL(policy unifiedmodel.Policy) (string, error) {
	return "", fmt.Errorf("policy creation not supported by this generator")
}

// Physical storage and placement
func (b *BaseGenerator) GenerateCreateTablespaceSQL(tablespace unifiedmodel.Tablespace) (string, error) {
	return "", fmt.Errorf("tablespace creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateSegmentSQL(segment unifiedmodel.Segment) (string, error) {
	return "", fmt.Errorf("segment creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateExtentSQL(extent unifiedmodel.Extent) (string, error) {
	return "", fmt.Errorf("extent creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreatePageSQL(page unifiedmodel.Page) (string, error) {
	return "", fmt.Errorf("page creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateFilegroupSQL(filegroup unifiedmodel.Filegroup) (string, error) {
	return "", fmt.Errorf("filegroup creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateDatafileSQL(datafile unifiedmodel.Datafile) (string, error) {
	return "", fmt.Errorf("datafile creation not supported by this generator")
}

// Connectivity and integration
func (b *BaseGenerator) GenerateCreateServerSQL(server unifiedmodel.Server) (string, error) {
	return "", fmt.Errorf("server creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateConnectionSQL(connection unifiedmodel.Connection) (string, error) {
	return "", fmt.Errorf("connection creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateEndpointSQL(endpoint unifiedmodel.Endpoint) (string, error) {
	return "", fmt.Errorf("endpoint creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateForeignDataWrapperSQL(fdw unifiedmodel.ForeignDataWrapper) (string, error) {
	return "", fmt.Errorf("foreign data wrapper creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateUserMappingSQL(userMapping unifiedmodel.UserMapping) (string, error) {
	return "", fmt.Errorf("user mapping creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateFederationSQL(federation unifiedmodel.Federation) (string, error) {
	return "", fmt.Errorf("federation creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateReplicaSQL(replica unifiedmodel.Replica) (string, error) {
	return "", fmt.Errorf("replica creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateClusterSQL(cluster unifiedmodel.Cluster) (string, error) {
	return "", fmt.Errorf("cluster creation not supported by this generator")
}

// Operational, pipelines and streaming
func (b *BaseGenerator) GenerateCreateTaskSQL(task unifiedmodel.Task) (string, error) {
	return "", fmt.Errorf("task creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateJobSQL(job unifiedmodel.Job) (string, error) {
	return "", fmt.Errorf("job creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateScheduleSQL(schedule unifiedmodel.Schedule) (string, error) {
	return "", fmt.Errorf("schedule creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreatePipelineSQL(pipeline unifiedmodel.Pipeline) (string, error) {
	return "", fmt.Errorf("pipeline creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateStreamSQL(stream unifiedmodel.Stream) (string, error) {
	return "", fmt.Errorf("stream creation not supported by this generator")
}

// Monitoring and alerting
func (b *BaseGenerator) GenerateCreateEventSQL(event unifiedmodel.Event) (string, error) {
	return "", fmt.Errorf("event creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateNotificationSQL(notification unifiedmodel.Notification) (string, error) {
	return "", fmt.Errorf("notification creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateAlertSQL(alert unifiedmodel.Alert) (string, error) {
	return "", fmt.Errorf("alert creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateStatisticSQL(statistic unifiedmodel.Statistic) (string, error) {
	return "", fmt.Errorf("statistic creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateHistogramSQL(histogram unifiedmodel.Histogram) (string, error) {
	return "", fmt.Errorf("histogram creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateMonitorSQL(monitor unifiedmodel.Monitor) (string, error) {
	return "", fmt.Errorf("monitor creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateMonitorMetricSQL(monitorMetric unifiedmodel.MonitorMetric) (string, error) {
	return "", fmt.Errorf("monitor metric creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateThresholdSQL(threshold unifiedmodel.Threshold) (string, error) {
	return "", fmt.Errorf("threshold creation not supported by this generator")
}

// Text processing / search configuration
func (b *BaseGenerator) GenerateCreateTextSearchComponentSQL(textSearchComponent unifiedmodel.TextSearchComponent) (string, error) {
	return "", fmt.Errorf("text search component creation not supported by this generator")
}

// Metadata and documentation
func (b *BaseGenerator) GenerateCreateCommentSQL(comment unifiedmodel.Comment) (string, error) {
	return "", fmt.Errorf("comment creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateAnnotationSQL(annotation unifiedmodel.Annotation) (string, error) {
	return "", fmt.Errorf("annotation creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateTagSQL(tag unifiedmodel.Tag) (string, error) {
	return "", fmt.Errorf("tag creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateAliasSQL(alias unifiedmodel.Alias) (string, error) {
	return "", fmt.Errorf("alias creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateSynonymSQL(synonym unifiedmodel.Synonym) (string, error) {
	return "", fmt.Errorf("synonym creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateLabelSQL(label unifiedmodel.Label) (string, error) {
	return "", fmt.Errorf("label creation not supported by this generator")
}

// Backup and recovery, versioning
func (b *BaseGenerator) GenerateCreateSnapshotSQL(snapshot unifiedmodel.Snapshot) (string, error) {
	return "", fmt.Errorf("snapshot creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateBackupSQL(backup unifiedmodel.Backup) (string, error) {
	return "", fmt.Errorf("backup creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateArchiveSQL(archive unifiedmodel.Archive) (string, error) {
	return "", fmt.Errorf("archive creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateRecoveryPointSQL(recoveryPoint unifiedmodel.RecoveryPoint) (string, error) {
	return "", fmt.Errorf("recovery point creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateVersionSQL(version unifiedmodel.VersionNode) (string, error) {
	return "", fmt.Errorf("version creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateMigrationSQL(migration unifiedmodel.Migration) (string, error) {
	return "", fmt.Errorf("migration creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateBranchSQL(branch unifiedmodel.Branch) (string, error) {
	return "", fmt.Errorf("branch creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateTimeTravelSQL(timeTravel unifiedmodel.TimeTravel) (string, error) {
	return "", fmt.Errorf("time travel creation not supported by this generator")
}

// Extensions and customization
func (b *BaseGenerator) GenerateCreateExtensionSQL(extension unifiedmodel.Extension) (string, error) {
	return "", fmt.Errorf("extension creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreatePluginSQL(plugin unifiedmodel.Plugin) (string, error) {
	return "", fmt.Errorf("plugin creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateModuleExtensionSQL(moduleExt unifiedmodel.ModuleExtension) (string, error) {
	return "", fmt.Errorf("module extension creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateTTLSettingSQL(ttlSetting unifiedmodel.TTLSetting) (string, error) {
	return "", fmt.Errorf("TTL setting creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateDimensionSQL(dimension unifiedmodel.DimensionSpec) (string, error) {
	return "", fmt.Errorf("dimension creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateDistanceMetricSQL(distanceMetric unifiedmodel.DistanceMetricSpec) (string, error) {
	return "", fmt.Errorf("distance metric creation not supported by this generator")
}

// Advanced analytics
func (b *BaseGenerator) GenerateCreateProjectionSQL(projection unifiedmodel.Projection) (string, error) {
	return "", fmt.Errorf("projection creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateAnalyticsAggSQL(analyticsAgg unifiedmodel.AggregationOp) (string, error) {
	return "", fmt.Errorf("analytics aggregation creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateTransformationSQL(transformation unifiedmodel.TransformationStep) (string, error) {
	return "", fmt.Errorf("transformation creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateEnrichmentSQL(enrichment unifiedmodel.Enrichment) (string, error) {
	return "", fmt.Errorf("enrichment creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateBufferPoolSQL(bufferPool unifiedmodel.BufferPool) (string, error) {
	return "", fmt.Errorf("buffer pool creation not supported by this generator")
}

// Replication & distribution
func (b *BaseGenerator) GenerateCreatePublicationSQL(publication unifiedmodel.Publication) (string, error) {
	return "", fmt.Errorf("publication creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateSubscriptionSQL(subscription unifiedmodel.Subscription) (string, error) {
	return "", fmt.Errorf("subscription creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateReplicationSlotSQL(replicationSlot unifiedmodel.ReplicationSlot) (string, error) {
	return "", fmt.Errorf("replication slot creation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateFailoverGroupSQL(failoverGroup unifiedmodel.FailoverGroup) (string, error) {
	return "", fmt.Errorf("failover group creation not supported by this generator")
}

// High-level generation methods
func (b *BaseGenerator) GenerateSchema(model *unifiedmodel.UnifiedModel) (string, []string, error) {
	return "", nil, fmt.Errorf("schema generation not supported by this generator")
}

func (b *BaseGenerator) GenerateCreateStatements(model *unifiedmodel.UnifiedModel) ([]string, error) {
	return nil, fmt.Errorf("statement generation not supported by this generator")
}
