package streamcapabilities

// StreamPlatform is the canonical identifier for a streaming platform supported by reDB.
// Use these constants to look up capability information.
type StreamPlatform string

const (
	// Message Queue / Streaming Platforms
	Kafka      StreamPlatform = "kafka"
	Redpanda   StreamPlatform = "redpanda"
	Pulsar     StreamPlatform = "pulsar"
	RabbitMQ   StreamPlatform = "rabbitmq"
	NATS       StreamPlatform = "nats"
	MQTT       StreamPlatform = "mqtt"
	MQTTServer StreamPlatform = "mqtt_server"

	// Cloud Streaming Services
	Kinesis   StreamPlatform = "kinesis"
	PubSub    StreamPlatform = "pubsub"
	EventHubs StreamPlatform = "eventhubs"
	SQS       StreamPlatform = "sqs"
	SNS       StreamPlatform = "sns"
)

// Capability describes what a streaming platform supports in a way that microservices can consume uniformly.
type Capability struct {
	// Human-friendly platform name, e.g., "Apache Kafka".
	Name string `json:"name"`

	// Canonical ID used across the codebase (see StreamPlatform constants), e.g., "kafka".
	ID StreamPlatform `json:"id"`

	// Whether the platform supports producing messages
	SupportsProducer bool `json:"supportsProducer"`

	// Whether the platform supports consuming messages
	SupportsConsumer bool `json:"supportsConsumer"`

	// Whether the platform can act as a broker/server itself (e.g., MQTT)
	SupportsServerMode bool `json:"supportsServerMode"`

	// Whether the platform supports topic/stream partitions
	SupportsPartitions bool `json:"supportsPartitions"`

	// Whether the platform supports consumer groups for parallel processing
	SupportsConsumerGroups bool `json:"supportsConsumerGroups"`

	// Whether the platform supports SASL authentication
	SupportsSASL bool `json:"supportsSASL"`

	// Whether the platform supports TLS/SSL encryption
	SupportsTLS bool `json:"supportsTLS"`

	// Default port for the platform
	DefaultPort int `json:"defaultPort"`

	// Default SSL/TLS port (if different from DefaultPort)
	DefaultSSLPort int `json:"defaultSSLPort"`

	// Whether the platform has schema registry support
	SchemaRegistrySupport bool `json:"schemaRegistrySupport"`

	// Connection string template for the platform
	ConnectionStringTemplate string `json:"connectionStringTemplate"`

	// Whether the platform supports transactions
	SupportsTransactions bool `json:"supportsTransactions"`

	// Whether the platform supports message ordering guarantees
	SupportsOrdering bool `json:"supportsOrdering"`

	// Whether the platform supports wildcards in topic/queue subscriptions
	SupportsWildcards bool `json:"supportsWildcards"`
}

// All is a registry of capabilities keyed by the canonical platform ID.
var All = map[StreamPlatform]Capability{
	Kafka: {
		Name:                     "Apache Kafka",
		ID:                       Kafka,
		SupportsProducer:         true,
		SupportsConsumer:         true,
		SupportsServerMode:       false,
		SupportsPartitions:       true,
		SupportsConsumerGroups:   true,
		SupportsSASL:             true,
		SupportsTLS:              true,
		DefaultPort:              9092,
		DefaultSSLPort:           9093,
		SchemaRegistrySupport:    true,
		ConnectionStringTemplate: "kafka://{{hosts}}/{{topic}}",
		SupportsTransactions:     true,
		SupportsOrdering:         true,
		SupportsWildcards:        false,
	},
	Redpanda: {
		Name:                     "Redpanda",
		ID:                       Redpanda,
		SupportsProducer:         true,
		SupportsConsumer:         true,
		SupportsServerMode:       false,
		SupportsPartitions:       true,
		SupportsConsumerGroups:   true,
		SupportsSASL:             true,
		SupportsTLS:              true,
		DefaultPort:              9092,
		DefaultSSLPort:           9093,
		SchemaRegistrySupport:    true,
		ConnectionStringTemplate: "redpanda://{{hosts}}/{{topic}}",
		SupportsTransactions:     true,
		SupportsOrdering:         true,
		SupportsWildcards:        false,
	},
	Kinesis: {
		Name:                     "AWS Kinesis",
		ID:                       Kinesis,
		SupportsProducer:         true,
		SupportsConsumer:         true,
		SupportsServerMode:       false,
		SupportsPartitions:       true,
		SupportsConsumerGroups:   false,
		SupportsSASL:             false,
		SupportsTLS:              true,
		DefaultPort:              443,
		DefaultSSLPort:           443,
		SchemaRegistrySupport:    false,
		ConnectionStringTemplate: "kinesis://{{region}}/{{stream}}",
		SupportsTransactions:     false,
		SupportsOrdering:         true,
		SupportsWildcards:        false,
	},
	PubSub: {
		Name:                     "Google Cloud Pub/Sub",
		ID:                       PubSub,
		SupportsProducer:         true,
		SupportsConsumer:         true,
		SupportsServerMode:       false,
		SupportsPartitions:       false,
		SupportsConsumerGroups:   false,
		SupportsSASL:             false,
		SupportsTLS:              true,
		DefaultPort:              443,
		DefaultSSLPort:           443,
		SchemaRegistrySupport:    true,
		ConnectionStringTemplate: "pubsub://{{project}}/{{topic}}",
		SupportsTransactions:     false,
		SupportsOrdering:         true,
		SupportsWildcards:        false,
	},
	EventHubs: {
		Name:                     "Azure Event Hubs",
		ID:                       EventHubs,
		SupportsProducer:         true,
		SupportsConsumer:         true,
		SupportsServerMode:       false,
		SupportsPartitions:       true,
		SupportsConsumerGroups:   true,
		SupportsSASL:             true,
		SupportsTLS:              true,
		DefaultPort:              5671,
		DefaultSSLPort:           5672,
		SchemaRegistrySupport:    true,
		ConnectionStringTemplate: "eventhubs://{{namespace}}.servicebus.windows.net/{{eventhub}}",
		SupportsTransactions:     false,
		SupportsOrdering:         true,
		SupportsWildcards:        false,
	},
	Pulsar: {
		Name:                     "Apache Pulsar",
		ID:                       Pulsar,
		SupportsProducer:         true,
		SupportsConsumer:         true,
		SupportsServerMode:       false,
		SupportsPartitions:       true,
		SupportsConsumerGroups:   true,
		SupportsSASL:             true,
		SupportsTLS:              true,
		DefaultPort:              6650,
		DefaultSSLPort:           6651,
		SchemaRegistrySupport:    true,
		ConnectionStringTemplate: "pulsar://{{hosts}}/{{topic}}",
		SupportsTransactions:     true,
		SupportsOrdering:         true,
		SupportsWildcards:        true,
	},
	RabbitMQ: {
		Name:                     "RabbitMQ",
		ID:                       RabbitMQ,
		SupportsProducer:         true,
		SupportsConsumer:         true,
		SupportsServerMode:       false,
		SupportsPartitions:       false,
		SupportsConsumerGroups:   false,
		SupportsSASL:             true,
		SupportsTLS:              true,
		DefaultPort:              5672,
		DefaultSSLPort:           5671,
		SchemaRegistrySupport:    false,
		ConnectionStringTemplate: "amqp://{{host}}:{{port}}/{{vhost}}",
		SupportsTransactions:     true,
		SupportsOrdering:         false,
		SupportsWildcards:        true,
	},
	NATS: {
		Name:                     "NATS",
		ID:                       NATS,
		SupportsProducer:         true,
		SupportsConsumer:         true,
		SupportsServerMode:       false,
		SupportsPartitions:       false,
		SupportsConsumerGroups:   false,
		SupportsSASL:             false,
		SupportsTLS:              true,
		DefaultPort:              4222,
		DefaultSSLPort:           4222,
		SchemaRegistrySupport:    false,
		ConnectionStringTemplate: "nats://{{hosts}}/{{subject}}",
		SupportsTransactions:     false,
		SupportsOrdering:         false,
		SupportsWildcards:        true,
	},
	MQTT: {
		Name:                     "MQTT Client",
		ID:                       MQTT,
		SupportsProducer:         true,
		SupportsConsumer:         true,
		SupportsServerMode:       false,
		SupportsPartitions:       false,
		SupportsConsumerGroups:   false,
		SupportsSASL:             false,
		SupportsTLS:              true,
		DefaultPort:              1883,
		DefaultSSLPort:           8883,
		SchemaRegistrySupport:    false,
		ConnectionStringTemplate: "mqtt://{{host}}:{{port}}/{{topic}}",
		SupportsTransactions:     false,
		SupportsOrdering:         false,
		SupportsWildcards:        true,
	},
	MQTTServer: {
		Name:                     "MQTT Broker",
		ID:                       MQTTServer,
		SupportsProducer:         true,
		SupportsConsumer:         true,
		SupportsServerMode:       true,
		SupportsPartitions:       false,
		SupportsConsumerGroups:   false,
		SupportsSASL:             false,
		SupportsTLS:              true,
		DefaultPort:              1883,
		DefaultSSLPort:           8883,
		SchemaRegistrySupport:    false,
		ConnectionStringTemplate: "mqtt://{{bind_address}}:{{port}}",
		SupportsTransactions:     false,
		SupportsOrdering:         false,
		SupportsWildcards:        true,
	},
	SQS: {
		Name:                     "AWS SQS",
		ID:                       SQS,
		SupportsProducer:         true,
		SupportsConsumer:         true,
		SupportsServerMode:       false,
		SupportsPartitions:       false,
		SupportsConsumerGroups:   false,
		SupportsSASL:             false,
		SupportsTLS:              true,
		DefaultPort:              443,
		DefaultSSLPort:           443,
		SchemaRegistrySupport:    false,
		ConnectionStringTemplate: "sqs://{{region}}/{{queue}}",
		SupportsTransactions:     false,
		SupportsOrdering:         false,
		SupportsWildcards:        false,
	},
	SNS: {
		Name:                     "AWS SNS",
		ID:                       SNS,
		SupportsProducer:         true,
		SupportsConsumer:         true,
		SupportsServerMode:       false,
		SupportsPartitions:       false,
		SupportsConsumerGroups:   false,
		SupportsSASL:             false,
		SupportsTLS:              true,
		DefaultPort:              443,
		DefaultSSLPort:           443,
		SchemaRegistrySupport:    false,
		ConnectionStringTemplate: "sns://{{region}}/{{topic}}",
		SupportsTransactions:     false,
		SupportsOrdering:         false,
		SupportsWildcards:        false,
	},
}
