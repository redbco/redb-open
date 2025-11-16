module github.com/redbco/redb-open/services/stream

go 1.23

replace github.com/redbco/redb-open => ../../

require (
	github.com/redbco/redb-open v0.0.0
	github.com/jackc/pgx/v5 v5.7.2
	google.golang.org/grpc v1.69.2
	google.golang.org/protobuf v1.36.1
)

require (
	github.com/segmentio/kafka-go v0.4.47
	github.com/aws/aws-sdk-go-v2 v1.32.7
	github.com/aws/aws-sdk-go-v2/config v1.28.7
	github.com/aws/aws-sdk-go-v2/credentials v1.17.49
	github.com/aws/aws-sdk-go-v2/service/kinesis v1.32.8
	cloud.google.com/go/pubsub v1.45.3
	github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs v1.2.3
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.17.0
	google.golang.org/api v0.214.0
	github.com/eclipse/paho.mqtt.golang v1.5.0
	github.com/mochi-mqtt/server/v2 v2.6.5
)

