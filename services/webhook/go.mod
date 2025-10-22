module github.com/redbco/redb-open/services/webhook

go 1.24.0

toolchain go1.24.9

require (
	github.com/redbco/redb-open/api v0.0.0
	github.com/redbco/redb-open/pkg v0.0.0
	google.golang.org/grpc v1.75.1
	google.golang.org/protobuf v1.36.9
)

require (
	github.com/google/uuid v1.6.0 // indirect
	golang.org/x/net v0.44.0 // indirect
	golang.org/x/sys v0.36.0 // indirect
	golang.org/x/text v0.30.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250908214217-97024824d090 // indirect
)

replace github.com/redbco/redb-open/pkg => ../../pkg

replace github.com/redbco/redb-open/api => ../../api
