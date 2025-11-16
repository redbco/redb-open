package main

// This file imports all stream adapters to ensure they are compiled into the binary.
// Each adapter registers itself with the adapter registry on init().

import (
	// Import adapters
	_ "github.com/redbco/redb-open/services/stream/internal/adapter/eventhubs"
	_ "github.com/redbco/redb-open/services/stream/internal/adapter/kafka"
	_ "github.com/redbco/redb-open/services/stream/internal/adapter/kinesis"
	_ "github.com/redbco/redb-open/services/stream/internal/adapter/mqtt"
	_ "github.com/redbco/redb-open/services/stream/internal/adapter/pubsub"
)
