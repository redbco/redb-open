// Package streamcapabilities provides a shared registry describing the capabilities of
// streaming platforms supported by the platform. Microservices can import this package to
// make decisions based on uniform metadata (producer/consumer support, partitions, TLS).
//
// Minimal usage example:
//
//	import "github.com/redbco/redb-open/pkg/streamcapabilities"
//
//	func supportsConsumerGroups(platform string) bool {
//	    return streamcapabilities.SupportsConsumerGroups(streamcapabilities.StreamPlatform(platform))
//	}
//
// Example: Checking producer support when you only have a platform string (e.g., "kafka")
// stored in your service's local database:
//
//	import (
//	    "strings"
//	    "github.com/redbco/redb-open/pkg/streamcapabilities"
//	)
//
//	// platform comes from your local DB (e.g., "kafka", "kinesis", ...)
//	func platformSupportsProducer(platform string) bool {
//	    canonical := streamcapabilities.StreamPlatform(strings.ToLower(platform))
//	    return streamcapabilities.SupportsProducer(canonical)
//	}
//
//	// For additional info (ports, TLS support, etc.):
//	func getPlatformCapability(platform string) (streamcapabilities.Capability, bool) {
//	    canonical := streamcapabilities.StreamPlatform(strings.ToLower(platform))
//	    return streamcapabilities.Get(canonical)
//	}
//
// The package exposes constants for platform IDs (e.g., streamcapabilities.Kafka) and a
// registry `All` for advanced consumers.
package streamcapabilities
