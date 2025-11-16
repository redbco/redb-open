package streamcapabilities

import "strings"

// Get retrieves the capability for a given streaming platform.
// Returns the capability and true if found, or an empty capability and false if not found.
func Get(platform StreamPlatform) (Capability, bool) {
	cap, ok := All[platform]
	return cap, ok
}

// GetByName retrieves the capability for a platform by its canonical name (case-insensitive).
func GetByName(name string) (Capability, bool) {
	return Get(StreamPlatform(strings.ToLower(name)))
}

// SupportsProducer checks if the platform supports producing messages.
func SupportsProducer(platform StreamPlatform) bool {
	cap, ok := Get(platform)
	if !ok {
		return false
	}
	return cap.SupportsProducer
}

// SupportsConsumer checks if the platform supports consuming messages.
func SupportsConsumer(platform StreamPlatform) bool {
	cap, ok := Get(platform)
	if !ok {
		return false
	}
	return cap.SupportsConsumer
}

// SupportsConsumerGroups checks if the platform supports consumer groups.
func SupportsConsumerGroups(platform StreamPlatform) bool {
	cap, ok := Get(platform)
	if !ok {
		return false
	}
	return cap.SupportsConsumerGroups
}

// SupportsPartitions checks if the platform supports partitions.
func SupportsPartitions(platform StreamPlatform) bool {
	cap, ok := Get(platform)
	if !ok {
		return false
	}
	return cap.SupportsPartitions
}

// SupportsTLS checks if the platform supports TLS/SSL encryption.
func SupportsTLS(platform StreamPlatform) bool {
	cap, ok := Get(platform)
	if !ok {
		return false
	}
	return cap.SupportsTLS
}

// HasSchemaRegistry checks if the platform has schema registry support.
func HasSchemaRegistry(platform StreamPlatform) bool {
	cap, ok := Get(platform)
	if !ok {
		return false
	}
	return cap.SchemaRegistrySupport
}

// GetDefaultPort returns the default port for the platform.
// Returns 0 if the platform is not found.
func GetDefaultPort(platform StreamPlatform) int {
	cap, ok := Get(platform)
	if !ok {
		return 0
	}
	return cap.DefaultPort
}

// GetDefaultSSLPort returns the default SSL port for the platform.
// Returns 0 if the platform is not found.
func GetDefaultSSLPort(platform StreamPlatform) int {
	cap, ok := Get(platform)
	if !ok {
		return 0
	}
	return cap.DefaultSSLPort
}

// IsValidPlatform checks if the given string is a valid streaming platform.
func IsValidPlatform(platform string) bool {
	_, ok := Get(StreamPlatform(strings.ToLower(platform)))
	return ok
}

// ListPlatforms returns a list of all supported streaming platforms.
func ListPlatforms() []StreamPlatform {
	platforms := make([]StreamPlatform, 0, len(All))
	for platform := range All {
		platforms = append(platforms, platform)
	}
	return platforms
}

// ListPlatformNames returns a list of human-friendly names for all supported platforms.
func ListPlatformNames() []string {
	names := make([]string, 0, len(All))
	for _, cap := range All {
		names = append(names, cap.Name)
	}
	return names
}
