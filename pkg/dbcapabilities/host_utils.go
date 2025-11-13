package dbcapabilities

import (
	"net"
	"strings"
)

// NormalizeHost converts localhost variants to a canonical form.
// It converts "localhost", "127.0.0.1", and "::1" to "localhost".
// All other hosts remain unchanged (no DNS resolution is performed).
func NormalizeHost(host string) string {
	host = strings.TrimSpace(host)
	host = strings.ToLower(host)

	// Check if it's localhost or loopback addresses
	if host == "localhost" || host == "127.0.0.1" || host == "::1" {
		return "localhost"
	}

	// For other loopback addresses in 127.0.0.0/8 range
	ip := net.ParseIP(host)
	if ip != nil && ip.IsLoopback() {
		return "localhost"
	}

	return host
}

// IsPrivateAddress determines if a host address is private/local.
// Returns true for:
//   - localhost and loopback IPs (127.0.0.0/8, ::1)
//   - Private IPv4 ranges: 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16
//   - Private IPv6 ranges: fc00::/7 (unique local), fe80::/10 (link-local)
//
// Returns false for:
//   - Public IPv4 addresses (outside private ranges)
//   - Public IPv6 addresses (outside private ranges)
//   - Hostnames (non-IP strings) - treated as public (DBaaS services with global DNS)
func IsPrivateAddress(host string) bool {
	host = strings.TrimSpace(host)

	// Try to parse as IP address
	ip := net.ParseIP(host)

	// If it's not a valid IP, it's a hostname - treat as public (DBaaS)
	if ip == nil {
		return false
	}

	// Check for loopback
	if ip.IsLoopback() {
		return true
	}

	// Check for private IPv4 ranges
	if ip.To4() != nil {
		// 10.0.0.0/8
		if ip[12] == 10 {
			return true
		}
		// 172.16.0.0/12 (172.16.0.0 - 172.31.255.255)
		if ip[12] == 172 && ip[13] >= 16 && ip[13] <= 31 {
			return true
		}
		// 192.168.0.0/16
		if ip[12] == 192 && ip[13] == 168 {
			return true
		}
		// Public IPv4 address
		return false
	}

	// Check for private IPv6 ranges
	// Link-local addresses (fe80::/10)
	if ip[0] == 0xfe && (ip[1]&0xc0) == 0x80 {
		return true
	}

	// Unique local addresses (fc00::/7)
	if (ip[0] & 0xfe) == 0xfc {
		return true
	}

	// Public IPv6 address
	return false
}

// IsLocalhostVariant checks if the given host is a localhost variant.
// This includes "localhost", "127.x.x.x", and "::1".
func IsLocalhostVariant(host string) bool {
	normalized := NormalizeHost(host)
	return normalized == "localhost"
}
