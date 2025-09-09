// Package dbcapabilities provides a shared registry describing the capabilities of
// databases supported by the platform. Microservices can import this package to
// make decisions based on uniform metadata (system databases, CDC, paradigms).
//
// Minimal usage example:
//
//	import "github.com/redbco/redb-open/pkg/dbcapabilities"
//
//	func usesCDC(db string) bool {
//	    return dbcapabilities.SupportsCDC(dbcapabilities.DatabaseID(db))
//	}
//
// Example: Checking CDC support when you only have a database_type string (e.g., "postgres")
// stored in your service's local database:
//
//	import (
//	    "strings"
//	    "github.com/redbco/redb-open/pkg/dbcapabilities"
//	)
//
//	// databaseType comes from your local DB (e.g., "postgres", "mysql", ...)
//	func databaseSupportsCDC(databaseType string) bool {
//	    canonical := dbcapabilities.DatabaseID(strings.ToLower(databaseType))
//	    return dbcapabilities.SupportsCDC(canonical)
//	}
//
//	// For additional info (system DB names, paradigms, etc.):
//	func getDatabaseCapability(databaseType string) (dbcapabilities.Capability, bool) {
//	    canonical := dbcapabilities.DatabaseID(strings.ToLower(databaseType))
//	    return dbcapabilities.Get(canonical)
//	}
//
// The package exposes constants for IDs (e.g., dbcapabilities.PostgreSQL) and a
// registry `All` for advanced consumers.
package dbcapabilities
