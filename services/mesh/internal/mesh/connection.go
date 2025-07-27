package mesh

import "time"

// Connection represents a connection to another node
type Connection struct {
	ID       string
	Status   string
	LastSeen time.Time
}
